//! HealthScan — periodic scan of owned segments for repair and degradation.
//!
//! For each owned segment, polls providers via `HealthQuery` (direct stream)
//! to get a total piece count across the network.
//! If count < k * tier_target → repair.
//! If count > k * tier_target AND no demand → degrade.
//!
//! No gossip. No PieceMap. No Merkle diff state machine.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use craftobj_core::{ContentId, HealthAction, HealthSnapshot, SegmentSnapshot};
use std::io::{BufRead, Write};
use craftobj_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::CraftObjCommand;
use crate::scaling::DemandTracker;

/// Minimum pieces a node must keep per segment.
const MIN_PIECES_PER_SEGMENT: usize = 2;

/// Default scan interval in seconds (5 minutes).
pub const DEFAULT_SCAN_INTERVAL_SECS: u64 = 300;

/// Maximum age of health snapshots to keep (24 hours).
const SNAPSHOT_MAX_AGE_MS: u64 = 24 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// HealthScan
// ---------------------------------------------------------------------------

/// HealthScan periodically scans owned segments and triggers repair/degradation.
///
/// Remote piece counts are obtained on-demand via `HealthQuery` — a lightweight
/// direct-stream request that returns the peer's piece count for a
/// (CID, segment) pair. No gossip, no cached PieceMap.
pub struct HealthScan {
    store: Arc<Mutex<FsStore>>,
    demand_tracker: Arc<Mutex<DemandTracker>>,
    local_peer_id: PeerId,
    tier_target: f64,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    scan_interval: Duration,
    data_dir: Option<PathBuf>,
    /// Current position in the sorted CID list for rotational scanning.
    rotation_cursor: usize,
}

impl HealthScan {
    /// Create a new HealthScan.
    pub fn new(
        store: Arc<Mutex<FsStore>>,
        demand_tracker: Arc<Mutex<DemandTracker>>,
        local_peer_id: PeerId,
        command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    ) -> Self {
        Self {
            store,
            demand_tracker,
            local_peer_id,
            tier_target: 1.5,
            command_tx,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            data_dir: None,
            rotation_cursor: 0,
        }
    }

    /// Set custom tier target (default 1.5).
    pub fn set_tier_target(&mut self, target: f64) {
        self.tier_target = target.max(1.5);
    }

    /// Set custom scan interval.
    pub fn set_scan_interval(&mut self, interval: Duration) {
        self.scan_interval = interval;
    }

    /// Set data directory for health history persistence.
    pub fn set_data_dir(&mut self, dir: PathBuf) {
        self.data_dir = Some(dir);
    }

    /// Get the scan interval.
    pub fn scan_interval(&self) -> Duration {
        self.scan_interval
    }

    // -----------------------------------------------------------------------
    // Helpers: DHT resolve + HealthQuery
    // -----------------------------------------------------------------------

    /// Resolve DHT providers for a content ID (excluding self).
    async fn resolve_providers(&self, cid: ContentId) -> Vec<PeerId> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .command_tx
            .send(CraftObjCommand::ResolveProviders {
                content_id: cid,
                reply_tx: tx,
            })
            .is_err()
        {
            warn!("HealthScan: failed to send ResolveProviders command");
            return Vec::new();
        }
        match rx.await {
            Ok(Ok(peers)) => peers.into_iter().filter(|p| *p != self.local_peer_id).collect(),
            Ok(Err(e)) => {
                debug!("HealthScan: ResolveProviders error for {}: {}", cid, e);
                Vec::new()
            }
            Err(_) => Vec::new(),
        }
    }

    /// Query a single peer for its piece count for (cid, segment).
    async fn health_query(&self, peer_id: PeerId, cid: ContentId, segment: u32) -> u32 {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .command_tx
            .send(CraftObjCommand::HealthQuery {
                peer_id,
                content_id: cid,
                segment_index: segment,
                reply_tx: tx,
            })
            .is_err()
        {
            return 0;
        }
        match rx.await {
            Ok(Ok(count)) => count,
            _ => 0,
        }
    }

    /// Query all providers for (cid, segment) and return total network piece count.
    /// Returns (total_remote_pieces, provider_count).
    async fn query_network_count(&self, cid: ContentId, segment: u32) -> (u32, usize) {
        let providers = self.resolve_providers(cid).await;
        if providers.is_empty() {
            return (0, 0);
        }
        let provider_count = providers.len();
        let mut total: u32 = 0;
        for peer in providers {
            total += self.health_query(peer, cid, segment).await;
        }
        (total, provider_count)
    }

    // -----------------------------------------------------------------------
    // Scan loop
    // -----------------------------------------------------------------------

    /// Run a single scan over all owned segments.
    ///
    /// 1. List CIDs from local store.
    /// 2. For each segment in the 1% batch: query network, repair or degrade.
    /// 3. Persist health snapshots.
    pub async fn run_scan(&mut self) {
        // List all CIDs this node holds pieces for
        let all_cids_sorted: Vec<ContentId> = {
            let s = self.store.lock().await;
            let mut cids = s.list_content().unwrap_or_default();
            cids.sort_by(|a, b| a.0.cmp(&b.0));
            cids
        };

        if all_cids_sorted.is_empty() {
            debug!("HealthScan: no owned segments to scan");
            return;
        }

        // 1% rotation batch (at least 1)
        let total = all_cids_sorted.len();
        let batch_size = (total / 100).max(1);
        let cursor = self.rotation_cursor % total;
        let end = std::cmp::min(cursor + batch_size, total);
        let batch_cids = &all_cids_sorted[cursor..end];

        info!(
            "HealthScan: scanning batch {}..{} of {} CIDs (1% rotation)",
            cursor, end, total
        );

        self.rotation_cursor = if end >= total { 0 } else { end };

        // Collect owned (cid, segment) pairs from local store
        let owned_segments: Vec<(ContentId, u32)> = {
            let s = self.store.lock().await;
            let mut out = Vec::new();
            for cid in batch_cids {
                if let Ok(segs) = s.list_segments(cid) {
                    for seg in segs {
                        out.push((*cid, seg));
                    }
                }
            }
            out
        };

        if owned_segments.is_empty() {
            debug!("HealthScan: no owned segments in current batch");
            return;
        }

        debug!("HealthScan: scanning {} owned segments", owned_segments.len());

        // Group by CID for snapshot generation
        let mut cid_segments: HashMap<ContentId, Vec<u32>> = HashMap::new();
        for (cid, seg) in &owned_segments {
            cid_segments.entry(*cid).or_default().push(*seg);
        }

        let mut actions_by_cid: HashMap<ContentId, Vec<HealthAction>> = HashMap::new();

        for (cid, segment) in &owned_segments {
            let action = self.scan_segment(*cid, *segment).await;
            if let Some(a) = action {
                actions_by_cid.entry(*cid).or_default().push(a);
            }
        }

        // Persist snapshots
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let default_k = craftobj_core::SEGMENT_SIZE.div_ceil(craftobj_core::PIECE_SIZE);

        for (cid, segments) in &cid_segments {
            let manifest = {
                let s = self.store.lock().await;
                s.get_record(cid).ok()
            };

            let mut seg_snapshots = Vec::new();
            let mut min_ratio = f64::MAX;

            for &seg in segments {
                let local_count = {
                    let s = self.store.lock().await;
                    s.list_pieces(cid, seg).unwrap_or_default().len() as u32
                };
                let (remote_total, provider_count) = self.query_network_count(*cid, seg).await;
                let total_pieces = local_count + remote_total;
                let k = manifest
                    .as_ref()
                    .map(|m| m.k_for_segment(seg as usize))
                    .unwrap_or(default_k);
                let ratio = if k > 0 { total_pieces as f64 / k as f64 } else { 0.0 };
                if ratio < min_ratio {
                    min_ratio = ratio;
                }
                seg_snapshots.push(SegmentSnapshot {
                    index: seg,
                    rank: total_pieces as usize,
                    k,
                    total_pieces: total_pieces as usize,
                    provider_count: provider_count + if local_count > 0 { 1 } else { 0 },
                });
            }

            // Total distinct providers for this CID across all segments
            let (_, any_provider_count) = self.query_network_count(*cid, 0).await;

            let snapshot = HealthSnapshot {
                timestamp: now,
                content_id: *cid,
                segment_count: segments.len(),
                segments: seg_snapshots,
                provider_count: any_provider_count + 1, // +1 for self
                health_ratio: if min_ratio == f64::MAX { 0.0 } else { min_ratio },
                actions: actions_by_cid.remove(cid).unwrap_or_default(),
            };

            self.persist_snapshot(&snapshot);
        }
    }

    /// Scan a single segment for repair or degradation needs.
    async fn scan_segment(&self, cid: ContentId, segment: u32) -> Option<HealthAction> {
        let local_count = {
            let s = self.store.lock().await;
            s.list_pieces(&cid, segment).unwrap_or_default().len()
        };
        let local_node = self.local_peer_id.to_bytes();

        let (remote_total, _provider_count) = self.query_network_count(cid, segment).await;
        let total_network_pieces = local_count + remote_total as usize;

        let default_k = craftobj_core::SEGMENT_SIZE.div_ceil(craftobj_core::PIECE_SIZE);
        let k = {
            let s = self.store.lock().await;
            s.get_record(&cid)
                .ok()
                .map(|m| m.k_for_segment(segment as usize))
                .unwrap_or(default_k)
        };

        let k_f = k as f64;
        let target_pieces = (k_f * self.tier_target).ceil() as usize;

        debug!(
            "HealthScan: {}/seg{} k={} target={} network_pieces={} local={} tier={:.1}",
            cid, segment, k, target_pieces, total_network_pieces, local_count, self.tier_target
        );

        // Under-replication → repair
        if total_network_pieces < target_pieces && local_count >= MIN_PIECES_PER_SEGMENT {
            // Deterministic: sort providers by piece count (piece is the same for all),
            // use local position. Since we have no per-provider counts here, use
            // a position of 0 (self is always willing to repair when under-replicated).
            self.attempt_repair(cid, segment, &local_node, 0).await;
            return Some(HealthAction::Repaired {
                segment,
                offset: 0,
            });
        }

        // Over-replication → degrade if no demand
        if total_network_pieces > target_pieces && local_count > MIN_PIECES_PER_SEGMENT {
            let has_demand = {
                let dt = self.demand_tracker.lock().await;
                dt.has_demand(&cid)
            };
            if !has_demand {
                debug!(
                    "HealthScan: {}/seg{} over-replicated (pieces={}, target={}), degrading",
                    cid, segment, total_network_pieces, target_pieces
                );
                self.attempt_degradation(cid, segment).await;
                return Some(HealthAction::Degraded { segment });
            }
        }

        None
    }

    /// Attempt to repair a segment by creating a new RLNC piece.
    async fn attempt_repair(
        &self,
        cid: ContentId,
        segment: u32,
        _local_node: &[u8],
        _offset: usize,
    ) {
        let store_guard = self.store.lock().await;
        let result = crate::health::heal_segment(&store_guard, &cid, segment, 1);

        if result.pieces_generated == 0 {
            warn!(
                "HealthScan repair failed for {}/seg{}: {:?}",
                cid, segment, result.errors
            );
            return;
        }

        // Publish DHT provider record for this CID
        let pkey = craftobj_routing::providers_dht_key(&cid);
        let _ = self.command_tx.send(CraftObjCommand::StartProviding { key: pkey });
        info!(
            "HealthScan repair complete for {}/seg{}: generated {} new piece(s)",
            cid, segment, result.pieces_generated
        );
    }

    /// Attempt to degrade a segment by dropping 1 piece.
    async fn attempt_degradation(&self, cid: ContentId, segment: u32) {
        let store_guard = self.store.lock().await;
        let mut pieces = match store_guard.list_pieces(&cid, segment) {
            Ok(p) if p.len() > MIN_PIECES_PER_SEGMENT => p,
            _ => return,
        };
        pieces.sort();
        let piece_to_drop = *pieces.last().unwrap();

        if let Err(e) = store_guard.delete_piece(&cid, segment, &piece_to_drop) {
            warn!(
                "HealthScan degradation failed for {}/seg{}: {}",
                cid, segment, e
            );
            return;
        }

        info!(
            "HealthScan degradation: dropped piece {} for {}/seg{}",
            hex::encode(&piece_to_drop[..8]),
            cid,
            segment
        );
    }

    /// Persist a health snapshot to JSONL file.
    fn persist_snapshot(&self, snapshot: &HealthSnapshot) {
        let Some(ref data_dir) = self.data_dir else {
            return;
        };
        let dir = data_dir.join("health_history");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!("Failed to create health_history dir: {}", e);
            return;
        }
        let path = dir.join(format!("{}.jsonl", snapshot.content_id));
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to open snapshot file: {}", e);
                return;
            }
        };
        if let Ok(json) = serde_json::to_string(snapshot) {
            let _ = writeln!(file, "{}", json);
        }

        self.prune_snapshots(&path, snapshot.timestamp);
    }

    /// Remove snapshot entries older than SNAPSHOT_MAX_AGE_MS.
    fn prune_snapshots(&self, path: &std::path::Path, now: u64) {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return,
        };
        let cutoff = now.saturating_sub(SNAPSHOT_MAX_AGE_MS);
        let kept: Vec<&str> = content
            .lines()
            .filter(|line| {
                serde_json::from_str::<HealthSnapshot>(line)
                    .map(|s| s.timestamp >= cutoff)
                    .unwrap_or(false)
            })
            .collect();

        if kept.len() < content.lines().count() {
            let _ = std::fs::write(path, kept.join("\n") + "\n");
        }
    }

    /// Load health snapshots for a CID, optionally filtered by `since` (unix millis).
    pub fn load_snapshots(&self, cid: &ContentId, since: Option<u64>) -> Vec<HealthSnapshot> {
        let Some(ref data_dir) = self.data_dir else {
            return vec![];
        };
        let path = data_dir.join("health_history").join(format!("{}.jsonl", cid));
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return vec![],
        };
        let reader = std::io::BufReader::new(file);
        let cutoff = since.unwrap_or(0);
        reader
            .lines()
            .map_while(Result::ok)
            .filter_map(|line| serde_json::from_str::<HealthSnapshot>(&line).ok())
            .filter(|s| s.timestamp >= cutoff)
            .collect()
    }
}

/// Run the HealthScan loop as a periodic task.
pub async fn run_health_scan_loop(health_scan: Arc<Mutex<HealthScan>>) {
    // Initial delay to let the daemon stabilize
    tokio::time::sleep(Duration::from_secs(10)).await;

    let interval = {
        let hs = health_scan.lock().await;
        hs.scan_interval()
    };

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let mut hs = health_scan.lock().await;
        hs.run_scan().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craftobj_core::ContentId;

    fn make_tx() -> mpsc::UnboundedSender<CraftObjCommand> {
        let (tx, _rx) = mpsc::unbounded_channel();
        tx
    }

    fn setup_test() -> (
        Arc<Mutex<FsStore>>,
        Arc<Mutex<DemandTracker>>,
        PeerId,
        std::path::PathBuf,
    ) {
        let local = PeerId::random();
        let dir = std::env::temp_dir().join(format!(
            "health-scan-test-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        let store = FsStore::new(&dir).unwrap();
        (
            Arc::new(Mutex::new(store)),
            Arc::new(Mutex::new(DemandTracker::new())),
            local,
            dir,
        )
    }

    #[test]
    fn test_health_scan_creation() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let scan = HealthScan::new(store, dt, local, tx);
        assert_eq!(scan.scan_interval(), Duration::from_secs(300));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_set_tier_target() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(store, dt, local, tx);
        scan.set_tier_target(2.0);
        assert_eq!(scan.tier_target, 2.0);
        scan.set_tier_target(1.0); // below minimum, should clamp to 1.5
        assert_eq!(scan.tier_target, 1.5);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_set_scan_interval() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(store, dt, local, tx);
        scan.set_scan_interval(Duration::from_secs(60));
        assert_eq!(scan.scan_interval(), Duration::from_secs(60));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_scan_empty() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(store, dt, local, tx);
        // Should complete without error on empty store
        scan.run_scan().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_health_query_no_response() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx(); // rx dropped immediately — simulates no handler
        let scan = HealthScan::new(store, dt, local, tx);
        let peer = PeerId::random();
        let cid = ContentId([1u8; 32]);
        // HealthQuery with no handler returns 0
        let count = scan.health_query(peer, cid, 0).await;
        assert_eq!(count, 0, "No handler should return 0");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_health_query_response() {
        let (store, dt, local, dir) = setup_test();
        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    CraftObjCommand::HealthQuery { reply_tx, .. } => {
                        let _ = reply_tx.send(Ok(7));
                    }
                    _ => {}
                }
            }
        });

        let scan = HealthScan::new(store, dt, local, tx);
        let peer = PeerId::random();
        let cid = ContentId([2u8; 32]);
        let count = scan.health_query(peer, cid, 0).await;
        assert_eq!(count, 7, "Should return the peer's piece count");
        std::fs::remove_dir_all(&dir).ok();
    }
}
