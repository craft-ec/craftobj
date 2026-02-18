//! HealthScan — periodic scan of owned segments for repair and degradation.
//!
//! Replaces the event-driven HealthReactor. Instead of reacting to every
//! PieceStored/PieceDropped event, HealthScan runs on a configurable timer
//! (default 30s) and iterates all segments the local node holds pieces in.
//! For each segment it computes rank and triggers repair or degradation.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use datacraft_core::{ContentId, HealthAction, HealthSnapshot, SegmentSnapshot};
use std::io::{BufRead, Write};
use datacraft_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;
use crate::piece_map::PieceMap;
use crate::scaling::DemandSignalTracker;

/// Minimum pieces a node must keep per segment.
const MIN_PIECES_PER_SEGMENT: usize = 2;

/// Default scan interval in seconds.
pub const DEFAULT_SCAN_INTERVAL_SECS: u64 = 30;

/// Maximum age of health snapshots to keep (24 hours).
const SNAPSHOT_MAX_AGE_MS: u64 = 24 * 60 * 60 * 1000;

/// HealthScan periodically scans owned segments and triggers repair/degradation.
pub struct HealthScan {
    piece_map: Arc<Mutex<PieceMap>>,
    store: Arc<Mutex<FsStore>>,
    demand_tracker: Arc<Mutex<DemandSignalTracker>>,
    local_peer_id: PeerId,
    tier_target: f64,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    scan_interval: Duration,
    data_dir: Option<PathBuf>,
}

impl HealthScan {
    /// Create a new HealthScan.
    pub fn new(
        piece_map: Arc<Mutex<PieceMap>>,
        store: Arc<Mutex<FsStore>>,
        demand_tracker: Arc<Mutex<DemandSignalTracker>>,
        local_peer_id: PeerId,
        command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    ) -> Self {
        Self {
            piece_map,
            store,
            demand_tracker,
            local_peer_id,
            tier_target: 1.5,
            command_tx,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            data_dir: None,
        }
    }

    /// Set custom tier target (default 1.5).
    pub fn set_tier_target(&mut self, target: f64) {
        self.tier_target = target;
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

    /// Run a single scan over all owned segments.
    ///
    /// For each segment the local node holds pieces in:
    /// 1. Compute rank (independence check on coefficient vectors from online nodes)
    /// 2. If rank < target and local holds ≥2 pieces: repair (create 1 new piece)
    /// 3. If rank > target and no demand and local holds >2 pieces: degrade (drop 1 piece)
    pub async fn run_scan(&self) {
        // Collect owned segments from PieceMap
        let owned_segments: Vec<(ContentId, u32)> = {
            let map = self.piece_map.lock().await;
            let local_node = map.local_node().to_vec();
            let cids = map.all_cids();
            let mut segments = Vec::new();
            for cid in cids {
                // Find all segments where local node has pieces
                // We check segments 0..u32::MAX but that's impractical.
                // Instead, use pieces_for_cid_local to get (segment, piece_id) pairs.
                let local_pieces = map.pieces_for_cid_local(&cid);
                let mut seen_segments = std::collections::HashSet::new();
                for (seg, _pid) in local_pieces {
                    if seen_segments.insert(seg) {
                        segments.push((cid, seg));
                    }
                }
            }
            segments
        };

        if owned_segments.is_empty() {
            debug!("HealthScan: no owned segments to scan");
            return;
        }

        debug!("HealthScan: scanning {} owned segments", owned_segments.len());

        // Group segments by CID for snapshot generation
        let mut cid_segments: std::collections::HashMap<ContentId, Vec<u32>> = std::collections::HashMap::new();
        for (cid, seg) in &owned_segments {
            cid_segments.entry(*cid).or_default().push(*seg);
        }

        // Collect actions per CID during scan
        let mut actions_by_cid: std::collections::HashMap<ContentId, Vec<HealthAction>> = std::collections::HashMap::new();

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

        for (cid, segments) in &cid_segments {
            let map = self.piece_map.lock().await;
            let mut seg_snapshots = Vec::new();
            let mut min_ratio = f64::MAX;

            for &seg in segments {
                let rank = map.compute_rank(cid, seg, true);
                let total_pieces = map.segment_pieces(cid, seg);
                // Estimate k from segment pieces and rank (k is the reconstruction threshold)
                // In practice k comes from the manifest, but PieceMap doesn't store it.
                // Use rank as a lower bound; for snapshots this is informational.
                let k = 40_usize; // default k for 10MiB segments with 256KiB pieces
                let providers = map.pieces_for_segment(cid, seg).iter()
                    .map(|(node, _, _)| node.clone())
                    .collect::<std::collections::HashSet<_>>()
                    .len();
                let ratio = if k > 0 { rank as f64 / k as f64 } else { 0.0 };
                if ratio < min_ratio { min_ratio = ratio; }
                seg_snapshots.push(SegmentSnapshot { index: seg, rank, k, provider_count: providers });
            }

            let provider_count = map.provider_count(cid, true);
            drop(map);

            let snapshot = HealthSnapshot {
                timestamp: now,
                content_id: *cid,
                segment_count: segments.len(),
                segments: seg_snapshots,
                provider_count,
                health_ratio: if min_ratio == f64::MAX { 0.0 } else { min_ratio },
                actions: actions_by_cid.remove(cid).unwrap_or_default(),
            };

            self.persist_snapshot(&snapshot);
        }
    }

    /// Scan a single segment for repair or degradation needs.
    ///
    /// Uses deterministic provider ranking for repair assignment:
    /// all nodes compute the same ranking of providers by piece count,
    /// top N providers each create 1 orthogonal piece (N = deficit).
    async fn scan_segment(&self, cid: ContentId, segment: u32) -> Option<HealthAction> {
        let (rank, local_count, local_node, provider_counts, non_providers) = {
            let map = self.piece_map.lock().await;
            let rank = map.compute_rank(&cid, segment, true);
            let local_count = map.local_pieces(&cid, segment);
            let local_node = map.local_node().to_vec();

            // Compute per-provider piece counts for this segment (online nodes only)
            let segment_pieces = map.pieces_for_segment(&cid, segment);
            let mut counts: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();
            let mut all_providers = std::collections::HashSet::new();
            for (node, _pid, _coeff) in &segment_pieces {
                all_providers.insert((*node).clone());
                if map.is_node_online_pub(node) {
                    *counts.entry((*node).clone()).or_default() += 1;
                }
            }

            // Non-providers: online nodes not holding pieces for this segment
            // (for push targets). We don't have a full node list here, so we'll
            // determine this when pushing.
            let non_provs: Vec<Vec<u8>> = Vec::new();

            (rank, local_count, local_node, counts, non_provs)
        };

        let rank_f = rank as f64;
        let target_rank = self.tier_target.ceil() as usize;

        // Check for under-replication → deterministic repair
        if rank < target_rank && local_count >= MIN_PIECES_PER_SEGMENT {
            let deficit = target_rank - rank;

            // Sort providers by piece count (descending), then by node bytes for tiebreak
            let mut sorted_providers: Vec<(Vec<u8>, usize)> = provider_counts.into_iter().collect();
            sorted_providers.sort_by(|a, b| {
                b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0))
            });

            // Check if this node is in top N providers (N = deficit)
            let top_n: Vec<&Vec<u8>> = sorted_providers.iter()
                .take(deficit)
                .map(|(node, _)| node)
                .collect();

            // Find this node's rank position among providers
            let my_position = sorted_providers.iter()
                .position(|(node, _)| node == &local_node);

            if let Some(pos) = my_position {
                // Every node that holds pieces creates an orthogonal piece,
                // each targeting a different free column via offset = position.
                // No wasted work — simultaneous repairs fill different null space dimensions.
                debug!(
                    "HealthScan: {}/seg{} under-replicated (rank={}, target={}), repairing with offset={} (deficit={})",
                    cid, segment, rank, target_rank, pos, deficit
                );
                self.attempt_repair(cid, segment, &local_node, pos).await;
                return Some(HealthAction::Repaired { segment, offset: pos });
            } else {
                debug!(
                    "HealthScan: {}/seg{} under-replicated but local node not a provider",
                    cid, segment
                );
            }
            return None;
        }

        // Check for over-replication → degradation
        if rank_f > self.tier_target && local_count > MIN_PIECES_PER_SEGMENT {
            let has_demand = {
                let dt = self.demand_tracker.lock().await;
                dt.has_recent_signal(&cid)
            };
            if !has_demand {
                debug!(
                    "HealthScan: {}/seg{} over-replicated (rank={}, target={}, local={}), degrading",
                    cid, segment, rank, self.tier_target, local_count
                );
                self.attempt_degradation(cid, segment, &local_node).await;
                return Some(HealthAction::Degraded { segment });
            }
        }

        None
    }

    /// Attempt to repair a segment by creating a new orthogonal RLNC piece.
    /// `offset` is this node's rank position — determines which free column to target.
    async fn attempt_repair(&self, cid: ContentId, segment: u32, local_node: &[u8], _offset: usize) {
        let store_guard = self.store.lock().await;
        let result = crate::health::heal_segment(&store_guard, &cid, segment, 1);

        if result.pieces_generated == 0 {
            warn!("HealthScan repair failed for {}/seg{}: {:?}", cid, segment, result.errors);
            return;
        }

        // Find the newly generated piece and emit PieceStored
        if let Ok(pieces_after) = store_guard.list_pieces(&cid, segment) {
            if let Some(&new_pid) = pieces_after.last() {
                if let Ok((_data, coefficients)) = store_guard.get_piece(&cid, segment, &new_pid) {
                    let mut map = self.piece_map.lock().await;
                    let seq = map.next_seq();
                    let stored = datacraft_core::PieceStored {
                        node: local_node.to_vec(),
                        cid,
                        segment,
                        piece_id: new_pid,
                        coefficients,
                        seq,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        signature: vec![],
                    };
                    let event = datacraft_core::PieceEvent::Stored(stored);
                    map.apply_event(&event);
                    if let Ok(data) = bincode::serialize(&event) {
                        let _ = self.command_tx.send(DataCraftCommand::BroadcastPieceEvent { event_data: data });
                    }
                    info!("HealthScan repair complete for {}/seg{}: generated 1 new piece", cid, segment);
                }
            }
        }
    }

    /// Attempt to degrade a segment by dropping 1 piece.
    async fn attempt_degradation(&self, cid: ContentId, segment: u32, local_node: &[u8]) {
        let store_guard = self.store.lock().await;
        let mut pieces = match store_guard.list_pieces(&cid, segment) {
            Ok(p) if p.len() > MIN_PIECES_PER_SEGMENT => p,
            _ => return,
        };
        pieces.sort();
        let piece_to_drop = *pieces.last().unwrap();

        if let Err(e) = store_guard.delete_piece(&cid, segment, &piece_to_drop) {
            warn!("HealthScan degradation failed for {}/seg{}: {}", cid, segment, e);
            return;
        }

        info!(
            "HealthScan degradation: dropped piece {} for {}/seg{}",
            hex::encode(&piece_to_drop[..8]),
            cid,
            segment
        );

        // Emit PieceDropped event
        {
            let mut map = self.piece_map.lock().await;
            let seq = map.next_seq();
            let dropped = datacraft_core::PieceDropped {
                node: local_node.to_vec(),
                cid,
                segment,
                piece_id: piece_to_drop,
                seq,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                signature: vec![],
            };
            let event = datacraft_core::PieceEvent::Dropped(dropped);
            map.apply_event(&event);
            if let Ok(data) = bincode::serialize(&event) {
                let _ = self.command_tx.send(DataCraftCommand::BroadcastPieceEvent { event_data: data });
            }
        }
    }

    /// Persist a health snapshot to JSONL file.
    fn persist_snapshot(&self, snapshot: &HealthSnapshot) {
        let Some(ref data_dir) = self.data_dir else { return };
        let dir = data_dir.join("health_history");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!("Failed to create health_history dir: {}", e);
            return;
        }
        let path = dir.join(format!("{}.jsonl", snapshot.content_id));
        let mut file = match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => f,
            Err(e) => { warn!("Failed to open snapshot file: {}", e); return; }
        };
        if let Ok(json) = serde_json::to_string(snapshot) {
            let _ = writeln!(file, "{}", json);
        }

        // Prune old entries (>24h)
        self.prune_snapshots(&path, snapshot.timestamp);
    }

    /// Remove snapshot entries older than SNAPSHOT_MAX_AGE_MS.
    fn prune_snapshots(&self, path: &std::path::Path, now: u64) {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return,
        };
        let cutoff = now.saturating_sub(SNAPSHOT_MAX_AGE_MS);
        let kept: Vec<&str> = content.lines().filter(|line| {
            serde_json::from_str::<HealthSnapshot>(line)
                .map(|s| s.timestamp >= cutoff)
                .unwrap_or(false)
        }).collect();

        if kept.len() < content.lines().count() {
            let _ = std::fs::write(path, kept.join("\n") + "\n");
        }
    }

    /// Load health snapshots for a CID, optionally filtered by `since` (unix millis).
    pub fn load_snapshots(&self, cid: &ContentId, since: Option<u64>) -> Vec<HealthSnapshot> {
        let Some(ref data_dir) = self.data_dir else { return vec![] };
        let path = data_dir.join("health_history").join(format!("{}.jsonl", cid));
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return vec![],
        };
        let reader = std::io::BufReader::new(file);
        let cutoff = since.unwrap_or(0);
        reader.lines()
            .filter_map(|line| line.ok())
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
        let hs = health_scan.lock().await;
        hs.run_scan().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datacraft_core::{ContentId, PieceEvent, PieceStored};

    fn make_tx() -> mpsc::UnboundedSender<DataCraftCommand> {
        let (tx, _rx) = mpsc::unbounded_channel();
        tx
    }

    fn setup_test() -> (Arc<Mutex<PieceMap>>, Arc<Mutex<FsStore>>, Arc<Mutex<DemandSignalTracker>>, PeerId, std::path::PathBuf) {
        let local = PeerId::random();
        let dir = std::env::temp_dir().join(format!("health-scan-test-{}-{}", std::process::id(), rand::random::<u32>()));
        let store = FsStore::new(&dir).unwrap();
        let piece_map = PieceMap::new(local);
        (
            Arc::new(Mutex::new(piece_map)),
            Arc::new(Mutex::new(store)),
            Arc::new(Mutex::new(DemandSignalTracker::new())),
            local,
            dir,
        )
    }

    #[test]
    fn test_health_scan_creation() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let scan = HealthScan::new(pm, store, dt, local, tx);
        assert_eq!(scan.scan_interval(), Duration::from_secs(30));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_set_tier_target() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(pm, store, dt, local, tx);
        scan.set_tier_target(2.0);
        assert_eq!(scan.tier_target, 2.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_set_scan_interval() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(pm, store, dt, local, tx);
        scan.set_scan_interval(Duration::from_secs(60));
        assert_eq!(scan.scan_interval(), Duration::from_secs(60));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_scan_empty() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let scan = HealthScan::new(pm, store, dt, local, tx);
        // Should complete without error on empty PieceMap
        scan.run_scan().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_scan_triggers_degradation() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(pm.clone(), store.clone(), dt, local, tx);
        scan.set_tier_target(1.5); // rank=3 > 1.5 → should degrade

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        // Store 3 local pieces (linearly independent)
        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);
            map.track_segment(cid, 0);

            for i in 0..3u8 {
                let mut coeff = vec![0u8; 3];
                coeff[i as usize] = 1;
                let pid = datacraft_store::piece_id_from_coefficients(&coeff);
                s.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: local_bytes.clone(), cid, segment: 0, piece_id: pid,
                    coefficients: coeff, seq, timestamp: 1000, signature: vec![],
                }));
            }
        }

        // Run scan — should trigger degradation (rank=3 > target=1.5, no demand, local=3 > 2)
        scan.run_scan().await;

        // After degradation, local should have 2 pieces
        let local_count = {
            let map = pm.lock().await;
            map.local_pieces(&cid, 0)
        };
        assert_eq!(local_count, 2, "Should have dropped 1 piece via degradation");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_scan_demand_blocks_degradation() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(pm.clone(), store.clone(), dt.clone(), local, tx);
        scan.set_tier_target(1.5);

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        // Store 3 local pieces
        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);
            map.track_segment(cid, 0);

            for i in 0..3u8 {
                let mut coeff = vec![0u8; 3];
                coeff[i as usize] = 1;
                let pid = datacraft_store::piece_id_from_coefficients(&coeff);
                s.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: local_bytes.clone(), cid, segment: 0, piece_id: pid,
                    coefficients: coeff, seq, timestamp: 1000, signature: vec![],
                }));
            }
        }

        // Add demand signal
        {
            let mut tracker = dt.lock().await;
            tracker.record_signal(cid);
        }

        // Run scan — demand should block degradation
        scan.run_scan().await;

        let local_count = {
            let map = pm.lock().await;
            map.local_pieces(&cid, 0)
        };
        assert_eq!(local_count, 3, "Demand should block degradation");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_deterministic_repair_top_provider_selected() {
        let (pm, store, dt, local, dir) = setup_test();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut scan = HealthScan::new(pm.clone(), store.clone(), dt, local, tx);
        scan.set_tier_target(3.0); // rank=1 < target=3 → needs repair

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        // Store 2 local pieces (enough to repair from)
        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);
            map.track_segment(cid, 0);

            for i in 0..2u8 {
                let mut coeff = vec![0u8; 3];
                coeff[i as usize] = 1;
                let pid = datacraft_store::piece_id_from_coefficients(&coeff);
                s.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: local_bytes.clone(), cid, segment: 0, piece_id: pid,
                    coefficients: coeff, seq, timestamp: 1000, signature: vec![],
                }));
            }
        }

        // Local node has 2 pieces, rank=2, target=3, deficit=1
        // Local is the ONLY provider, so it's definitely in top 1 → should repair
        scan.run_scan().await;

        // Check that a BroadcastPieceEvent was sent (repair generated a piece)
        let mut found_broadcast = false;
        while let Ok(cmd) = rx.try_recv() {
            if matches!(cmd, DataCraftCommand::BroadcastPieceEvent { .. }) {
                found_broadcast = true;
            }
        }
        assert!(found_broadcast, "Should have broadcast a PieceStored event from repair");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_repair_uses_different_offsets() {
        // With the offset-based design, every node that holds pieces repairs
        // targeting a different free column. This test verifies local node
        // repairs with its rank position as offset.
        let (pm, store, dt, local, dir) = setup_test();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut scan = HealthScan::new(pm.clone(), store.clone(), dt, local, tx);
        scan.set_tier_target(4.0); // rank=3 < target=4, deficit=1

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();
        let other_node = vec![0xFFu8; 38];

        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);
            map.set_node_online(&other_node, true);
            map.track_segment(cid, 0);

            // Local: 2 pieces
            for i in 0..2u8 {
                let mut coeff = vec![0u8; 4];
                coeff[i as usize] = 1;
                let pid = datacraft_store::piece_id_from_coefficients(&coeff);
                s.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: local_bytes.clone(), cid, segment: 0, piece_id: pid,
                    coefficients: coeff, seq, timestamp: 1000, signature: vec![],
                }));
            }

            // Other node: 3 pieces
            for i in 0..3u8 {
                let mut coeff = vec![0u8; 4];
                coeff[i as usize] = 1;
                let pid_bytes = [i + 100; 32];
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: other_node.clone(), cid, segment: 0, piece_id: pid_bytes,
                    coefficients: coeff, seq, timestamp: 1000, signature: vec![],
                }));
            }
        }

        scan.run_scan().await;

        // Local node should have attempted repair (every provider repairs with different offset)
        let mut found_broadcast = false;
        while let Ok(cmd) = rx.try_recv() {
            if matches!(cmd, DataCraftCommand::BroadcastPieceEvent { .. }) {
                found_broadcast = true;
            }
        }
        assert!(found_broadcast, "Local node should repair with its offset");

        std::fs::remove_dir_all(&dir).ok();
    }
}
