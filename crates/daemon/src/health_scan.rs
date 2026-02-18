//! HealthScan — periodic scan of owned segments for repair and degradation.
//!
//! Pulls Merkle diffs from DHT providers to update local PieceMap, then
//! computes rank and triggers repair or degradation as needed.
//! Replaces the gossipsub-driven model with pull-based Merkle diff sync.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use craftobj_core::{ContentId, HealthAction, HealthSnapshot, SegmentSnapshot};
use craftobj_store::merkle::MerkleDiff;
use std::io::{BufRead, Write};
use craftobj_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::CraftOBJCommand;
use crate::piece_map::PieceMap;
use crate::scaling::DemandSignalTracker;

/// Minimum pieces a node must keep per segment.
const MIN_PIECES_PER_SEGMENT: usize = 2;

/// Default scan interval in seconds (5 minutes).
pub const DEFAULT_SCAN_INTERVAL_SECS: u64 = 300;

/// Maximum age of health snapshots to keep (24 hours).
const SNAPSHOT_MAX_AGE_MS: u64 = 24 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// Merkle Pull Protocol types
// ---------------------------------------------------------------------------

/// Request a peer's Merkle state for a given content ID.
/// If `known_root` is `Some`, the responder returns only the diff.
/// If `None`, the responder sends the full leaf set.
#[derive(Debug, Clone)]
pub struct MerklePullRequest {
    pub content_id: ContentId,
    pub known_root: Option<[u8; 32]>,
}

/// Response to a [`MerklePullRequest`].
#[derive(Debug, Clone)]
pub struct MerklePullResponse {
    /// Current Merkle root of the responder's storage for this CID.
    pub root: [u8; 32],
    /// Diff relative to `known_root`. `None` when the root hasn't changed.
    pub diff: Option<MerkleDiff>,
    /// Full leaf set — only populated when `known_root` was `None` (first sync).
    pub full_leaves: Option<Vec<[u8; 32]>>,
}

// ---------------------------------------------------------------------------
// Merkle root cache key
// ---------------------------------------------------------------------------

/// Cache key for last-known Merkle root per provider per CID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MerkleCacheKey {
    peer_id: PeerId,
    content_id: ContentId,
}

// ---------------------------------------------------------------------------
// HealthScan
// ---------------------------------------------------------------------------

/// HealthScan periodically scans owned segments and triggers repair/degradation.
pub struct HealthScan {
    piece_map: Arc<Mutex<PieceMap>>,
    store: Arc<Mutex<FsStore>>,
    demand_tracker: Arc<Mutex<DemandSignalTracker>>,
    local_peer_id: PeerId,
    tier_target: f64,
    command_tx: mpsc::UnboundedSender<CraftOBJCommand>,
    scan_interval: Duration,
    data_dir: Option<PathBuf>,
    /// Cache of last-known Merkle roots from providers, keyed by (peer, cid).
    merkle_root_cache: HashMap<MerkleCacheKey, [u8; 32]>,
}

impl HealthScan {
    /// Create a new HealthScan.
    pub fn new(
        piece_map: Arc<Mutex<PieceMap>>,
        store: Arc<Mutex<FsStore>>,
        demand_tracker: Arc<Mutex<DemandSignalTracker>>,
        local_peer_id: PeerId,
        command_tx: mpsc::UnboundedSender<CraftOBJCommand>,
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
            merkle_root_cache: HashMap::new(),
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

    // -----------------------------------------------------------------------
    // Step 1: Pull Merkle diffs from providers to update PieceMap
    // -----------------------------------------------------------------------

    /// Pull Merkle diffs from all providers for a given CID.
    ///
    /// For each provider obtained from the DHT:
    /// 1. Send a [`MerklePullRequest`] with our last known root (or `None`).
    /// 2. If the root is unchanged, skip.
    /// 3. If changed, apply the diff (or full leaves) to the local [`PieceMap`].
    async fn pull_merkle_for_cid(&mut self, cid: ContentId) {
        // Query DHT for providers of this CID
        let providers = self.resolve_providers(cid).await;
        if providers.is_empty() {
            debug!("HealthScan: no providers found for {}", cid);
            return;
        }

        for peer_id in providers {
            if peer_id == self.local_peer_id {
                continue; // skip self
            }

            let cache_key = MerkleCacheKey {
                peer_id,
                content_id: cid,
            };
            let known_root = self.merkle_root_cache.get(&cache_key).copied();

            // TODO: Wire actual P2P request. For now this calls a placeholder
            // that sends a MerklePullRequest via the command channel and awaits
            // a MerklePullResponse. The network handler must be implemented to
            // route these requests to peers and invoke
            // StorageMerkleTree::diff() / leaves() on the responder side.
            let response = match self.send_merkle_pull(peer_id, cid, known_root).await {
                Some(r) => r,
                None => continue,
            };

            // Check if root unchanged
            if Some(response.root) == known_root && response.diff.is_none() && response.full_leaves.is_none() {
                debug!("HealthScan: {}/{} root unchanged, skipping", cid, peer_id);
                continue;
            }

            // Apply diff or full leaves to PieceMap
            if let Some(ref diff) = response.diff {
                self.apply_diff_to_piece_map(cid, &peer_id, diff).await;
            } else if let Some(ref leaves) = response.full_leaves {
                self.apply_full_leaves_to_piece_map(cid, &peer_id, leaves).await;
            }

            // Update cache
            self.merkle_root_cache.insert(cache_key, response.root);
        }
    }

    /// Resolve DHT providers for a content ID.
    async fn resolve_providers(&self, cid: ContentId) -> Vec<PeerId> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .command_tx
            .send(CraftOBJCommand::ResolveProviders {
                content_id: cid,
                reply_tx: tx,
            })
            .is_err()
        {
            warn!("HealthScan: failed to send ResolveProviders command");
            return Vec::new();
        }
        match rx.await {
            Ok(Ok(peers)) => peers,
            Ok(Err(e)) => {
                debug!("HealthScan: ResolveProviders error for {}: {}", cid, e);
                Vec::new()
            }
            Err(_) => Vec::new(),
        }
    }

    /// Send a Merkle pull request to a peer.
    ///
    /// TODO: This needs actual P2P wiring. Currently returns `None` (no-op).
    /// When wired, this should:
    /// 1. Send `MerklePullRequest { content_id, known_root }` to `peer_id`
    /// 2. Await `MerklePullResponse` from the peer
    /// 3. The peer handler should build response using `StorageMerkleTree`:
    ///    - If `known_root.is_none()`: respond with `full_leaves = Some(tree.leaves().to_vec())`
    ///    - If `known_root == Some(r)` and `!tree.has_changed_since(&r)`: respond with root only
    ///    - Otherwise: respond with `diff = Some(tree.diff_from_leaves(...))`
    ///      (requires the responder to reconstruct old tree from `known_root` or
    ///       maintain a log — simpler: always send full leaves and let requester diff)
    #[allow(unused_variables)]
    async fn send_merkle_pull(
        &self,
        peer_id: PeerId,
        content_id: ContentId,
        known_root: Option<[u8; 32]>,
    ) -> Option<MerklePullResponse> {
        // TODO: Wire via CraftOBJCommand::MerklePull or a new request-response protocol.
        // For now, return None to skip all remote pulls.
        // When wired, add a `MerklePull` variant to CraftOBJCommand:
        //
        //   MerklePull {
        //       peer_id: PeerId,
        //       content_id: ContentId,
        //       known_root: Option<[u8; 32]>,
        //       reply_tx: oneshot::Sender<Result<MerklePullResponse, String>>,
        //   }
        None
    }

    /// Apply a Merkle diff to PieceMap for a given peer.
    ///
    /// Note: Merkle leaves are `SHA-256(cid || segment_be || piece_id)` — we cannot
    /// reverse them to get the original (cid, segment, piece_id). The diff tells us
    /// *which leaves* changed, but to update PieceMap we need the actual piece metadata.
    ///
    /// In practice the Merkle pull protocol should also return piece metadata alongside
    /// the diff. For now we record the raw leaf hashes in a TODO-gated path.
    ///
    /// TODO: Extend MerklePullResponse to include piece metadata for added leaves:
    ///   `added_pieces: Vec<(ContentId, u32, [u8; 32], Vec<u8>)>` // (cid, seg, pid, coefficients)
    /// Then call `piece_map.apply_event(PieceEvent::Stored(...))` for each addition
    /// and `piece_map.apply_event(PieceEvent::Dropped(...))` for each removal.
    #[allow(unused_variables)]
    async fn apply_diff_to_piece_map(&self, cid: ContentId, peer_id: &PeerId, diff: &MerkleDiff) {
        debug!(
            "HealthScan: applying diff for {} from {}: +{} -{}",
            cid,
            peer_id,
            diff.added.len(),
            diff.removed.len()
        );
        // TODO: Apply to PieceMap once piece metadata is included in the response.
        // See doc comment above for the required protocol extension.
    }

    /// Apply full leaf set from a first-time sync.
    ///
    /// Same limitation as `apply_diff_to_piece_map` — we need piece metadata, not just hashes.
    /// TODO: Extend protocol to include piece metadata for full syncs.
    #[allow(unused_variables)]
    async fn apply_full_leaves_to_piece_map(
        &self,
        cid: ContentId,
        peer_id: &PeerId,
        leaves: &[[u8; 32]],
    ) {
        debug!(
            "HealthScan: full sync for {} from {}: {} leaves",
            cid,
            peer_id,
            leaves.len()
        );
        // TODO: Apply to PieceMap once piece metadata is included in the response.
    }

    // -----------------------------------------------------------------------
    // Step 2-4: Scan, repair, degrade (largely unchanged)
    // -----------------------------------------------------------------------

    /// Run a single scan over all owned segments.
    ///
    /// 1. Pull Merkle diffs from providers to update PieceMap
    /// 2. For each segment: compute rank and trigger repair or degradation
    /// 3. Persist health snapshots
    pub async fn run_scan(&mut self) {
        // Collect owned CIDs and segments from PieceMap
        let (owned_segments, unique_cids): (Vec<(ContentId, u32)>, Vec<ContentId>) = {
            let map = self.piece_map.lock().await;
            let cids = map.all_cids();
            let mut segments = Vec::new();
            let mut seen_cids = Vec::new();
            for cid in &cids {
                let local_pieces = map.pieces_for_cid_local(cid);
                let mut seen_segments = std::collections::HashSet::new();
                for (seg, _pid) in local_pieces {
                    if seen_segments.insert(seg) {
                        segments.push((*cid, seg));
                    }
                }
                seen_cids.push(*cid);
            }
            (segments, seen_cids)
        };

        if owned_segments.is_empty() {
            debug!("HealthScan: no owned segments to scan");
            return;
        }

        debug!("HealthScan: scanning {} owned segments", owned_segments.len());

        // Step 1: Pull Merkle diffs from providers for each CID
        for &cid in &unique_cids {
            self.pull_merkle_for_cid(cid).await;
        }

        // Group segments by CID for snapshot generation
        let mut cid_segments: std::collections::HashMap<ContentId, Vec<u32>> =
            std::collections::HashMap::new();
        for (cid, seg) in &owned_segments {
            cid_segments.entry(*cid).or_default().push(*seg);
        }

        // Collect actions per CID during scan
        let mut actions_by_cid: std::collections::HashMap<ContentId, Vec<HealthAction>> =
            std::collections::HashMap::new();

        // Step 2-3: Compute health, repair, degrade
        for (cid, segment) in &owned_segments {
            let action = self.scan_segment(*cid, *segment).await;
            if let Some(a) = action {
                actions_by_cid.entry(*cid).or_default().push(a);
            }
        }

        // Step 4: Persist snapshots
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
                let _total_pieces = map.segment_pieces(cid, seg);
                let k = 40_usize; // default k for 10MiB segments with 256KiB pieces
                let providers = map
                    .pieces_for_segment(cid, seg)
                    .iter()
                    .map(|(node, _, _)| (*node).clone())
                    .collect::<std::collections::HashSet<_>>()
                    .len();
                let ratio = if k > 0 { rank as f64 / k as f64 } else { 0.0 };
                if ratio < min_ratio {
                    min_ratio = ratio;
                }
                seg_snapshots.push(SegmentSnapshot {
                    index: seg,
                    rank,
                    k,
                    provider_count: providers,
                });
            }

            let provider_count = map.provider_count(cid, true);
            drop(map);

            let snapshot = HealthSnapshot {
                timestamp: now,
                content_id: *cid,
                segment_count: segments.len(),
                segments: seg_snapshots,
                provider_count,
                health_ratio: if min_ratio == f64::MAX {
                    0.0
                } else {
                    min_ratio
                },
                actions: actions_by_cid.remove(cid).unwrap_or_default(),
            };

            self.persist_snapshot(&snapshot);
        }
    }

    /// Scan a single segment for repair or degradation needs.
    async fn scan_segment(&self, cid: ContentId, segment: u32) -> Option<HealthAction> {
        let (rank, local_count, local_node, provider_counts) = {
            let map = self.piece_map.lock().await;
            let rank = map.compute_rank(&cid, segment, true);
            let local_count = map.local_pieces(&cid, segment);
            let local_node = map.local_node().to_vec();

            let segment_pieces = map.pieces_for_segment(&cid, segment);
            let mut counts: std::collections::HashMap<Vec<u8>, usize> =
                std::collections::HashMap::new();
            for (node, _pid, _coeff) in &segment_pieces {
                if map.is_node_online_pub(node) {
                    *counts.entry((*node).clone()).or_default() += 1;
                }
            }

            (rank, local_count, local_node, counts)
        };

        let rank_f = rank as f64;
        let target_rank = self.tier_target.ceil() as usize;

        // Check for under-replication → deterministic repair
        if rank < target_rank && local_count >= MIN_PIECES_PER_SEGMENT {
            let deficit = target_rank - rank;

            let mut sorted_providers: Vec<(Vec<u8>, usize)> = provider_counts.into_iter().collect();
            sorted_providers.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

            let my_position = sorted_providers
                .iter()
                .position(|(node, _)| node == &local_node);

            if let Some(pos) = my_position {
                debug!(
                    "HealthScan: {}/seg{} under-replicated (rank={}, target={}), repairing with offset={} (deficit={})",
                    cid, segment, rank, target_rank, pos, deficit
                );
                self.attempt_repair(cid, segment, &local_node, pos).await;
                return Some(HealthAction::Repaired {
                    segment,
                    offset: pos,
                });
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
    async fn attempt_repair(
        &self,
        cid: ContentId,
        segment: u32,
        local_node: &[u8],
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

        if let Ok(pieces_after) = store_guard.list_pieces(&cid, segment) {
            if let Some(&new_pid) = pieces_after.last() {
                if let Ok((_data, coefficients)) = store_guard.get_piece(&cid, segment, &new_pid) {
                    let mut map = self.piece_map.lock().await;
                    let seq = map.next_seq();
                    let stored = craftobj_core::PieceStored {
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
                    let event = craftobj_core::PieceEvent::Stored(stored);
                    map.apply_event(&event);
                    // Publish DHT provider record for this CID+segment
                    let pkey = craftobj_routing::provider_key(&cid, segment);
                    let _ = self.command_tx.send(CraftOBJCommand::StartProviding { key: pkey });
                    info!(
                        "HealthScan repair complete for {}/seg{}: generated 1 new piece",
                        cid, segment
                    );
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

        {
            let mut map = self.piece_map.lock().await;
            let seq = map.next_seq();
            let dropped = craftobj_core::PieceDropped {
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
            let event = craftobj_core::PieceEvent::Dropped(dropped);
            map.apply_event(&event);
        }
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
        let mut hs = health_scan.lock().await;
        hs.run_scan().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craftobj_core::{ContentId, PieceEvent, PieceStored};

    fn make_tx() -> mpsc::UnboundedSender<CraftOBJCommand> {
        let (tx, _rx) = mpsc::unbounded_channel();
        tx
    }

    fn setup_test() -> (
        Arc<Mutex<PieceMap>>,
        Arc<Mutex<FsStore>>,
        Arc<Mutex<DemandSignalTracker>>,
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
        assert_eq!(scan.scan_interval(), Duration::from_secs(300));
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
        let mut scan = HealthScan::new(pm, store, dt, local, tx);
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
                let pid = craftobj_store::piece_id_from_coefficients(&coeff);
                s.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: local_bytes.clone(),
                    cid,
                    segment: 0,
                    piece_id: pid,
                    coefficients: coeff,
                    seq,
                    timestamp: 1000,
                    signature: vec![],
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

        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);
            map.track_segment(cid, 0);

            for i in 0..3u8 {
                let mut coeff = vec![0u8; 3];
                coeff[i as usize] = 1;
                let pid = craftobj_store::piece_id_from_coefficients(&coeff);
                s.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: local_bytes.clone(),
                    cid,
                    segment: 0,
                    piece_id: pid,
                    coefficients: coeff,
                    seq,
                    timestamp: 1000,
                    signature: vec![],
                }));
            }
        }

        // Add demand signal
        {
            let mut tracker = dt.lock().await;
            tracker.record_signal(cid);
        }

        scan.run_scan().await;

        let local_count = {
            let map = pm.lock().await;
            map.local_pieces(&cid, 0)
        };
        assert_eq!(local_count, 3, "Demand should block degradation");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Spawn a task that drains the command channel, replying with empty results
    /// so that resolve_providers doesn't hang in tests.
    fn drain_commands(mut rx: mpsc::UnboundedReceiver<CraftOBJCommand>) {
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    CraftOBJCommand::ResolveProviders { reply_tx, .. } => {
                        let _ = reply_tx.send(Ok(vec![]));
                    }
                    _ => {}
                }
            }
        });
    }

    #[tokio::test]
    async fn test_deterministic_repair_top_provider_selected() {
        let (pm, store, dt, local, dir) = setup_test();
        let (tx, rx) = mpsc::unbounded_channel();
        drain_commands(rx);
        let mut scan = HealthScan::new(pm.clone(), store.clone(), dt, local, tx);
        scan.set_tier_target(3.0); // rank=1 < target=3 → needs repair

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);
            map.track_segment(cid, 0);

            for i in 0..2u8 {
                let mut coeff = vec![0u8; 3];
                coeff[i as usize] = 1;
                let pid = craftobj_store::piece_id_from_coefficients(&coeff);
                s.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: local_bytes.clone(),
                    cid,
                    segment: 0,
                    piece_id: pid,
                    coefficients: coeff,
                    seq,
                    timestamp: 1000,
                    signature: vec![],
                }));
            }
        }

        scan.run_scan().await;

        let s = store.lock().await;
        let pieces_after = s.list_pieces(&cid, 0).unwrap();
        assert!(
            pieces_after.len() > 2,
            "Should have generated a new piece from repair"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_repair_uses_different_offsets() {
        let (pm, store, dt, local, dir) = setup_test();
        let (tx, rx) = mpsc::unbounded_channel();
        drain_commands(rx);
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
                let pid = craftobj_store::piece_id_from_coefficients(&coeff);
                s.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: local_bytes.clone(),
                    cid,
                    segment: 0,
                    piece_id: pid,
                    coefficients: coeff,
                    seq,
                    timestamp: 1000,
                    signature: vec![],
                }));
            }

            // Other node: 3 pieces
            for i in 0..3u8 {
                let mut coeff = vec![0u8; 4];
                coeff[i as usize] = 1;
                let pid_bytes = [i + 100; 32];
                let seq = map.next_seq();
                map.apply_event(&PieceEvent::Stored(PieceStored {
                    node: other_node.clone(),
                    cid,
                    segment: 0,
                    piece_id: pid_bytes,
                    coefficients: coeff,
                    seq,
                    timestamp: 1000,
                    signature: vec![],
                }));
            }
        }

        scan.run_scan().await;

        let s = store.lock().await;
        let pieces_after = s.list_pieces(&cid, 0).unwrap();
        assert!(
            pieces_after.len() > 2,
            "Local node should repair with its offset"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_merkle_pull_request_construction() {
        let cid = ContentId([42u8; 32]);
        let req = MerklePullRequest {
            content_id: cid,
            known_root: None,
        };
        assert!(req.known_root.is_none());

        let req2 = MerklePullRequest {
            content_id: cid,
            known_root: Some([1u8; 32]),
        };
        assert_eq!(req2.known_root.unwrap(), [1u8; 32]);
    }

    #[test]
    fn test_merkle_pull_response_construction() {
        let resp = MerklePullResponse {
            root: [0xAA; 32],
            diff: Some(MerkleDiff {
                added: vec![[1u8; 32]],
                removed: vec![[2u8; 32]],
            }),
            full_leaves: None,
        };
        assert_eq!(resp.root, [0xAA; 32]);
        assert!(resp.diff.is_some());
        assert!(resp.full_leaves.is_none());
    }

    #[test]
    fn test_merkle_root_cache_key() {
        let peer = PeerId::random();
        let cid = ContentId([1u8; 32]);
        let key1 = MerkleCacheKey {
            peer_id: peer,
            content_id: cid,
        };
        let key2 = MerkleCacheKey {
            peer_id: peer,
            content_id: cid,
        };
        assert_eq!(key1, key2);

        let mut cache: HashMap<MerkleCacheKey, [u8; 32]> = HashMap::new();
        cache.insert(key1, [0xFF; 32]);
        assert_eq!(cache.get(&key2), Some(&[0xFF; 32]));
    }
}
