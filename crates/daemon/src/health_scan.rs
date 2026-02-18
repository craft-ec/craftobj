//! HealthScan — periodic scan of owned segments for repair and degradation.
//!
//! Replaces the event-driven HealthReactor. Instead of reacting to every
//! PieceStored/PieceDropped event, HealthScan runs on a configurable timer
//! (default 30s) and iterates all segments the local node holds pieces in.
//! For each segment it computes rank and triggers repair or degradation.

use std::sync::Arc;
use std::time::Duration;

use datacraft_core::ContentId;
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

/// HealthScan periodically scans owned segments and triggers repair/degradation.
pub struct HealthScan {
    piece_map: Arc<Mutex<PieceMap>>,
    store: Arc<Mutex<FsStore>>,
    demand_tracker: Arc<Mutex<DemandSignalTracker>>,
    local_peer_id: PeerId,
    tier_target: f64,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    scan_interval: Duration,
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

        for (cid, segment) in owned_segments {
            self.scan_segment(cid, segment).await;
        }
    }

    /// Scan a single segment for repair or degradation needs.
    async fn scan_segment(&self, cid: ContentId, segment: u32) {
        let (rank, local_count, local_node) = {
            let map = self.piece_map.lock().await;
            let rank = map.compute_rank(&cid, segment, true);
            let local_count = map.local_pieces(&cid, segment);
            let local_node = map.local_node().to_vec();
            (rank, local_count, local_node)
        };

        let rank_f = rank as f64;

        // Check for under-replication → repair
        if rank_f < self.tier_target && local_count >= MIN_PIECES_PER_SEGMENT {
            debug!(
                "HealthScan: {}/seg{} under-replicated (rank={}, target={}), attempting repair",
                cid, segment, rank, self.tier_target
            );
            self.attempt_repair(cid, segment, &local_node).await;
            return;
        }

        // Check for over-replication → degradation
        if rank_f > self.tier_target && local_count > MIN_PIECES_PER_SEGMENT {
            // Check demand before degrading
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
            }
        }
    }

    /// Attempt to repair a segment by creating a new RLNC piece.
    async fn attempt_repair(&self, cid: ContentId, segment: u32, local_node: &[u8]) {
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
}
