//! HealthReactor — reactive repair and degradation based on PieceMap changes.
//!
//! Replaces the signal-based RepairCoordinator and DegradationCoordinator.
//! Every node watches its own PieceMap for rank changes and independently
//! schedules repair (when under-replicated) or degradation (when over-replicated).
//! Random delays prevent duplicate work across nodes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datacraft_core::ContentId;
use datacraft_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;
use crate::piece_map::PieceMap;
use crate::scaling::DemandSignalTracker;

/// Minimum pieces a node must keep per segment.
const MIN_PIECES_PER_SEGMENT: usize = 2;

/// HealthReactor watches PieceMap changes and schedules repair/degradation.
pub struct HealthReactor {
    piece_map: Arc<Mutex<PieceMap>>,
    store: Arc<Mutex<FsStore>>,
    pending_repairs: HashMap<(ContentId, u32), JoinHandle<()>>,
    pending_degradations: HashMap<(ContentId, u32), JoinHandle<()>>,
    demand_tracker: Arc<Mutex<DemandSignalTracker>>,
    local_peer_id: PeerId,
    tier_target: f64,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
}

impl HealthReactor {
    /// Create a new HealthReactor.
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
            pending_repairs: HashMap::new(),
            pending_degradations: HashMap::new(),
            demand_tracker,
            local_peer_id,
            tier_target: 1.5,
            command_tx,
        }
    }

    /// Set custom tier target (default 1.5).
    pub fn set_tier_target(&mut self, target: f64) {
        self.tier_target = target;
    }

    /// Called when a PieceDropped event is processed in the PieceMap.
    /// Checks if repair is needed for the affected segment.
    pub fn on_piece_dropped(&mut self, cid: ContentId, segment: u32) {
        let key = (cid, segment);

        // If we already have a pending repair, skip
        if self.pending_repairs.contains_key(&key) {
            debug!("Already have pending repair for {}/seg{}", cid, segment);
            return;
        }

        // Cancel any pending degradation — rank just dropped
        if let Some(handle) = self.pending_degradations.remove(&key) {
            handle.abort();
            debug!("Cancelled pending degradation for {}/seg{} due to PieceDropped", cid, segment);
        }

        let piece_map = self.piece_map.clone();
        let store = self.store.clone();
        let tier_target = self.tier_target;
        let command_tx = self.command_tx.clone();

        // Spawn a task that checks rank and maybe repairs after delay
        let handle = tokio::spawn(async move {
            // Check conditions under lock
            let (rank, local_count, local_node) = {
                let map = piece_map.lock().await;
                let rank = map.compute_rank(&cid, segment, true);
                let local_count = map.local_pieces(&cid, segment);
                let local_node = map.local_node().to_vec();
                (rank, local_count, local_node)
            };

            // Need: rank < tier_target AND we hold ≥2 pieces
            // For rank comparison: rank is absolute, tier_target is a ratio.
            // We need to know k to compare. Since we don't have the manifest here,
            // we compare rank as a float ratio would require k. Instead, if rank < 2
            // (absolute minimum for any reconstruction), that's a problem.
            // Actually, the design says tier_target is a ratio (rank/k). But without k,
            // we can't compute health ratio here. Let's use absolute rank for now —
            // tier_target of 1.5 means we want rank >= 2 effectively.
            // TODO: Pass k from manifest for proper ratio comparison
            if local_count < MIN_PIECES_PER_SEGMENT {
                debug!("Cannot repair {}/seg{}: only {} local pieces", cid, segment, local_count);
                return;
            }

            // Use absolute rank comparison: rank < ceil(tier_target) as threshold
            // A rank of 1 with tier_target 1.5 means we're below target
            let rank_f = rank as f64;
            if rank_f >= tier_target {
                debug!("No repair needed for {}/seg{}: rank={} >= target={}", cid, segment, rank, tier_target);
                return;
            }

            // Random delay 0.5s–10s
            let delay_secs = 0.5 + rand::random::<f64>() * 9.5;
            debug!("Scheduling repair for {}/seg{} after {:.1}s (rank={}, target={})",
                cid, segment, delay_secs, rank, tier_target);
            tokio::time::sleep(Duration::from_secs_f64(delay_secs)).await;

            // Re-check rank after delay — may have recovered
            let (rank_now, local_count_now) = {
                let map = piece_map.lock().await;
                (map.compute_rank(&cid, segment, true), map.local_pieces(&cid, segment))
            };

            if rank_now as f64 >= tier_target {
                debug!("Repair cancelled for {}/seg{}: rank recovered to {}", cid, segment, rank_now);
                return;
            }

            if local_count_now < MIN_PIECES_PER_SEGMENT {
                debug!("Cannot repair {}/seg{}: only {} local pieces after delay", cid, segment, local_count_now);
                return;
            }

            // Execute repair: recombine local pieces via RLNC
            let store_guard = store.lock().await;
            let result = crate::health::heal_segment(&store_guard, &cid, segment, 1);

            if result.pieces_generated == 0 {
                warn!("Repair failed for {}/seg{}: {:?}", cid, segment, result.errors);
                return;
            }

            // Find the newly generated piece and emit PieceStored
            if let Ok(pieces_after) = store_guard.list_pieces(&cid, segment) {
                if let Some(&new_pid) = pieces_after.last() {
                    if let Ok((_data, coefficients)) = store_guard.get_piece(&cid, segment, &new_pid) {
                        let mut map = piece_map.lock().await;
                        let seq = map.next_seq();
                        let stored = datacraft_core::PieceStored {
                            node: local_node,
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
                            let _ = command_tx.send(DataCraftCommand::BroadcastPieceEvent { event_data: data });
                        }
                        info!("Repair complete for {}/seg{}: generated 1 new piece", cid, segment);
                    }
                }
            }
        });

        self.pending_repairs.insert(key, handle);
    }

    /// Called when a PieceStored event is processed in the PieceMap.
    /// Cancels pending repairs if rank recovered, schedules degradation if over-replicated.
    pub fn on_piece_stored(&mut self, cid: ContentId, segment: u32) {
        let key = (cid, segment);

        // Cancel pending repair if rank recovered
        if let Some(handle) = self.pending_repairs.remove(&key) {
            // The spawned task will re-check rank anyway, but aborting saves resources
            handle.abort();
            debug!("Aborted pending repair for {}/seg{} due to PieceStored", cid, segment);
        }

        // If we already have a pending degradation, skip
        if self.pending_degradations.contains_key(&key) {
            debug!("Already have pending degradation for {}/seg{}", cid, segment);
            return;
        }

        let piece_map = self.piece_map.clone();
        let store = self.store.clone();
        let demand_tracker = self.demand_tracker.clone();
        let tier_target = self.tier_target;
        let command_tx = self.command_tx.clone();

        // Spawn a task to check if degradation is needed
        let handle = tokio::spawn(async move {
            let (rank, local_count, local_node) = {
                let map = piece_map.lock().await;
                let rank = map.compute_rank(&cid, segment, true);
                let local_count = map.local_pieces(&cid, segment);
                let local_node = map.local_node().to_vec();
                (rank, local_count, local_node)
            };

            // Need: rank > tier_target AND no demand AND local holds >2
            if (rank as f64) <= tier_target {
                return;
            }

            if local_count <= MIN_PIECES_PER_SEGMENT {
                return;
            }

            // Check demand
            {
                let dt = demand_tracker.lock().await;
                if dt.has_recent_signal(&cid) {
                    debug!("Skipping degradation for {}/seg{}: has demand", cid, segment);
                    return;
                }
            }

            // Random delay 0.5s–10s
            let delay_secs = 0.5 + rand::random::<f64>() * 9.5;
            debug!("Scheduling degradation for {}/seg{} after {:.1}s (rank={}, target={})",
                cid, segment, delay_secs, rank, tier_target);
            tokio::time::sleep(Duration::from_secs_f64(delay_secs)).await;

            // Re-check conditions after delay
            let (rank_now, local_count_now) = {
                let map = piece_map.lock().await;
                (map.compute_rank(&cid, segment, true), map.local_pieces(&cid, segment))
            };

            if (rank_now as f64) <= tier_target {
                debug!("Degradation cancelled for {}/seg{}: rank dropped to {}", cid, segment, rank_now);
                return;
            }

            if local_count_now <= MIN_PIECES_PER_SEGMENT {
                debug!("Cannot degrade {}/seg{}: only {} local pieces", cid, segment, local_count_now);
                return;
            }

            // Re-check demand
            {
                let dt = demand_tracker.lock().await;
                if dt.has_recent_signal(&cid) {
                    debug!("Degradation cancelled for {}/seg{}: demand appeared", cid, segment);
                    return;
                }
            }

            // Execute degradation: drop 1 piece (highest piece_id, deterministic)
            let store_guard = store.lock().await;
            let mut pieces = match store_guard.list_pieces(&cid, segment) {
                Ok(p) if p.len() > MIN_PIECES_PER_SEGMENT => p,
                _ => return,
            };
            pieces.sort();
            let piece_to_drop = *pieces.last().unwrap();

            if let Err(e) = store_guard.delete_piece(&cid, segment, &piece_to_drop) {
                warn!("Degradation failed for {}/seg{}: {}", cid, segment, e);
                return;
            }

            info!("Degradation: dropped piece {} for {}/seg{}",
                hex::encode(&piece_to_drop[..8]), cid, segment);

            // Emit PieceDropped event
            {
                let mut map = piece_map.lock().await;
                let seq = map.next_seq();
                let dropped = datacraft_core::PieceDropped {
                    node: local_node,
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
                    let _ = command_tx.send(DataCraftCommand::BroadcastPieceEvent { event_data: data });
                }
            }
        });

        self.pending_degradations.insert(key, handle);
    }

    /// Number of pending repairs (for diagnostics).
    pub fn pending_repair_count(&self) -> usize {
        self.pending_repairs.len()
    }

    /// Number of pending degradations (for diagnostics).
    pub fn pending_degradation_count(&self) -> usize {
        self.pending_degradations.len()
    }

    /// Clean up completed task handles.
    pub fn cleanup_finished(&mut self) {
        self.pending_repairs.retain(|_, handle| !handle.is_finished());
        self.pending_degradations.retain(|_, handle| !handle.is_finished());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datacraft_core::{ContentId, PieceEvent, PieceStored, PieceDropped};

    fn make_tx() -> mpsc::UnboundedSender<DataCraftCommand> {
        let (tx, _rx) = mpsc::unbounded_channel();
        tx
    }

    fn setup_test() -> (Arc<Mutex<PieceMap>>, Arc<Mutex<FsStore>>, Arc<Mutex<DemandSignalTracker>>, PeerId, std::path::PathBuf) {
        let local = PeerId::random();
        let dir = std::env::temp_dir().join(format!("health-reactor-test-{}-{}", std::process::id(), rand::random::<u32>()));
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
    fn test_health_reactor_creation() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let reactor = HealthReactor::new(pm, store, dt, local, tx);
        assert_eq!(reactor.pending_repair_count(), 0);
        assert_eq!(reactor.pending_degradation_count(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_set_tier_target() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut reactor = HealthReactor::new(pm, store, dt, local, tx);
        reactor.set_tier_target(2.0);
        assert_eq!(reactor.tier_target, 2.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_on_piece_dropped_schedules_repair() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut reactor = HealthReactor::new(pm.clone(), store.clone(), dt, local, tx);

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        // Store 2 local pieces so repair is possible
        {
            let s = store.lock().await;
            let coeff1 = vec![1, 0, 0];
            let pid1 = datacraft_store::piece_id_from_coefficients(&coeff1);
            s.store_piece(&cid, 0, &pid1, b"data1", &coeff1).unwrap();

            let coeff2 = vec![0, 1, 0];
            let pid2 = datacraft_store::piece_id_from_coefficients(&coeff2);
            s.store_piece(&cid, 0, &pid2, b"data2", &coeff2).unwrap();

            // Record in PieceMap
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);
            let seq = map.next_seq();
            map.apply_event(&PieceEvent::Stored(PieceStored {
                node: local_bytes.clone(), cid, segment: 0, piece_id: pid1,
                coefficients: coeff1, seq, timestamp: 1000, signature: vec![],
            }));
            let seq = map.next_seq();
            map.apply_event(&PieceEvent::Stored(PieceStored {
                node: local_bytes.clone(), cid, segment: 0, piece_id: pid2,
                coefficients: vec![0, 1, 0], seq, timestamp: 1000, signature: vec![],
            }));
        }

        // Trigger piece dropped — rank is 2 which is > 1.5 default target
        // Set target higher so repair triggers
        reactor.set_tier_target(3.0);
        reactor.on_piece_dropped(cid, 0);

        // Should have a pending repair
        assert_eq!(reactor.pending_repair_count(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_on_piece_stored_cancels_repair() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut reactor = HealthReactor::new(pm.clone(), store.clone(), dt, local, tx);

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        // Setup: local pieces in store and PieceMap
        {
            let s = store.lock().await;
            let coeff1 = vec![1, 0, 0];
            let pid1 = datacraft_store::piece_id_from_coefficients(&coeff1);
            s.store_piece(&cid, 0, &pid1, b"data1", &coeff1).unwrap();
            let coeff2 = vec![0, 1, 0];
            let pid2 = datacraft_store::piece_id_from_coefficients(&coeff2);
            s.store_piece(&cid, 0, &pid2, b"data2", &coeff2).unwrap();

            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);
            let seq = map.next_seq();
            map.apply_event(&PieceEvent::Stored(PieceStored {
                node: local_bytes.clone(), cid, segment: 0, piece_id: pid1,
                coefficients: coeff1, seq, timestamp: 1000, signature: vec![],
            }));
            let seq = map.next_seq();
            map.apply_event(&PieceEvent::Stored(PieceStored {
                node: local_bytes.clone(), cid, segment: 0, piece_id: pid2,
                coefficients: vec![0, 1, 0], seq, timestamp: 1000, signature: vec![],
            }));
        }

        reactor.set_tier_target(3.0);
        reactor.on_piece_dropped(cid, 0);
        assert_eq!(reactor.pending_repair_count(), 1);

        // on_piece_stored should cancel the repair
        reactor.on_piece_stored(cid, 0);
        assert_eq!(reactor.pending_repair_count(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_on_piece_stored_schedules_degradation() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut reactor = HealthReactor::new(pm.clone(), store.clone(), dt, local, tx);

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        // Store 3 local pieces
        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);

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

        // rank=3, tier_target=1.5 → over-replicated, should schedule degradation
        reactor.on_piece_stored(cid, 0);
        assert_eq!(reactor.pending_degradation_count(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_demand_blocks_degradation() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut reactor = HealthReactor::new(pm.clone(), store.clone(), dt.clone(), local, tx);

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        // Store 3 local pieces
        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);

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

        // With demand, degradation should NOT be scheduled
        reactor.on_piece_stored(cid, 0);
        // The task spawns but exits immediately due to demand check
        // Give it a moment
        tokio::time::sleep(Duration::from_millis(50)).await;
        reactor.cleanup_finished();
        assert_eq!(reactor.pending_degradation_count(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_piece_dropped_cancels_degradation() {
        let (pm, store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut reactor = HealthReactor::new(pm.clone(), store.clone(), dt, local, tx);

        let cid = ContentId([1u8; 32]);
        let local_bytes = local.to_bytes().to_vec();

        // Store 3 local pieces
        {
            let s = store.lock().await;
            let mut map = pm.lock().await;
            map.set_node_online(&local_bytes, true);

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

        // Schedule degradation
        reactor.on_piece_stored(cid, 0);
        assert_eq!(reactor.pending_degradation_count(), 1);

        // PieceDropped should cancel it
        reactor.on_piece_dropped(cid, 0);
        assert_eq!(reactor.pending_degradation_count(), 0);

        std::fs::remove_dir_all(&dir).ok();
    }
}
