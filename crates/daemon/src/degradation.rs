//! Degradation Coordinator
//!
//! Handles network-wide degradation coordination via gossipsub.
//! When a PDP challenger detects over-replication, it publishes a DegradationSignal.
//! Nodes holding excess pieces for that CID/segment schedule delayed piece drops.
//! After dropping a piece, nodes publish a DegradationAnnouncement.
//! Other nodes see the announcement and skip their own drop.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datacraft_core::{ContentId, DegradationAnnouncement, DegradationMessage, DegradationSignal};
use datacraft_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;

/// Base delay for degradation scheduling (random interval 0.5s–this value).
const BASE_DEGRADATION_DELAY: Duration = Duration::from_secs(10);

/// Maximum age for a degradation signal to be acted on.
const MAX_SIGNAL_AGE: Duration = Duration::from_secs(300);

/// Minimum number of pieces a node must keep per segment to remain a valid RLNC provider.
const MIN_PIECES_PER_SEGMENT: usize = 2;

/// Tracks a pending degradation for a (content_id, segment_index) pair.
#[derive(Debug, Clone)]
struct PendingDegradation {
    excess_pieces: usize,
    /// piece_ids we've seen announced as dropped (deduplication).
    seen_announcements: HashSet<[u8; 32]>,
    /// When the signal was received.
    received_at: std::time::Instant,
}

/// Coordinates degradation (piece dropping) across the network via gossipsub.
pub struct DegradationCoordinator {
    local_peer_id: PeerId,
    /// Pending degradations keyed by (content_id, segment_index).
    pending: HashMap<(ContentId, u32), PendingDegradation>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
}

impl DegradationCoordinator {
    pub fn new(
        local_peer_id: PeerId,
        command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    ) -> Self {
        Self {
            local_peer_id,
            pending: HashMap::new(),
            command_tx,
        }
    }

    /// Handle an incoming degradation signal from gossipsub.
    /// Returns the delay before we should attempt to drop a piece (None if we shouldn't).
    pub fn handle_degradation_signal(
        &mut self,
        signal: &DegradationSignal,
        store: &FsStore,
    ) -> Option<Duration> {
        // Check signal age
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now_ts.saturating_sub(signal.timestamp) > MAX_SIGNAL_AGE.as_secs() {
            debug!("Ignoring stale degradation signal for {}/seg{}", signal.content_id, signal.segment_index);
            return None;
        }

        // Check if we hold pieces for this CID/segment
        let pieces = match store.list_pieces(&signal.content_id, signal.segment_index) {
            Ok(p) => p,
            Err(_) => return None,
        };

        // Must keep at least MIN_PIECES_PER_SEGMENT to remain a valid provider.
        // If we have exactly MIN_PIECES_PER_SEGMENT or fewer, skip.
        if pieces.len() <= MIN_PIECES_PER_SEGMENT {
            debug!(
                "Only {} pieces for {}/seg{}, need ≥{} to remain provider, skipping degradation",
                pieces.len(), signal.content_id, signal.segment_index, MIN_PIECES_PER_SEGMENT + 1
            );
            return None;
        }

        let key = (signal.content_id, signal.segment_index);

        // Don't re-register if we already have a pending degradation
        if self.pending.contains_key(&key) {
            debug!("Already have pending degradation for {}/seg{}", signal.content_id, signal.segment_index);
            return None;
        }

        info!(
            "Scheduling degradation for {}/seg{}: {} excess, have {} local pieces",
            signal.content_id, signal.segment_index, signal.excess_pieces, pieces.len()
        );

        self.pending.insert(key, PendingDegradation {
            excess_pieces: signal.excess_pieces,
            seen_announcements: HashSet::new(),
            received_at: std::time::Instant::now(),
        });

        Some(self.compute_delay())
    }

    /// Handle an incoming degradation announcement from gossipsub.
    /// Returns true if the pending degradation for this key can be cancelled.
    pub fn handle_degradation_announcement(&mut self, announcement: &DegradationAnnouncement) -> bool {
        let key = (announcement.content_id, announcement.segment_index);

        let pending = match self.pending.get_mut(&key) {
            Some(p) => p,
            None => return false,
        };

        // Deduplicate by piece_id
        if !pending.seen_announcements.insert(announcement.piece_id) {
            debug!("Duplicate degradation announcement for piece_id, ignoring");
            return false;
        }

        let announcements_seen = pending.seen_announcements.len();

        if announcements_seen >= pending.excess_pieces {
            info!(
                "Degradation complete for {}/seg{}: {} announcements >= {} excess",
                announcement.content_id, announcement.segment_index,
                announcements_seen, pending.excess_pieces
            );
            self.pending.remove(&key);
            return true;
        }

        debug!(
            "Degradation progress for {}/seg{}: {}/{} announcements",
            announcement.content_id, announcement.segment_index,
            announcements_seen, pending.excess_pieces
        );
        false
    }

    /// Execute degradation: drop 1 piece for a pending (content_id, segment_index).
    /// Returns the piece_id of the dropped piece, or None if cancelled/failed.
    pub fn execute_degradation(
        &mut self,
        store: &FsStore,
        content_id: ContentId,
        segment_index: u32,
        merkle_tree: Option<&mut datacraft_store::merkle::StorageMerkleTree>,
    ) -> Option<[u8; 32]> {
        let key = (content_id, segment_index);

        // Check if still needed
        let pending = match self.pending.get(&key) {
            Some(p) => p.clone(),
            None => {
                debug!("Degradation for {}/seg{} no longer pending", content_id, segment_index);
                return None;
            }
        };

        let remaining = pending.excess_pieces.saturating_sub(pending.seen_announcements.len());
        if remaining == 0 {
            self.pending.remove(&key);
            return None;
        }

        // Get current pieces
        let mut pieces = match store.list_pieces(&content_id, segment_index) {
            Ok(p) => p,
            Err(_) => return None,
        };

        // Safety: must keep at least MIN_PIECES_PER_SEGMENT
        if pieces.len() <= MIN_PIECES_PER_SEGMENT {
            debug!("Cannot drop: only {} pieces for {}/seg{}", pieces.len(), content_id, segment_index);
            self.pending.remove(&key);
            return None;
        }

        // Drop the piece with the highest piece_id (deterministic selection)
        pieces.sort();
        let piece_to_drop = *pieces.last().unwrap();

        if let Err(e) = store.delete_piece(&content_id, segment_index, &piece_to_drop) {
            warn!("Failed to delete piece for degradation {}/seg{}: {}", content_id, segment_index, e);
            return None;
        }

        info!("Degradation: dropped piece {} for {}/seg{}", hex::encode(&piece_to_drop[..8]), content_id, segment_index);

        // Update merkle tree
        if let Some(tree) = merkle_tree {
            tree.remove(&content_id, segment_index, &piece_to_drop);
        }

        // Publish announcement
        let announcement = DegradationAnnouncement {
            content_id,
            segment_index,
            piece_id: piece_to_drop,
            dropper: self.local_peer_id.to_bytes().to_vec(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let msg = DegradationMessage::Announcement(announcement.clone());
        if let Ok(data) = bincode::serialize(&msg) {
            let _ = self.command_tx.send(DataCraftCommand::BroadcastDegradationMessage {
                degradation_data: data,
            });
        }

        // Handle our own announcement
        self.handle_degradation_announcement(&announcement);

        Some(piece_to_drop)
    }

    /// Compute random delay for degradation coordination.
    fn compute_delay(&self) -> Duration {
        let delay_secs = rand::random::<f64>() * BASE_DEGRADATION_DELAY.as_secs_f64();
        Duration::from_secs_f64(delay_secs.max(0.5))
    }

    /// Clean up stale pending degradations (older than MAX_SIGNAL_AGE).
    pub fn cleanup_stale(&mut self) {
        let cutoff = std::time::Instant::now() - MAX_SIGNAL_AGE;
        self.pending.retain(|key, deg| {
            if deg.received_at < cutoff {
                debug!("Cleaning up stale degradation for {:?}/seg{}", key.0, key.1);
                false
            } else {
                true
            }
        });
    }

    /// Number of pending degradations (for diagnostics).
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

/// Create a DegradationSignal for broadcasting.
pub fn create_degradation_signal(
    content_id: ContentId,
    segment_index: u32,
    excess_pieces: usize,
    current_rank: usize,
    target_rank: usize,
    k: usize,
    challenger: &PeerId,
) -> DegradationSignal {
    DegradationSignal {
        content_id,
        segment_index,
        excess_pieces,
        current_rank,
        target_rank,
        k,
        challenger: challenger.to_bytes().to_vec(),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tx() -> mpsc::UnboundedSender<DataCraftCommand> {
        let (tx, _rx) = mpsc::unbounded_channel();
        tx
    }

    #[test]
    fn test_degradation_signal_serialization_roundtrip() {
        let signal = DegradationSignal {
            content_id: ContentId([42u8; 32]),
            segment_index: 3,
            excess_pieces: 5,
            current_rank: 12,
            target_rank: 7,
            k: 100,
            challenger: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000000,
        };
        let msg = DegradationMessage::Signal(signal.clone());
        let data = bincode::serialize(&msg).unwrap();
        let decoded: DegradationMessage = bincode::deserialize(&data).unwrap();
        match decoded {
            DegradationMessage::Signal(s) => {
                assert_eq!(s.content_id, signal.content_id);
                assert_eq!(s.segment_index, 3);
                assert_eq!(s.excess_pieces, 5);
                assert_eq!(s.current_rank, 12);
                assert_eq!(s.target_rank, 7);
                assert_eq!(s.k, 100);
            }
            _ => panic!("Expected Signal"),
        }
    }

    #[test]
    fn test_degradation_announcement_serialization_roundtrip() {
        let announcement = DegradationAnnouncement {
            content_id: ContentId([10u8; 32]),
            segment_index: 1,
            piece_id: [99u8; 32],
            dropper: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000001,
        };
        let msg = DegradationMessage::Announcement(announcement.clone());
        let data = bincode::serialize(&msg).unwrap();
        let decoded: DegradationMessage = bincode::deserialize(&data).unwrap();
        match decoded {
            DegradationMessage::Announcement(a) => {
                assert_eq!(a.content_id, announcement.content_id);
                assert_eq!(a.segment_index, 1);
                assert_eq!(a.piece_id, [99u8; 32]);
            }
            _ => panic!("Expected Announcement"),
        }
    }

    #[test]
    fn test_random_delay_bounds() {
        let tx = make_tx();
        let coord = DegradationCoordinator::new(PeerId::random(), tx);
        for _ in 0..100 {
            let delay = coord.compute_delay();
            assert!(delay.as_secs_f64() >= 0.5, "Delay should be at least 0.5s");
            assert!(delay <= BASE_DEGRADATION_DELAY, "Delay should not exceed base");
        }
    }

    #[test]
    fn test_announcement_deduplication() {
        let tx = make_tx();
        let mut coord = DegradationCoordinator::new(PeerId::random(), tx);
        let cid = ContentId([1u8; 32]);

        coord.pending.insert((cid, 0), PendingDegradation {
            excess_pieces: 2,
            seen_announcements: HashSet::new(),
            received_at: std::time::Instant::now(),
        });

        let announcement = DegradationAnnouncement {
            content_id: cid,
            segment_index: 0,
            piece_id: [50u8; 32],
            dropper: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000000,
        };

        // First announcement: counted
        let cancelled = coord.handle_degradation_announcement(&announcement);
        assert!(!cancelled);
        assert_eq!(coord.pending[&(cid, 0)].seen_announcements.len(), 1);

        // Duplicate: ignored
        let cancelled = coord.handle_degradation_announcement(&announcement);
        assert!(!cancelled);
        assert_eq!(coord.pending[&(cid, 0)].seen_announcements.len(), 1);
    }

    #[test]
    fn test_cancellation_when_enough_announcements() {
        let tx = make_tx();
        let mut coord = DegradationCoordinator::new(PeerId::random(), tx);
        let cid = ContentId([2u8; 32]);

        coord.pending.insert((cid, 0), PendingDegradation {
            excess_pieces: 2,
            seen_announcements: HashSet::new(),
            received_at: std::time::Instant::now(),
        });

        let a1 = DegradationAnnouncement {
            content_id: cid,
            segment_index: 0,
            piece_id: [10u8; 32],
            dropper: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000000,
        };
        assert!(!coord.handle_degradation_announcement(&a1));

        let a2 = DegradationAnnouncement {
            content_id: cid,
            segment_index: 0,
            piece_id: [20u8; 32],
            dropper: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000001,
        };
        assert!(coord.handle_degradation_announcement(&a2));

        assert!(!coord.pending.contains_key(&(cid, 0)));
    }

    #[test]
    fn test_skip_when_only_two_pieces() {
        let tx = make_tx();
        let mut coord = DegradationCoordinator::new(PeerId::random(), tx);

        let dir = std::env::temp_dir().join(format!("degradation-test-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId([5u8; 32]);

        // Store exactly 2 pieces — should skip (need >2 to drop)
        for i in 0..2u8 {
            let coeff = vec![i, 0, 0];
            let pid = datacraft_store::piece_id_from_coefficients(&coeff);
            store.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
        }

        let signal = DegradationSignal {
            content_id: cid,
            segment_index: 0,
            excess_pieces: 1,
            current_rank: 5,
            target_rank: 3,
            k: 10,
            challenger: PeerId::random().to_bytes().to_vec(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        };

        let result = coord.handle_degradation_signal(&signal, &store);
        assert!(result.is_none(), "Should skip when only 2 pieces");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_accepts_when_enough_pieces() {
        let tx = make_tx();
        let mut coord = DegradationCoordinator::new(PeerId::random(), tx);

        let dir = std::env::temp_dir().join(format!("degradation-accept-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId([6u8; 32]);

        // Store 3 pieces — should accept (can drop 1 and keep 2)
        for i in 0..3u8 {
            let coeff = vec![i, 0, 0];
            let pid = datacraft_store::piece_id_from_coefficients(&coeff);
            store.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
        }

        let signal = DegradationSignal {
            content_id: cid,
            segment_index: 0,
            excess_pieces: 1,
            current_rank: 5,
            target_rank: 3,
            k: 10,
            challenger: PeerId::random().to_bytes().to_vec(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        };

        let result = coord.handle_degradation_signal(&signal, &store);
        assert!(result.is_some(), "Should accept when >2 pieces");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_execute_degradation_drops_highest_piece() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut coord = DegradationCoordinator::new(PeerId::random(), tx);

        let dir = std::env::temp_dir().join(format!("degradation-exec-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId([7u8; 32]);

        // Store 3 pieces
        let mut piece_ids = Vec::new();
        for i in 0..3u8 {
            let coeff = vec![i, 0, 0];
            let pid = datacraft_store::piece_id_from_coefficients(&coeff);
            store.store_piece(&cid, 0, &pid, b"data", &coeff).unwrap();
            piece_ids.push(pid);
        }
        piece_ids.sort();
        let highest = *piece_ids.last().unwrap();

        // Register pending
        coord.pending.insert((cid, 0), PendingDegradation {
            excess_pieces: 1,
            seen_announcements: HashSet::new(),
            received_at: std::time::Instant::now(),
        });

        let dropped = coord.execute_degradation(&store, cid, 0, None);
        assert_eq!(dropped, Some(highest));

        // Verify piece is gone
        assert!(!store.has_piece(&cid, 0, &highest));
        // Verify other pieces remain
        assert_eq!(store.list_pieces(&cid, 0).unwrap().len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }
}
