//! Repair Coordinator
//!
//! Handles network-wide repair coordination via gossipsub.
//! When a PDP challenger detects under-replication, it publishes a RepairSignal.
//! Nodes holding pieces for that CID/segment schedule delayed repair.
//! After generating a new piece, nodes publish a RepairAnnouncement.
//! Other nodes decrement their pending count and skip repair if enough announcements seen.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use datacraft_core::{ContentId, ContentManifest, RepairAnnouncement, RepairMessage, RepairSignal};
use datacraft_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;
use crate::health;
use crate::peer_scorer::PeerScorer;

/// Base delay for repair scheduling (highest-scored node gets ~0, lowest gets this).
const BASE_REPAIR_DELAY: Duration = Duration::from_secs(10);

/// Maximum age for a repair signal to be acted on (avoid stale repairs).
const MAX_SIGNAL_AGE: Duration = Duration::from_secs(300);

/// Tracks a pending repair for a (content_id, segment_index) pair.
#[derive(Debug, Clone)]
struct PendingRepair {
    pieces_needed: usize,
    k: usize,
    /// piece_ids we've seen announced (deduplication).
    seen_announcements: HashSet<[u8; 32]>,
    /// When the signal was received.
    received_at: std::time::Instant,
}

/// Coordinates repair across the network via gossipsub.
pub struct RepairCoordinator {
    local_peer_id: PeerId,
    /// Pending repairs keyed by (content_id, segment_index).
    pending: HashMap<(ContentId, u32), PendingRepair>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    peer_scorer: Option<Arc<Mutex<PeerScorer>>>,
}

impl RepairCoordinator {
    pub fn new(
        local_peer_id: PeerId,
        command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    ) -> Self {
        Self {
            local_peer_id,
            pending: HashMap::new(),
            command_tx,
            peer_scorer: None,
        }
    }

    pub fn set_peer_scorer(&mut self, scorer: Arc<Mutex<PeerScorer>>) {
        self.peer_scorer = Some(scorer);
    }

    /// Handle an incoming repair signal from gossipsub.
    /// Returns the delay before we should attempt repair (None if we shouldn't repair).
    pub fn handle_repair_signal(
        &mut self,
        signal: &RepairSignal,
        store: &FsStore,
    ) -> Option<Duration> {
        // Check signal age
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now_ts.saturating_sub(signal.timestamp) > MAX_SIGNAL_AGE.as_secs() {
            debug!("Ignoring stale repair signal for {}/seg{}", signal.content_id, signal.segment_index);
            return None;
        }

        // Check if we hold pieces for this CID/segment
        let pieces = match store.list_pieces(&signal.content_id, signal.segment_index) {
            Ok(p) if p.len() >= 2 => p,
            _ => {
                debug!("No pieces (or <2) for {}/seg{}, skipping repair", signal.content_id, signal.segment_index);
                return None;
            }
        };

        let key = (signal.content_id, signal.segment_index);

        // Don't re-register if we already have a pending repair for this
        if self.pending.contains_key(&key) {
            debug!("Already have pending repair for {}/seg{}", signal.content_id, signal.segment_index);
            return None;
        }

        info!(
            "Scheduling repair for {}/seg{}: {} pieces needed, have {} local pieces",
            signal.content_id, signal.segment_index, signal.pieces_needed, pieces.len()
        );

        self.pending.insert(key, PendingRepair {
            pieces_needed: signal.pieces_needed,
            k: signal.k,
            seen_announcements: HashSet::new(),
            received_at: std::time::Instant::now(),
        });

        Some(self.compute_delay())
    }

    /// Handle an incoming repair announcement from gossipsub.
    /// Returns true if the pending repair for this key can be cancelled (enough repairs done).
    pub fn handle_repair_announcement(&mut self, announcement: &RepairAnnouncement) -> bool {
        let key = (announcement.content_id, announcement.segment_index);

        let pending = match self.pending.get_mut(&key) {
            Some(p) => p,
            None => return false,
        };

        // Deduplicate by piece_id
        if !pending.seen_announcements.insert(announcement.piece_id) {
            debug!("Duplicate repair announcement for piece_id, ignoring");
            return false;
        }

        let announcements_seen = pending.seen_announcements.len();

        if announcements_seen >= pending.pieces_needed {
            info!(
                "Repair complete for {}/seg{}: {} announcements >= {} needed",
                announcement.content_id, announcement.segment_index,
                announcements_seen, pending.pieces_needed
            );
            self.pending.remove(&key);
            return true;
        }

        debug!(
            "Repair progress for {}/seg{}: {}/{} announcements",
            announcement.content_id, announcement.segment_index,
            announcements_seen, pending.pieces_needed
        );
        false
    }

    /// Execute repair for a pending (content_id, segment_index) pair.
    /// Returns the piece_id of the generated piece, or None if repair was cancelled/failed.
    pub fn execute_repair(
        &mut self,
        store: &FsStore,
        manifest: &ContentManifest,
        content_id: ContentId,
        segment_index: u32,
    ) -> Option<[u8; 32]> {
        let key = (content_id, segment_index);

        // Check if still needed
        let pending = match self.pending.get(&key) {
            Some(p) => p.clone(),
            None => {
                debug!("Repair for {}/seg{} no longer pending", content_id, segment_index);
                return None;
            }
        };

        let remaining = pending.pieces_needed.saturating_sub(pending.seen_announcements.len());
        if remaining == 0 {
            self.pending.remove(&key);
            return None;
        }

        // Generate one new piece
        let result = health::heal_content(store, manifest, 1);
        if result.pieces_generated == 0 {
            warn!("Repair failed for {}/seg{}: {:?}", content_id, segment_index, result.errors);
            return None;
        }

        // Find the piece we just generated (latest piece not in our previous list)
        // For simplicity, get the list of pieces after healing and find the newest
        let pieces_after = store.list_pieces(&content_id, segment_index).ok()?;
        let piece_id = *pieces_after.last()?;

        info!("Repair generated piece for {}/seg{}", content_id, segment_index);

        // Publish announcement
        let announcement = RepairAnnouncement {
            content_id,
            segment_index,
            piece_id,
            repairer: self.local_peer_id.to_bytes().to_vec(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let msg = RepairMessage::Announcement(announcement.clone());
        if let Ok(data) = bincode::serialize(&msg) {
            let _ = self.command_tx.send(DataCraftCommand::BroadcastRepairMessage {
                repair_data: data,
            });
        }

        // Also handle our own announcement
        self.handle_repair_announcement(&announcement);

        Some(piece_id)
    }

    /// Compute random delay for repair coordination.
    /// All nodes wait a random interval (0.5s–BASE_REPAIR_DELAY) and listen for
    /// announcements. First to finish wins, others cancel. Simple and proven.
    fn compute_delay(&self) -> Duration {
        let delay_secs = rand::random::<f64>() * BASE_REPAIR_DELAY.as_secs_f64();
        Duration::from_secs_f64(delay_secs.max(0.5))
    }

    /// Clean up stale pending repairs (older than MAX_SIGNAL_AGE).
    pub fn cleanup_stale(&mut self) {
        let cutoff = std::time::Instant::now() - MAX_SIGNAL_AGE;
        self.pending.retain(|key, repair| {
            if repair.received_at < cutoff {
                debug!("Cleaning up stale repair for {:?}/seg{}", key.0, key.1);
                false
            } else {
                true
            }
        });
    }

    /// Number of pending repairs (for diagnostics).
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }
}

/// Create a RepairSignal for broadcasting.
pub fn create_repair_signal(
    content_id: ContentId,
    segment_index: u32,
    pieces_needed: usize,
    current_rank: usize,
    k: usize,
    challenger: &PeerId,
) -> RepairSignal {
    RepairSignal {
        content_id,
        segment_index,
        pieces_needed,
        current_rank,
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
    fn test_repair_signal_serialization_roundtrip() {
        let signal = RepairSignal {
            content_id: ContentId([42u8; 32]),
            segment_index: 3,
            pieces_needed: 5,
            current_rank: 7,
            k: 100,
            challenger: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000000,
        };
        let msg = RepairMessage::Signal(signal.clone());
        let data = bincode::serialize(&msg).unwrap();
        let decoded: RepairMessage = bincode::deserialize(&data).unwrap();
        match decoded {
            RepairMessage::Signal(s) => {
                assert_eq!(s.content_id, signal.content_id);
                assert_eq!(s.segment_index, 3);
                assert_eq!(s.pieces_needed, 5);
                assert_eq!(s.current_rank, 7);
                assert_eq!(s.k, 100);
            }
            _ => panic!("Expected Signal"),
        }
    }

    #[test]
    fn test_repair_announcement_serialization_roundtrip() {
        let announcement = RepairAnnouncement {
            content_id: ContentId([10u8; 32]),
            segment_index: 1,
            piece_id: [99u8; 32],
            repairer: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000001,
        };
        let msg = RepairMessage::Announcement(announcement.clone());
        let data = bincode::serialize(&msg).unwrap();
        let decoded: RepairMessage = bincode::deserialize(&data).unwrap();
        match decoded {
            RepairMessage::Announcement(a) => {
                assert_eq!(a.content_id, announcement.content_id);
                assert_eq!(a.segment_index, 1);
                assert_eq!(a.piece_id, [99u8; 32]);
            }
            _ => panic!("Expected Announcement"),
        }
    }

    #[test]
    fn test_random_delay_bounds() {
        // Random delay should be between 0.5s and BASE_REPAIR_DELAY
        let tx = make_tx();
        let coord = RepairCoordinator::new(PeerId::random(), tx);
        for _ in 0..100 {
            let delay = coord.compute_delay();
            assert!(delay.as_secs_f64() >= 0.5, "Delay should be at least 0.5s");
            assert!(delay <= BASE_REPAIR_DELAY, "Delay should not exceed base");
        }
    }

    #[test]
    fn test_announcement_deduplication() {
        let tx = make_tx();
        let mut coord = RepairCoordinator::new(PeerId::random(), tx);
        let cid = ContentId([1u8; 32]);

        // Insert a pending repair
        coord.pending.insert((cid, 0), PendingRepair {
            pieces_needed: 2,
            k: 100,
            seen_announcements: HashSet::new(),
            received_at: std::time::Instant::now(),
        });

        let announcement = RepairAnnouncement {
            content_id: cid,
            segment_index: 0,
            piece_id: [50u8; 32],
            repairer: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000000,
        };

        // First announcement: counted
        let cancelled = coord.handle_repair_announcement(&announcement);
        assert!(!cancelled);
        assert_eq!(coord.pending[&(cid, 0)].seen_announcements.len(), 1);

        // Duplicate: ignored
        let cancelled = coord.handle_repair_announcement(&announcement);
        assert!(!cancelled);
        assert_eq!(coord.pending[&(cid, 0)].seen_announcements.len(), 1);
    }

    #[test]
    fn test_repair_cancellation_when_enough_announcements() {
        let tx = make_tx();
        let mut coord = RepairCoordinator::new(PeerId::random(), tx);
        let cid = ContentId([2u8; 32]);

        coord.pending.insert((cid, 0), PendingRepair {
            pieces_needed: 2,
            k: 100,
            seen_announcements: HashSet::new(),
            received_at: std::time::Instant::now(),
        });

        // First announcement
        let a1 = RepairAnnouncement {
            content_id: cid,
            segment_index: 0,
            piece_id: [10u8; 32],
            repairer: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000000,
        };
        assert!(!coord.handle_repair_announcement(&a1));

        // Second announcement (different piece) → should cancel
        let a2 = RepairAnnouncement {
            content_id: cid,
            segment_index: 0,
            piece_id: [20u8; 32],
            repairer: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000001,
        };
        assert!(coord.handle_repair_announcement(&a2));

        // Pending should be removed
        assert!(!coord.pending.contains_key(&(cid, 0)));
    }
}
