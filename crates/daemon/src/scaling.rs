//! Scaling Coordinator
//!
//! Demand-driven piece distribution via gossipsub (push-based).
//! When content is hot (high fetch rate), serving nodes publish demand signals.
//! Existing providers see the signal, create a new piece via RLNC recombination,
//! and push it to the highest-rated non-provider node — same pattern as repair.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datacraft_core::{ContentId, DemandSignal};
use datacraft_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;
use crate::peer_scorer::PeerScorer;
use crate::push_target;

/// Minimum fetches in the demand window to consider content "hot".
const DEMAND_THRESHOLD: u32 = 10;

/// Time window for measuring demand (seconds).
const DEMAND_WINDOW_SECS: u64 = 300; // 5 minutes

/// Minimum interval between demand signal broadcasts per CID (seconds).
const SIGNAL_COOLDOWN_SECS: u64 = 300; // 5 minutes

/// Maximum age for a demand signal to be acted on.
const MAX_SIGNAL_AGE: Duration = Duration::from_secs(300);

/// Base delay for scaling coordination (avoid thundering herd).
const BASE_SCALING_DELAY: Duration = Duration::from_secs(10);

/// Tracks fetch request rates per CID to detect demand.
#[derive(Default)]
pub struct DemandTracker {
    /// Fetch timestamps per content ID.
    fetches: HashMap<ContentId, Vec<Instant>>,
    /// Last time we broadcast a demand signal per CID.
    last_broadcast: HashMap<ContentId, Instant>,
}

impl DemandTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that we served a piece for this content.
    pub fn record_fetch(&mut self, content_id: ContentId) {
        self.fetches.entry(content_id).or_default().push(Instant::now());
    }

    /// Return CIDs with demand above threshold in the current window.
    pub fn check_demand(&mut self) -> Vec<(ContentId, u32)> {
        let cutoff = Instant::now() - Duration::from_secs(DEMAND_WINDOW_SECS);
        let mut hot = Vec::new();

        self.fetches.retain(|cid, timestamps| {
            // Prune old entries
            timestamps.retain(|t| *t > cutoff);
            if timestamps.is_empty() {
                return false;
            }
            let count = timestamps.len() as u32;
            if count >= DEMAND_THRESHOLD {
                hot.push((*cid, count));
            }
            true
        });

        hot
    }

    /// Check if we should broadcast a demand signal for this CID (rate-limited).
    pub fn should_broadcast_demand(&mut self, content_id: &ContentId) -> bool {
        let now = Instant::now();
        if let Some(last) = self.last_broadcast.get(content_id) {
            if now.duration_since(*last) < Duration::from_secs(SIGNAL_COOLDOWN_SECS) {
                return false;
            }
        }
        self.last_broadcast.insert(*content_id, now);
        true
    }

    /// Clean up stale broadcast timestamps.
    pub fn cleanup(&mut self) {
        let cutoff = Instant::now() - Duration::from_secs(SIGNAL_COOLDOWN_SECS * 2);
        self.last_broadcast.retain(|_, t| *t > cutoff);
    }
}

/// Coordinates scaling: providers create pieces and push them to non-provider nodes.
/// Push-based — same pattern as repair.
pub struct ScalingCoordinator {
    local_peer_id: PeerId,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    peer_scorer: Option<Arc<Mutex<PeerScorer>>>,
    /// Track which CIDs we've recently attempted scaling for (avoid duplicates).
    recent_scaling: HashMap<ContentId, Instant>,
}

impl ScalingCoordinator {
    pub fn new(
        local_peer_id: PeerId,
        command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    ) -> Self {
        Self {
            local_peer_id,
            command_tx,
            peer_scorer: None,
            recent_scaling: HashMap::new(),
        }
    }

    pub fn set_peer_scorer(&mut self, scorer: Arc<Mutex<PeerScorer>>) {
        self.peer_scorer = Some(scorer);
    }

    /// Handle an incoming scaling notice (demand signal) from gossipsub.
    /// Called by nodes that ARE providers for this CID.
    /// Returns Some(delay) if we should proceed with push, None if we should skip.
    pub fn handle_scaling_notice(
        &mut self,
        signal: &DemandSignal,
        store: &FsStore,
    ) -> Option<Duration> {
        // Check signal age
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if now_ts.saturating_sub(signal.timestamp) > MAX_SIGNAL_AGE.as_secs() {
            debug!("Ignoring stale scaling notice for {}", signal.content_id);
            return None;
        }

        // Don't respond to our own signals
        if signal.reporter == self.local_peer_id.to_bytes() {
            return None;
        }

        // Already attempted scaling for this CID recently?
        if let Some(last) = self.recent_scaling.get(&signal.content_id) {
            if last.elapsed() < Duration::from_secs(SIGNAL_COOLDOWN_SECS) {
                debug!("Already scaled {} recently, skipping", signal.content_id);
                return None;
            }
        }

        // We must hold ≥2 pieces for this content to create a recombination.
        // Check segment 0 as proxy.
        let pieces = match store.list_pieces(&signal.content_id, 0) {
            Ok(p) if p.len() >= 2 => p,
            _ => {
                debug!(
                    "Not a provider for {} (no pieces or <2), skipping scaling",
                    signal.content_id
                );
                return None;
            }
        };

        info!(
            "Scheduling scaling push for {}: demand_level={}, have {} local pieces",
            signal.content_id, signal.demand_level, pieces.len()
        );

        self.recent_scaling.insert(signal.content_id, Instant::now());

        Some(self.compute_delay())
    }

    /// Execute scaling: create a new piece via RLNC recombination and push it
    /// to the highest-rated non-provider node.
    pub fn execute_scaling(
        &self,
        store: &FsStore,
        content_id: ContentId,
        known_providers: &[PeerId],
    ) {
        // Collect existing pieces from segment 0 for recombination
        let piece_ids = match store.list_pieces(&content_id, 0) {
            Ok(ids) if ids.len() >= 2 => ids,
            _ => {
                debug!("Not enough pieces for scaling recombination of {}", content_id);
                return;
            }
        };

        let mut existing_pieces = Vec::new();
        for pid in &piece_ids {
            if let Ok((data, coeff)) = store.get_piece(&content_id, 0, pid) {
                existing_pieces.push(craftec_erasure::CodedPiece {
                    data,
                    coefficients: coeff,
                });
            }
        }

        if existing_pieces.len() < 2 {
            debug!("Could not read enough pieces for scaling {}", content_id);
            return;
        }

        // Create one new piece via RLNC recombination
        let new_piece = match craftec_erasure::create_piece_from_existing(&existing_pieces) {
            Ok(p) => p,
            Err(e) => {
                warn!("Scaling recombination failed for {}: {}", content_id, e);
                return;
            }
        };

        let new_pid = datacraft_store::piece_id_from_coefficients(&new_piece.coefficients);

        info!("Scaling: created new piece for {}, selecting push target", content_id);

        // Select push target: prefer non-providers, fall back to providers.
        let target = push_target::select_push_target(
            &self.local_peer_id,
            known_providers,
            &self.peer_scorer,
        );
        let target_peer = match target {
            Some(p) => p,
            None => {
                // Store locally as fallback
                let _ = store.store_piece(&content_id, 0, &new_pid, &new_piece.data, &new_piece.coefficients);
                debug!("No push target available for scaling {}, stored locally", content_id);
                return;
            }
        };

        // Push piece to target node
        let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
        if self.command_tx.send(DataCraftCommand::PushPiece {
            peer_id: target_peer,
            content_id,
            segment_index: 0,
            piece_id: new_pid,
            coefficients: new_piece.coefficients,
            piece_data: new_piece.data,
            reply_tx,
        }).is_err() {
            warn!("Failed to send PushPiece command for scaling {}", content_id);
        } else {
            info!("Scaling: pushing piece to {} for {}", target_peer, content_id);
        }
    }

    /// Compute random delay for scaling coordination (same as repair).
    fn compute_delay(&self) -> Duration {
        let delay_secs = rand::random::<f64>() * BASE_SCALING_DELAY.as_secs_f64();
        Duration::from_secs_f64(delay_secs.max(0.5))
    }

    /// Clean up stale entries.
    pub fn cleanup(&mut self) {
        let cutoff = Instant::now() - Duration::from_secs(SIGNAL_COOLDOWN_SECS * 2);
        self.recent_scaling.retain(|_, t| *t > cutoff);
    }
}

/// Create a demand signal for broadcasting.
pub fn create_demand_signal(
    content_id: ContentId,
    demand_level: u32,
    current_providers: u32,
    local_peer_id: &PeerId,
) -> DemandSignal {
    DemandSignal {
        content_id,
        demand_level,
        current_providers,
        reporter: local_peer_id.to_bytes().to_vec(),
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
    fn test_demand_tracker_record_and_check() {
        let mut tracker = DemandTracker::new();
        let cid = ContentId([1u8; 32]);

        // Below threshold
        for _ in 0..5 {
            tracker.record_fetch(cid);
        }
        assert!(tracker.check_demand().is_empty());

        // At threshold
        for _ in 0..5 {
            tracker.record_fetch(cid);
        }
        let hot = tracker.check_demand();
        assert_eq!(hot.len(), 1);
        assert_eq!(hot[0].0, cid);
        assert_eq!(hot[0].1, 10);
    }

    #[test]
    fn test_demand_signal_rate_limiting() {
        let mut tracker = DemandTracker::new();
        let cid = ContentId([2u8; 32]);

        // First broadcast allowed
        assert!(tracker.should_broadcast_demand(&cid));

        // Second broadcast within cooldown blocked
        assert!(!tracker.should_broadcast_demand(&cid));
    }

    #[test]
    fn test_demand_signal_serialization_roundtrip() {
        let signal = DemandSignal {
            content_id: ContentId([42u8; 32]),
            demand_level: 50,
            current_providers: 3,
            reporter: PeerId::random().to_bytes().to_vec(),
            timestamp: 1700000000,
        };
        let data = bincode::serialize(&signal).unwrap();
        let decoded: DemandSignal = bincode::deserialize(&data).unwrap();
        assert_eq!(decoded.content_id, signal.content_id);
        assert_eq!(decoded.demand_level, 50);
        assert_eq!(decoded.current_providers, 3);
    }

    #[test]
    fn test_scaling_coordinator_skips_own_signal() {
        let peer_id = PeerId::random();
        let tx = make_tx();
        let mut coord = ScalingCoordinator::new(peer_id, tx);

        let signal = DemandSignal {
            content_id: ContentId([1u8; 32]),
            demand_level: 20,
            current_providers: 2,
            reporter: peer_id.to_bytes().to_vec(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        let store = datacraft_store::FsStore::new(tempfile::tempdir().unwrap().path()).unwrap();
        assert!(coord.handle_scaling_notice(&signal, &store).is_none());
    }

    #[test]
    fn test_scaling_coordinator_skips_stale_signal() {
        let tx = make_tx();
        let mut coord = ScalingCoordinator::new(PeerId::random(), tx);

        let signal = DemandSignal {
            content_id: ContentId([1u8; 32]),
            demand_level: 20,
            current_providers: 2,
            reporter: PeerId::random().to_bytes().to_vec(),
            timestamp: 1000, // very old
        };

        let store = datacraft_store::FsStore::new(tempfile::tempdir().unwrap().path()).unwrap();
        assert!(coord.handle_scaling_notice(&signal, &store).is_none());
    }

    #[test]
    fn test_scaling_non_provider_ignores_notice() {
        let tx = make_tx();
        let mut coord = ScalingCoordinator::new(PeerId::random(), tx);

        let signal = DemandSignal {
            content_id: ContentId([1u8; 32]),
            demand_level: 20,
            current_providers: 2,
            reporter: PeerId::random().to_bytes().to_vec(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Empty store — we're not a provider
        let store = datacraft_store::FsStore::new(tempfile::tempdir().unwrap().path()).unwrap();
        assert!(coord.handle_scaling_notice(&signal, &store).is_none());
    }

    #[test]
    fn test_scaling_provider_accepts_notice() {
        let tx = make_tx();
        let mut coord = ScalingCoordinator::new(PeerId::random(), tx);
        let cid = ContentId([5u8; 32]);

        let signal = DemandSignal {
            content_id: cid,
            demand_level: 20,
            current_providers: 2,
            reporter: PeerId::random().to_bytes().to_vec(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Create a store with ≥2 pieces so we count as a provider
        let tmp = tempfile::tempdir().unwrap();
        let store = datacraft_store::FsStore::new(tmp.path()).unwrap();
        let piece_data = vec![0u8; 100];
        let coeff1 = vec![1u8, 0, 0];
        let coeff2 = vec![0u8, 1, 0];
        let pid1 = datacraft_store::piece_id_from_coefficients(&coeff1);
        let pid2 = datacraft_store::piece_id_from_coefficients(&coeff2);
        store.store_piece(&cid, 0, &pid1, &piece_data, &coeff1).unwrap();
        store.store_piece(&cid, 0, &pid2, &piece_data, &coeff2).unwrap();

        let result = coord.handle_scaling_notice(&signal, &store);
        assert!(result.is_some());
        let delay = result.unwrap();
        assert!(delay.as_secs_f64() >= 0.5);
        assert!(delay <= BASE_SCALING_DELAY);
    }

    #[test]
    fn test_random_delay_bounds() {
        let tx = make_tx();
        let coord = ScalingCoordinator::new(PeerId::random(), tx);
        for _ in 0..100 {
            let delay = coord.compute_delay();
            assert!(delay.as_secs_f64() >= 0.5, "Delay should be at least 0.5s");
            assert!(delay <= BASE_SCALING_DELAY, "Delay should not exceed base");
        }
    }

    #[test]
    fn test_demand_tracker_cleanup() {
        let mut tracker = DemandTracker::new();
        let cid = ContentId([3u8; 32]);
        tracker.should_broadcast_demand(&cid);
        assert!(!tracker.last_broadcast.is_empty());
        // cleanup won't remove recent entries
        tracker.cleanup();
        assert!(!tracker.last_broadcast.is_empty());
    }
}
