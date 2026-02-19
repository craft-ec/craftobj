//! Scaling Coordinator
//!
//! Demand-driven piece distribution (push-based).
//! When content is hot (high local fetch rate), nodes can create new pieces via
//! RLNC recombination and push them to non-provider nodes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use craftobj_core::{ContentId, DemandSignal};
use craftobj_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::CraftObjCommand;
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
pub struct DemandTracker {
    /// Fetch timestamps per content ID.
    fetches: HashMap<ContentId, Vec<Instant>>,
    /// Last time we broadcast a demand signal per CID.
    last_broadcast: HashMap<ContentId, Instant>,
    /// Minimum fetches in the demand window to consider content "hot".
    threshold: u32,
}

impl Default for DemandTracker {
    fn default() -> Self {
        Self {
            fetches: HashMap::new(),
            last_broadcast: HashMap::new(),
            threshold: DEMAND_THRESHOLD,
        }
    }
}

impl DemandTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new DemandTracker with a custom threshold.
    pub fn with_threshold(threshold: u32) -> Self {
        Self {
            threshold,
            ..Self::default()
        }
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
            if count >= self.threshold {
                hot.push((*cid, count));
            }
            true
        });

        hot
    }

    /// Check if we should broadcast a demand signal for this CID.
    /// Rate-limited and only broadcasts when non-provider peers exist —
    /// if every peer is already a provider, there's nobody to scale to.
    pub fn should_broadcast_demand(&mut self, content_id: &ContentId, non_providers_exist: bool) -> bool {
        if !non_providers_exist {
            debug!("All peers are providers for {}, skipping scaling broadcast", content_id);
            return false;
        }
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

/// Tracks received DemandSignals.
/// Used by health_scan to check if content has active demand before degrading.
#[derive(Default)]
pub struct DemandSignalTracker {
    /// Last time a DemandSignal was received per CID.
    last_signal: HashMap<ContentId, Instant>,
}

impl DemandSignalTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a DemandSignal was received for this CID.
    pub fn record_signal(&mut self, content_id: ContentId) {
        self.last_signal.insert(content_id, Instant::now());
    }

    /// Check if a DemandSignal was seen within the demand window (5 min).
    pub fn has_recent_signal(&self, content_id: &ContentId) -> bool {
        if let Some(last) = self.last_signal.get(content_id) {
            last.elapsed() < Duration::from_secs(DEMAND_WINDOW_SECS)
        } else {
            false
        }
    }

    /// Clean up stale entries.
    pub fn cleanup(&mut self) {
        let cutoff = Instant::now() - Duration::from_secs(DEMAND_WINDOW_SECS * 2);
        self.last_signal.retain(|_, t| *t > cutoff);
    }
}

/// Coordinates scaling: providers create pieces and push them to non-provider nodes.
/// Push-based — same pattern as repair.
pub struct ScalingCoordinator {
    local_peer_id: PeerId,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    peer_scorer: Option<Arc<Mutex<PeerScorer>>>,
    /// Track which CIDs we've recently attempted scaling for (avoid duplicates).
    recent_scaling: HashMap<ContentId, Instant>,
    /// Storage Merkle tree for incremental updates on local store.
    merkle_tree: Option<Arc<Mutex<craftobj_store::merkle::StorageMerkleTree>>>,
}

impl ScalingCoordinator {
    pub fn new(
        local_peer_id: PeerId,
        command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    ) -> Self {
        Self {
            local_peer_id,
            command_tx,
            peer_scorer: None,
            recent_scaling: HashMap::new(),
            merkle_tree: None,
        }
    }

    pub fn set_merkle_tree(&mut self, tree: Arc<Mutex<craftobj_store::merkle::StorageMerkleTree>>) {
        self.merkle_tree = Some(tree);
    }

    pub fn set_peer_scorer(&mut self, scorer: Arc<Mutex<PeerScorer>>) {
        self.peer_scorer = Some(scorer);
    }

    /// Handle an incoming scaling notice (demand signal).
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

        // We must hold ≥2 pieces for ANY segment to create a recombination.
        let manifest = match store.get_record(&signal.content_id) {
            Ok(m) => m,
            Err(_) => {
                debug!("No manifest for {}, skipping scaling", signal.content_id);
                return None;
            }
        };

        let mut total_local = 0usize;
        for seg in 0..manifest.segment_count() as u32 {
            total_local += store.list_pieces(&signal.content_id, seg).unwrap_or_default().len();
        }

        if total_local < 2 {
            debug!(
                "Not a provider for {} (total {} pieces < 2), skipping scaling",
                signal.content_id, total_local
            );
            return None;
        }

        info!(
            "Scheduling scaling push for {}: demand_level={}, have {} local pieces",
            signal.content_id, signal.demand_level, total_local
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
        // Find a segment with ≥2 pieces for recombination
        let manifest = match store.get_record(&content_id) {
            Ok(m) => m,
            Err(_) => {
                debug!("No manifest for scaling {}", content_id);
                return;
            }
        };

        let mut best_seg: Option<(u32, Vec<[u8; 32]>)> = None;
        for seg in 0..manifest.segment_count() as u32 {
            if let Ok(ids) = store.list_pieces(&content_id, seg) {
                if ids.len() >= 2 && (best_seg.is_none() || ids.len() > best_seg.as_ref().unwrap().1.len()) {
                    best_seg = Some((seg, ids));
                }
            }
        }

        let (segment_index, piece_ids) = match best_seg {
            Some(s) => s,
            None => {
                debug!("Not enough pieces for scaling recombination of {}", content_id);
                return;
            }
        };

        let mut existing_pieces = Vec::new();
        for pid in &piece_ids {
            if let Ok((data, coeff)) = store.get_piece(&content_id, segment_index, pid) {
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

        let new_pid = craftobj_store::piece_id_from_coefficients(&new_piece.coefficients);

        info!("Scaling: created new piece for {}/seg{}, selecting push target", content_id, segment_index);

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
                let _ = store.store_piece(&content_id, segment_index, &new_pid, &new_piece.data, &new_piece.coefficients);
                if let Some(ref mt) = self.merkle_tree {
                    if let Ok(mut tree) = mt.try_lock() {
                        tree.insert(&content_id, segment_index, &new_pid);
                    }
                }
                debug!("No push target available for scaling {}, stored locally", content_id);
                return;
            }
        };

        // Push piece to target node via DistributePieces (unified send_pieces)
        let payload = craftobj_transfer::PiecePayload {
            segment_index,
            piece_id: new_pid,
            coefficients: new_piece.coefficients,
            data: new_piece.data,
        };
        let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
        if self.command_tx.send(CraftObjCommand::DistributePieces {
            peer_id: target_peer,
            content_id,
            pieces: vec![payload],
            reply_tx,
        }).is_err() {
            warn!("Failed to send DistributePieces command for scaling {}", content_id);
        } else {
            info!("Scaling: pushing piece to {} for {}/seg{}", target_peer, content_id, segment_index);
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

    fn make_tx() -> mpsc::UnboundedSender<CraftObjCommand> {
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

        // First broadcast allowed (non-providers exist)
        assert!(tracker.should_broadcast_demand(&cid, true));

        // Second broadcast within cooldown blocked
        assert!(!tracker.should_broadcast_demand(&cid, true));
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

        let store = craftobj_store::FsStore::new(tempfile::tempdir().unwrap().path()).unwrap();
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

        let store = craftobj_store::FsStore::new(tempfile::tempdir().unwrap().path()).unwrap();
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
        let store = craftobj_store::FsStore::new(tempfile::tempdir().unwrap().path()).unwrap();
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

        // Create a store with manifest + ≥2 pieces so we count as a provider
        let tmp = tempfile::tempdir().unwrap();
        let store = craftobj_store::FsStore::new(tmp.path()).unwrap();
        let manifest = craftobj_core::ContentManifest {
            content_id: cid,
            total_size: 200,
            creator: String::new(),
            signature: vec![],
            verification: craftec_erasure::ContentVerificationRecord {
                file_size: 200,
                segment_hashes: vec![],
            },
        };
        store.store_record(&manifest).unwrap();
        let piece_data = vec![0u8; 100];
        let coeff1 = vec![1u8, 0, 0];
        let coeff2 = vec![0u8, 1, 0];
        let pid1 = craftobj_store::piece_id_from_coefficients(&coeff1);
        let pid2 = craftobj_store::piece_id_from_coefficients(&coeff2);
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
        tracker.should_broadcast_demand(&cid, true);
        assert!(!tracker.last_broadcast.is_empty());
        // cleanup won't remove recent entries
        tracker.cleanup();
        assert!(!tracker.last_broadcast.is_empty());
    }

    #[test]
    fn test_no_scaling_broadcast_when_all_providers() {
        let mut tracker = DemandTracker::new();
        let cid = ContentId([4u8; 32]);
        // No non-providers → should not broadcast
        assert!(!tracker.should_broadcast_demand(&cid, false));
        // Confirm no broadcast was recorded
        assert!(!tracker.last_broadcast.contains_key(&cid));
    }
}
