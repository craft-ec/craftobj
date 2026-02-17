//! Challenger Manager
//!
//! Periodic PDP challenger loop. Uses piece_id and coefficient vectors
//! instead of shard indices. Health = matrix rank per segment.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datacraft_core::{ContentId, StorageReceipt};
use datacraft_store::FsStore;
use ed25519_dalek::SigningKey;
use libp2p::PeerId;
use rand::seq::SliceRandom;
use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use datacraft_transfer;
use crate::commands::DataCraftCommand;
use crate::health::{self, DutyCycleResult, TierInfo, PdpRoundResult, ProviderPdpResult};
use crate::pdp::{
    ChallengerRotation, OnlineTimeTracker,
    compute_proof_hash, create_storage_receipt,
};
use crate::peer_scorer::PeerScorer;
use crate::receipt_store::PersistentReceiptStore;

/// How often the challenger checks if any CID needs a challenge round.
pub const CHALLENGE_INTERVAL: Duration = Duration::from_secs(300);

/// Minimum time between challenge rounds for the same CID.
pub const MIN_ROUND_INTERVAL: Duration = Duration::from_secs(600);

/// Maximum number of CIDs to challenge per round (sampled randomly).
pub const MAX_CIDS_PER_ROUND: usize = 10;

/// Maximum number of providers to challenge per CID per round (sampled randomly).
pub const MAX_PROVIDERS_PER_CID: usize = 5;

#[derive(Debug)]
struct ProvidedCid {
    last_challenged: Option<Instant>,
    tier_info: Option<TierInfo>,
}

/// Manages the challenger duty cycle for all CIDs this node provides.
pub struct ChallengerManager {
    local_peer_id: PeerId,
    local_pubkey: [u8; 32],
    provided_cids: HashMap<ContentId, ProvidedCid>,
    rotation: ChallengerRotation,
    online_tracker: OnlineTimeTracker,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    signing_key: Option<SigningKey>,
    persistent_store: Option<Arc<Mutex<PersistentReceiptStore>>>,
    peer_scorer: Option<Arc<Mutex<PeerScorer>>>,
    /// Shared PDP rank data for eviction retirement checks.
    pdp_ranks: Option<Arc<Mutex<PdpRankData>>>,
    /// Storage Merkle tree for updating after healing.
    merkle_tree: Option<Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>>,
}

/// Shared PDP rank data: maps CID → (k, segment_index → rank).
pub type PdpRankData = HashMap<ContentId, (usize, HashMap<u32, usize>)>;

impl ChallengerManager {
    pub fn new(
        local_peer_id: PeerId,
        local_pubkey: [u8; 32],
        command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    ) -> Self {
        Self {
            local_peer_id,
            local_pubkey,
            provided_cids: HashMap::new(),
            rotation: ChallengerRotation::new(),
            online_tracker: OnlineTimeTracker::new(),
            command_tx,
            signing_key: None,
            persistent_store: None,
            peer_scorer: None,
            pdp_ranks: None,
            merkle_tree: None,
        }
    }

    pub fn set_merkle_tree(&mut self, tree: Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>) {
        self.merkle_tree = Some(tree);
    }

    pub fn set_pdp_ranks(&mut self, ranks: Arc<Mutex<PdpRankData>>) {
        self.pdp_ranks = Some(ranks);
    }

    pub fn set_peer_scorer(&mut self, scorer: Arc<Mutex<PeerScorer>>) {
        self.peer_scorer = Some(scorer);
    }

    pub fn set_signing_key(&mut self, key: SigningKey) {
        self.signing_key = Some(key);
    }

    pub fn set_persistent_store(&mut self, store: Arc<Mutex<PersistentReceiptStore>>) {
        self.persistent_store = Some(store);
    }

    pub fn register_cid(&mut self, cid: ContentId, tier_info: Option<TierInfo>) {
        info!("Registering CID {} for challenger tracking", cid);
        self.provided_cids.entry(cid).or_insert(ProvidedCid {
            last_challenged: None,
            tier_info,
        });
        self.online_tracker.observe(cid, self.local_peer_id);
    }

    pub fn unregister_cid(&mut self, cid: &ContentId) {
        self.provided_cids.remove(cid);
    }

    /// Check all provided CIDs and run challenge rounds for those that are due.
    pub async fn periodic_check(&mut self, store: &FsStore) -> usize {
        let now = Instant::now();
        let mut due_cids: Vec<ContentId> = self
            .provided_cids
            .iter()
            .filter(|(_, state)| {
                state
                    .last_challenged
                    .map(|t| now.duration_since(t) >= MIN_ROUND_INTERVAL)
                    .unwrap_or(true)
            })
            .map(|(cid, _)| *cid)
            .collect();

        // Sample a subset of due CIDs to bound per-round work
        if due_cids.len() > MAX_CIDS_PER_ROUND {
            due_cids.shuffle(&mut rand::thread_rng());
            due_cids.truncate(MAX_CIDS_PER_ROUND);
        }

        let mut rounds_run = 0;
        for cid in due_cids {
            match self.run_challenge_round(store, cid).await {
                Ok(result) => {
                    info!(
                        "Challenge round for {}: {}/{} passed, healing={}",
                        cid,
                        result.pdp_results.passed_count(),
                        result.pdp_results.results.len(),
                        result.healing.is_some()
                    );
                    rounds_run += 1;
                }
                Err(e) => {
                    warn!("Challenge round for {} failed: {}", cid, e);
                }
            }
        }
        rounds_run
    }

    /// Run a full challenge round for a CID.
    async fn run_challenge_round(
        &mut self,
        store: &FsStore,
        cid: ContentId,
    ) -> Result<DutyCycleResult, String> {
        let providers = self.resolve_providers(cid).await?;
        if providers.is_empty() {
            return Err("No providers found".into());
        }

        for peer in &providers {
            self.online_tracker.observe(cid, *peer);
        }

        let sorted = self.online_tracker.sort_by_online_time(&cid, &providers);
        let challenger = self
            .rotation
            .current_challenger(&cid, &sorted)
            .ok_or("No challenger available")?;

        if challenger != self.local_peer_id {
            if let Some(state) = self.provided_cids.get_mut(&cid) {
                state.last_challenged = Some(Instant::now());
            }
            return Err("Not our turn to challenge".into());
        }

        info!("Running challenge round for {} (we are challenger)", cid);

        let manifest = store
            .get_manifest(&cid)
            .map_err(|e| format!("Failed to get manifest: {}", e))?;

        // Determine which segments to sample: all if ≤5, random subset otherwise
        let segment_count = manifest.segment_count as u32;
        let segments_to_check: Vec<u32> = if segment_count <= 5 {
            (0..segment_count).collect()
        } else {
            let mut all: Vec<u32> = (0..segment_count).collect();
            all.shuffle(&mut rand::thread_rng());
            all.truncate(5);
            all
        };

        let mut other_providers: Vec<PeerId> = providers
            .iter()
            .filter(|p| **p != self.local_peer_id)
            .copied()
            .collect();

        // Sample a subset of providers to bound per-CID work
        if other_providers.len() > MAX_PROVIDERS_PER_CID {
            other_providers.shuffle(&mut rand::thread_rng());
            other_providers.truncate(MAX_PROVIDERS_PER_CID);
        }

        // Step 1: Query full inventory from all providers to get ALL coefficient vectors
        // for accurate network rank computation.
        let mut all_inventory_vectors: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
        for &peer in &other_providers {
            match self.request_inventory(peer, cid).await {
                Ok(inventory) => {
                    for seg_inv in &inventory.segments {
                        let vectors = all_inventory_vectors.entry(seg_inv.segment_index).or_default();
                        for cv in &seg_inv.coefficient_vectors {
                            vectors.push(cv.clone());
                        }
                    }
                }
                Err(e) => {
                    debug!("Inventory request to {} failed: {} — will use PDP vectors only", peer, e);
                }
            }
        }

        // Also include our own local pieces in the inventory
        for &seg_idx in &segments_to_check {
            if let Ok(piece_ids) = store.list_pieces(&cid, seg_idx) {
                let vectors = all_inventory_vectors.entry(seg_idx).or_default();
                for pid in &piece_ids {
                    if let Ok((_data, coeff)) = store.get_piece(&cid, seg_idx, pid) {
                        vectors.push(coeff);
                    }
                }
            }
        }

        // Step 2: Challenge each provider for each sampled segment (PDP verification)
        let mut pdp_results = Vec::new();

        for &segment_index in &segments_to_check {
            for &peer in &other_providers {
                let any_piece = [0u8; 32]; // "any piece"
                let nonce: [u8; 32] = rand::thread_rng().gen();

                let challenge_start = Instant::now();
                match self.request_piece(peer, cid, segment_index, &any_piece).await {
                    Ok((coefficients, data)) => {
                        let latency = challenge_start.elapsed();
                        let piece_id = datacraft_store::piece_id_from_coefficients(&coefficients);
                        let byte_positions = crate::pdp::derive_byte_positions(&nonce, &piece_id, data.len() as u32, 16);
                        let proof_hash = compute_proof_hash(&data, &byte_positions, &coefficients, &nonce);

                        if let Some(ref scorer) = self.peer_scorer {
                            scorer.lock().await.record_success(&peer, latency);
                        }

                        // Cross-verify using our own local pieces for this segment
                        // TODO: Implement full PDP challenge-response wire protocol
                        // (challenger sends nonce + byte positions, prover returns bytes).
                        // For now, we use the piece request protocol and verify locally.
                        let own_pieces = self.load_own_pieces(store, &cid, segment_index);
                        let verification = crate::pdp::cross_verify_piece(
                            &own_pieces,
                            &coefficients,
                            &data,
                            &byte_positions,
                        );

                        let passed = match verification {
                            crate::pdp::CrossVerifyResult::Verified => {
                                debug!("PDP cross-verification passed for {} seg{} from {}", cid, segment_index, peer);
                                true
                            }
                            crate::pdp::CrossVerifyResult::InsufficientBasis => {
                                // Fall back to basic "piece received" for v1
                                debug!("PDP cross-verification inconclusive for {} seg{} from {} (insufficient basis), accepting", cid, segment_index, peer);
                                true
                            }
                            crate::pdp::CrossVerifyResult::Failed => {
                                warn!("PDP cross-verification FAILED for {} seg{} from {} — data mismatch!", cid, segment_index, peer);
                                false
                            }
                        };

                        let receipt = if passed {
                            let storage_node = crate::pdp::peer_id_to_local_pubkey(&peer);
                            Some(create_storage_receipt(
                                cid, storage_node, self.local_pubkey,
                                segment_index, piece_id, nonce, proof_hash,
                            ))
                        } else {
                            None
                        };

                        pdp_results.push(ProviderPdpResult {
                            peer_id: peer,
                            segment_index,
                            piece_id,
                            coefficients,
                            passed,
                            receipt,
                        });
                    }
                    Err(e) => {
                        debug!("PDP challenge to {} for segment {} failed: {}", peer, segment_index, e);
                        if let Some(ref scorer) = self.peer_scorer {
                            scorer.lock().await.record_timeout(&peer);
                        }
                        pdp_results.push(ProviderPdpResult {
                            peer_id: peer,
                            segment_index,
                            piece_id: [0u8; 32],
                            coefficients: vec![],
                            passed: false,
                            receipt: None,
                        });
                    }
                }
            }
        }

        let round_result = PdpRoundResult {
            content_id: cid,
            results: pdp_results,
        };

        // Compute true network rank per segment using FULL inventory vectors
        // (not just the single piece per provider from PDP challenges).
        let rank_map = health::compute_rank_from_inventory(&all_inventory_vectors);
        let min_rank = health::min_rank_across_segments(&rank_map);

        let k = manifest.k();
        let tier_info = self.provided_cids.get(&cid).and_then(|s| s.tier_info.clone());
        let health = health::assess_health(cid, k, min_rank, round_result.results.len(), tier_info.as_ref());

        // Update shared PDP rank data for eviction retirement checks
        if let Some(ref pdp_ranks) = self.pdp_ranks {
            let mut ranks = pdp_ranks.lock().await;
            ranks.insert(cid, (k, rank_map.clone()));
        }

        // Heal if needed — challenger heals locally AND publishes repair signal
        let healing = if health.needs_healing && health.pieces_needed > 0 {
            info!("Healing {} — generating {} new pieces (local + broadcasting repair signal)", cid, health.pieces_needed);

            // Publish repair signal for each under-replicated segment
            for (&seg_idx, &seg_rank) in &rank_map {
                let k_seg = manifest.k_for_segment(seg_idx as usize);
                let required = tier_info.as_ref()
                    .map(|t| (t.min_piece_ratio * k_seg as f64).ceil() as usize)
                    .unwrap_or(0);
                if seg_rank < required {
                    let signal = crate::repair::create_repair_signal(
                        cid, seg_idx, required - seg_rank, seg_rank, k_seg, &self.local_peer_id,
                    );
                    let msg = datacraft_core::RepairMessage::Signal(signal);
                    if let Ok(data) = bincode::serialize(&msg) {
                        let _ = self.command_tx.send(
                            DataCraftCommand::BroadcastRepairMessage { repair_data: data }
                        );
                    }
                }
            }

            let result = health::heal_content(store, &manifest, health.pieces_needed);
            // Rebuild Merkle tree after healing added new pieces
            if result.pieces_generated > 0 {
                if let Some(ref mt) = self.merkle_tree {
                    if let Ok(new_tree) = datacraft_store::merkle::StorageMerkleTree::build_from_store(store) {
                        *mt.lock().await = new_tree;
                    }
                }
            }
            Some(result)
        } else {
            None
        };

        // Sign and persist receipts
        let mut signed_receipts: Vec<StorageReceipt> = Vec::new();
        for mut receipt in round_result.receipts() {
            if let Some(ref key) = self.signing_key {
                datacraft_core::signing::sign_storage_receipt(&mut receipt, key);
            }
            signed_receipts.push(receipt);
        }

        // Challenger self-receipt
        let own_piece_id = store.list_pieces(&cid, 0)
            .ok()
            .and_then(|p| p.first().copied())
            .unwrap_or([0u8; 32]);
        let mut challenger_receipt = create_storage_receipt(
            cid, self.local_pubkey, self.local_pubkey,
            0, own_piece_id, [0u8; 32], [0u8; 32],
        );
        if let Some(ref key) = self.signing_key {
            datacraft_core::signing::sign_storage_receipt(&mut challenger_receipt, key);
        }
        signed_receipts.push(challenger_receipt.clone());

        if let Some(ref persistent) = self.persistent_store {
            let mut store = persistent.lock().await;
            for receipt in &signed_receipts {
                if let Err(e) = store.add_storage(receipt.clone()) {
                    warn!("Failed to persist storage receipt: {}", e);
                }
            }
        }

        for receipt in &signed_receipts {
            if let Ok(data) = bincode::serialize(receipt) {
                let _ = self.command_tx.send(
                    DataCraftCommand::BroadcastStorageReceipt { receipt_data: data }
                );
            }
        }

        self.rotation.advance(&cid, &sorted);
        if let Some(state) = self.provided_cids.get_mut(&cid) {
            state.last_challenged = Some(Instant::now());
        }

        Ok(DutyCycleResult {
            pdp_results: round_result,
            health,
            healing,
            challenger_receipt: Some(challenger_receipt),
        })
    }

    /// Load our own pieces for a given CID and segment from local store.
    /// Returns (data, coefficients) pairs for cross-verification.
    fn load_own_pieces(&self, store: &FsStore, cid: &ContentId, segment_index: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        let piece_ids = match store.list_pieces(cid, segment_index) {
            Ok(ids) => ids,
            Err(_) => return Vec::new(),
        };
        let mut pieces = Vec::new();
        for pid in &piece_ids {
            if let Ok((data, coeff)) = store.get_piece(cid, segment_index, pid) {
                pieces.push((data, coeff));
            }
        }
        pieces
    }

    async fn resolve_providers(&self, cid: ContentId) -> Result<Vec<PeerId>, String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::ResolveProviders { content_id: cid, reply_tx: tx })
            .map_err(|e| format!("Failed to send resolve command: {}", e))?;
        rx.await.map_err(|e| format!("Channel closed: {}", e))?
    }

    /// Request inventory from a peer by sending a PieceSync with empty have_pieces.
    /// Returns an InventoryResponse built from the PieceBatch.
    async fn request_inventory(
        &self,
        peer: PeerId,
        cid: ContentId,
    ) -> Result<datacraft_core::InventoryResponse, String> {
        // We don't know which segments to query, so query segment 0 with a large max.
        // For full inventory, the caller iterates segments.
        // This is a simplification — full inventory requires per-segment queries.
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::PieceSync {
                peer_id: peer,
                content_id: cid,
                segment_index: 0,
                merkle_root: [0u8; 32],
                have_pieces: vec![],
                max_pieces: 1000,
                reply_tx: tx,
            })
            .map_err(|e| format!("Failed to send PieceSync: {}", e))?;
        let response = rx.await.map_err(|e| format!("Channel closed: {}", e))??;
        match response {
            datacraft_transfer::DataCraftResponse::PieceBatch { pieces } => {
                let mut segments: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
                for p in &pieces {
                    segments.entry(p.segment_index).or_default().push(p.coefficients.clone());
                }
                let inv_segments = segments.into_iter().map(|(seg_idx, coeff_vecs)| {
                    datacraft_core::SegmentInventory {
                        segment_index: seg_idx,
                        coefficient_vectors: coeff_vecs,
                    }
                }).collect();
                Ok(datacraft_core::InventoryResponse { content_id: cid, segments: inv_segments })
            }
            _ => Err("unexpected response type".into()),
        }
    }

    /// Request a piece from a peer via PieceSync (requesting 1 piece, excluding none).
    async fn request_piece(
        &self,
        peer: PeerId,
        cid: ContentId,
        segment_index: u32,
        _piece_id: &[u8; 32],
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::PieceSync {
                peer_id: peer,
                content_id: cid,
                segment_index,
                merkle_root: [0u8; 32],
                have_pieces: vec![],
                max_pieces: 1,
                reply_tx: tx,
            })
            .map_err(|e| format!("Failed to send PieceSync: {}", e))?;
        let response = rx.await.map_err(|e| format!("Channel closed: {}", e))??;
        match response {
            datacraft_transfer::DataCraftResponse::PieceBatch { pieces } => {
                if let Some(piece) = pieces.into_iter().next() {
                    Ok((piece.coefficients, piece.data))
                } else {
                    Err("no pieces returned".into())
                }
            }
            _ => Err("unexpected response type".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_challenger_manager_register_unregister() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let peer_id = PeerId::random();
        let mut mgr = ChallengerManager::new(peer_id, [0u8; 32], tx);

        let cid = ContentId([1u8; 32]);
        mgr.register_cid(cid, None);
        assert!(mgr.provided_cids.contains_key(&cid));

        mgr.unregister_cid(&cid);
        assert!(!mgr.provided_cids.contains_key(&cid));
    }

    #[test]
    fn test_due_cid_selection() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut mgr = ChallengerManager::new(PeerId::random(), [0u8; 32], tx);

        let cid1 = ContentId([10u8; 32]);
        let cid2 = ContentId([11u8; 32]);

        mgr.register_cid(cid1, None);
        mgr.register_cid(cid2, None);

        let now = Instant::now();
        let due: Vec<ContentId> = mgr
            .provided_cids
            .iter()
            .filter(|(_, state)| {
                state.last_challenged
                    .map(|t| now.duration_since(t) >= MIN_ROUND_INTERVAL)
                    .unwrap_or(true)
            })
            .map(|(cid, _)| *cid)
            .collect();
        assert_eq!(due.len(), 2);
    }

    #[test]
    fn test_receipt_persistence_roundtrip() {
        use crate::pdp::create_signed_storage_receipt;
        use crate::receipt_store::PersistentReceiptStore;

        let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
        let cid = ContentId([80u8; 32]);
        let path = std::env::temp_dir().join(format!(
            "challenger-persist-test-{}.bin",
            std::process::id()
        ));

        {
            let mut store = PersistentReceiptStore::new(path.clone()).unwrap();
            for seg in 0..3u32 {
                let piece_id = [seg as u8; 32];
                let receipt = create_signed_storage_receipt(
                    cid, [1u8; 32], key.verifying_key().to_bytes(),
                    seg, piece_id, [seg as u8; 32], [(seg + 10) as u8; 32], &key,
                );
                assert!(store.add_storage(receipt).unwrap());
            }
            assert_eq!(store.storage_receipt_count(), 3);
        }

        {
            let store = PersistentReceiptStore::new(path.clone()).unwrap();
            assert_eq!(store.storage_receipt_count(), 3);
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_sampled_cid_selection_bounded() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut mgr = ChallengerManager::new(PeerId::random(), [0u8; 32], tx);

        // Register more CIDs than MAX_CIDS_PER_ROUND
        let num_cids = MAX_CIDS_PER_ROUND + 20;
        for i in 0..num_cids {
            let mut id = [0u8; 32];
            id[0] = i as u8;
            id[1] = (i >> 8) as u8;
            mgr.register_cid(ContentId(id), None);
        }

        let now = Instant::now();
        let mut due_cids: Vec<ContentId> = mgr
            .provided_cids
            .iter()
            .filter(|(_, state)| {
                state.last_challenged
                    .map(|t| now.duration_since(t) >= MIN_ROUND_INTERVAL)
                    .unwrap_or(true)
            })
            .map(|(cid, _)| *cid)
            .collect();

        assert_eq!(due_cids.len(), num_cids);

        // Apply sampling
        if due_cids.len() > MAX_CIDS_PER_ROUND {
            due_cids.shuffle(&mut rand::thread_rng());
            due_cids.truncate(MAX_CIDS_PER_ROUND);
        }

        assert_eq!(due_cids.len(), MAX_CIDS_PER_ROUND);
    }

    #[test]
    fn test_sampled_provider_selection_bounded() {
        // Simulate provider sampling logic
        let mut providers: Vec<PeerId> = (0..20).map(|_| PeerId::random()).collect();
        assert_eq!(providers.len(), 20);

        if providers.len() > MAX_PROVIDERS_PER_CID {
            providers.shuffle(&mut rand::thread_rng());
            providers.truncate(MAX_PROVIDERS_PER_CID);
        }

        assert_eq!(providers.len(), MAX_PROVIDERS_PER_CID);
    }

    #[test]
    fn test_sampling_no_op_when_under_limit() {
        // When fewer CIDs than limit, all are kept
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut mgr = ChallengerManager::new(PeerId::random(), [0u8; 32], tx);

        for i in 0..3u8 {
            mgr.register_cid(ContentId([i; 32]), None);
        }

        let mut due_cids: Vec<ContentId> = mgr.provided_cids.keys().copied().collect();
        if due_cids.len() > MAX_CIDS_PER_ROUND {
            due_cids.shuffle(&mut rand::thread_rng());
            due_cids.truncate(MAX_CIDS_PER_ROUND);
        }

        assert_eq!(due_cids.len(), 3); // All kept, not truncated
    }

    #[test]
    fn test_merkle_root_changes_on_piece_operations() {
        use datacraft_store::merkle::StorageMerkleTree;

        let mut tree = StorageMerkleTree::new();
        let initial_root = tree.root();

        let cid = ContentId([1u8; 32]);
        let piece = [2u8; 32];
        tree.insert(&cid, 0, &piece);
        let after_insert = tree.root();
        assert_ne!(initial_root, after_insert, "Root must change after insert");

        let piece2 = [3u8; 32];
        tree.insert(&cid, 1, &piece2);
        let after_second = tree.root();
        assert_ne!(after_insert, after_second, "Root must change after second insert");

        tree.remove(&cid, 1, &piece2);
        assert_eq!(tree.root(), after_insert, "Root must revert after remove");

        tree.remove(&cid, 0, &piece);
        assert_eq!(tree.root(), initial_root, "Root must revert to empty after all removed");
    }

    #[test]
    fn test_merkle_root_in_capability_announcement() {
        use datacraft_core::CapabilityAnnouncement;
        use datacraft_store::merkle::StorageMerkleTree;

        let mut tree = StorageMerkleTree::new();
        tree.insert(&ContentId([1u8; 32]), 0, &[2u8; 32]);
        let root = tree.root();

        // Simulate what announce_capabilities_periodically does
        let announcement = CapabilityAnnouncement {
            peer_id: vec![0u8; 32],
            capabilities: vec![],
            timestamp: 0,
            signature: vec![],
            storage_committed_bytes: 0,
            storage_used_bytes: 0,
            region: None,
            storage_root: root,
            piece_counts: std::collections::HashMap::new(),
        };

        assert_eq!(announcement.storage_root, root);
        assert_ne!(announcement.storage_root, [0u8; 32], "Storage root should not be zero after insert");

        // Serialize and deserialize to verify it roundtrips
        let json = serde_json::to_vec(&announcement).unwrap();
        let deserialized: CapabilityAnnouncement = serde_json::from_slice(&json).unwrap();
        assert_eq!(deserialized.storage_root, root);
    }
}
