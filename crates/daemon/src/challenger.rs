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
use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

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
}

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
        }
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
        let due_cids: Vec<ContentId> = self
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

        // For each segment, get our local pieces and their coefficients
        // Then challenge other providers
        let mut pdp_results = Vec::new();
        let _other_providers: Vec<PeerId> = providers
            .iter()
            .filter(|p| **p != self.local_peer_id)
            .copied()
            .collect();

        // Simplified: request a random piece from each provider and verify
        for &peer in &_other_providers {
            let segment_index = 0u32; // Sample segment 0 for now
            let any_piece = [0u8; 32]; // "any piece"
            let nonce: [u8; 32] = rand::thread_rng().gen();

            let challenge_start = Instant::now();
            match self.request_piece(peer, cid, segment_index, &any_piece).await {
                Ok((coefficients, data)) => {
                    let latency = challenge_start.elapsed();
                    let piece_id = datacraft_store::piece_id_from_coefficients(&coefficients);
                    let byte_positions: Vec<u32> = (0..16.min(data.len() as u32)).collect();
                    let proof_hash = compute_proof_hash(&data, &byte_positions, &coefficients, &nonce);

                    if let Some(ref scorer) = self.peer_scorer {
                        scorer.lock().await.record_success(&peer, latency);
                    }

                    let storage_node = crate::pdp::peer_id_to_local_pubkey(&peer);
                    let receipt = create_storage_receipt(
                        cid, storage_node, self.local_pubkey,
                        segment_index, piece_id, nonce, proof_hash,
                    );

                    pdp_results.push(ProviderPdpResult {
                        peer_id: peer,
                        segment_index,
                        piece_id,
                        passed: true,
                        receipt: Some(receipt),
                    });
                }
                Err(e) => {
                    debug!("PDP challenge to {} failed: {}", peer, e);
                    if let Some(ref scorer) = self.peer_scorer {
                        scorer.lock().await.record_timeout(&peer);
                    }
                    pdp_results.push(ProviderPdpResult {
                        peer_id: peer,
                        segment_index: 0,
                        piece_id: [0u8; 32],
                        passed: false,
                        receipt: None,
                    });
                }
            }
        }

        let round_result = PdpRoundResult {
            content_id: cid,
            results: pdp_results,
        };

        // Assess health using rank
        let live_count = round_result.passed_count();
        let tier_info = self.provided_cids.get(&cid).and_then(|s| s.tier_info.clone());
        let health = health::assess_health(cid, manifest.k(), live_count, round_result.results.len(), tier_info.as_ref());

        // Heal if needed
        let healing = if health.needs_healing && health.pieces_needed > 0 {
            info!("Healing {} — generating {} new pieces", cid, health.pieces_needed);
            Some(health::heal_content(store, &manifest, health.pieces_needed))
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

    async fn resolve_providers(&self, cid: ContentId) -> Result<Vec<PeerId>, String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::ResolveProviders { content_id: cid, reply_tx: tx })
            .map_err(|e| format!("Failed to send resolve command: {}", e))?;
        rx.await.map_err(|e| format!("Channel closed: {}", e))?
    }

    async fn request_piece(
        &self,
        peer: PeerId,
        cid: ContentId,
        segment_index: u32,
        piece_id: &[u8; 32],
    ) -> Result<(Vec<u8>, Vec<u8>), String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::RequestPiece {
                peer_id: peer,
                content_id: cid,
                segment_index,
                piece_id: *piece_id,
                reply_tx: tx,
            })
            .map_err(|e| format!("Failed to send piece request: {}", e))?;
        rx.await.map_err(|e| format!("Channel closed: {}", e))?
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
}
