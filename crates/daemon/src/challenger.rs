//! Challenger Manager
//!
//! Periodic PDP challenger loop integrated into the daemon event loop.
//! Manages rotation state, tracks provided CIDs, orchestrates challenge rounds,
//! and triggers self-healing when needed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use datacraft_core::{ChunkManifest, ContentId, StorageReceipt};
use datacraft_store::FsStore;
use ed25519_dalek::SigningKey;
use libp2p::PeerId;
use rand::seq::SliceRandom;
use rand::Rng;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;
use crate::health::{
    self, DutyCycleResult, ProviderInfo, TierInfo,
};
use crate::pdp::{
    ChallengerRotation, OnlineTimeTracker, PdpResponse, ReceiptStore,
    compute_proof_hash, verify_pdp_response,
};
use crate::peer_scorer::PeerScorer;
use crate::receipt_store::PersistentReceiptStore;

/// How often the challenger checks if any CID needs a challenge round.
pub const CHALLENGE_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// Minimum time between challenge rounds for the same CID.
pub const MIN_ROUND_INTERVAL: Duration = Duration::from_secs(600); // 10 minutes

/// Maximum number of chunks to sample per challenge round (statistical sufficiency).
const MAX_CHUNKS_TO_SAMPLE: usize = 8;

/// Tracks a provided CID and its challenge state.
#[derive(Debug)]
struct ProvidedCid {
    /// When we last ran a challenge round for this CID.
    last_challenged: Option<Instant>,
    /// Tier info (None for free CIDs).
    tier_info: Option<TierInfo>,
}

/// Manages the challenger duty cycle for all CIDs this node provides.
pub struct ChallengerManager {
    /// Our local peer ID.
    local_peer_id: PeerId,
    /// Our public key bytes (for receipts).
    local_pubkey: [u8; 32],
    /// CIDs this node provides, with challenge state.
    provided_cids: HashMap<ContentId, ProvidedCid>,
    /// Rotation state per CID.
    rotation: ChallengerRotation,
    /// Online time tracker for peers.
    online_tracker: OnlineTimeTracker,
    /// Receipt store for PDP receipts (in-memory).
    receipt_store: ReceiptStore,
    /// Command sender to the swarm event loop.
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    /// Ed25519 signing key for receipt signing.
    signing_key: Option<SigningKey>,
    /// Persistent receipt store (shared with handler).
    persistent_store: Option<Arc<Mutex<PersistentReceiptStore>>>,
    /// Shared peer scorer — PDP results feed into peer reliability scores.
    peer_scorer: Option<Arc<Mutex<PeerScorer>>>,
}

impl ChallengerManager {
    /// Create a new ChallengerManager.
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
            receipt_store: ReceiptStore::new(),
            command_tx,
            signing_key: None,
            persistent_store: None,
            peer_scorer: None,
        }
    }

    /// Set the shared peer scorer for recording PDP challenge results.
    pub fn set_peer_scorer(&mut self, scorer: Arc<Mutex<PeerScorer>>) {
        self.peer_scorer = Some(scorer);
    }

    /// Set the signing key for receipt signing.
    pub fn set_signing_key(&mut self, key: SigningKey) {
        self.signing_key = Some(key);
    }

    /// Set the persistent receipt store.
    pub fn set_persistent_store(&mut self, store: Arc<Mutex<PersistentReceiptStore>>) {
        self.persistent_store = Some(store);
    }

    /// Register a CID that this node provides.
    pub fn register_cid(&mut self, cid: ContentId, tier_info: Option<TierInfo>) {
        info!("Registering CID {} for challenger tracking", cid);
        self.provided_cids.entry(cid).or_insert(ProvidedCid {
            last_challenged: None,
            tier_info,
        });
        // Track ourselves as a provider
        self.online_tracker.observe(cid, self.local_peer_id);
    }

    /// Unregister a CID (e.g. on unpin).
    pub fn unregister_cid(&mut self, cid: &ContentId) {
        info!("Unregistering CID {} from challenger tracking", cid);
        self.provided_cids.remove(cid);
    }

    /// Get a reference to the receipt store.
    pub fn receipt_store(&self) -> &ReceiptStore {
        &self.receipt_store
    }

    /// Get a mutable reference to the receipt store.
    pub fn receipt_store_mut(&mut self) -> &mut ReceiptStore {
        &mut self.receipt_store
    }

    /// Check all provided CIDs and run challenge rounds for those that are due.
    ///
    /// Returns the number of challenge rounds executed.
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
    ///
    /// Full duty cycle:
    /// 1. get_providers from DHT
    /// 2. Check rotation — is it our turn?
    /// 3. Fetch k-1 shards from providers
    /// 4. PDP challenge each provider
    /// 5. Verify, assess health, heal if needed
    /// 6. Store receipts
    pub async fn run_challenge_round(
        &mut self,
        store: &FsStore,
        cid: ContentId,
    ) -> Result<DutyCycleResult, String> {
        // 1. Get providers from DHT
        let providers = self.resolve_providers(cid).await?;
        if providers.is_empty() {
            return Err("No providers found".into());
        }

        // Update online tracker with all providers
        for peer in &providers {
            self.online_tracker.observe(cid, *peer);
        }

        // 2. Check rotation: is it our turn?
        let sorted = self.online_tracker.sort_by_online_time(&cid, &providers);
        let challenger = self
            .rotation
            .current_challenger(&cid, &sorted)
            .ok_or("No challenger available")?;

        if challenger != self.local_peer_id {
            debug!(
                "Not our turn to challenge {} (challenger: {})",
                cid, challenger
            );
            // Still update last_challenged time to avoid re-checking immediately
            if let Some(state) = self.provided_cids.get_mut(&cid) {
                state.last_challenged = Some(Instant::now());
            }
            return Err("Not our turn to challenge".into());
        }

        info!("Running challenge round for {} (we are challenger)", cid);

        // Get manifest from local store
        let manifest = store
            .get_manifest(&cid)
            .map_err(|e| format!("Failed to get manifest: {}", e))?;
        let k = manifest.k;

        // Determine which chunks to sample (random subset)
        let chunks_to_sample = self.select_chunks_to_sample(manifest.chunk_count);

        // 3. Fetch k-1 shards from other providers for sampled chunks
        // We already have our own shards locally. Fetch from others to
        // be able to verify their PDP responses.
        let other_providers: Vec<PeerId> = providers
            .iter()
            .filter(|p| **p != self.local_peer_id)
            .copied()
            .collect();

        // For each sampled chunk, fetch shards from a random subset of providers
        let mut fetched_shards: HashMap<(u32, u8), Vec<u8>> = HashMap::new();
        let shards_needed = k.saturating_sub(1); // we have 1 shard locally

        for &chunk_idx in &chunks_to_sample {
            let mut providers_to_fetch: Vec<&PeerId> = other_providers.iter().collect();
            providers_to_fetch.shuffle(&mut rand::thread_rng());

            let mut fetched_for_chunk = 0;
            for peer in providers_to_fetch.iter().take(shards_needed) {
                // Try to figure out which shard index this peer holds
                // For now, try sequential indices
                for shard_idx in 0..=(k + other_providers.len()).min(255) {
                    if fetched_for_chunk >= shards_needed {
                        break;
                    }
                    match self
                        .request_shard(**peer, cid, chunk_idx, shard_idx as u8)
                        .await
                    {
                        Ok(data) => {
                            fetched_shards.insert((chunk_idx, shard_idx as u8), data);
                            fetched_for_chunk += 1;
                            break; // Got one from this peer, move to next
                        }
                        Err(_) => continue,
                    }
                }
            }
        }

        // 4. Send PDP challenges to each provider for sampled chunks
        let mut pdp_responses: Vec<(PeerId, u32, Option<PdpResponse>)> = Vec::new();
        let mut provider_infos: Vec<ProviderInfo> = Vec::new();

        for peer in &other_providers {
            for &chunk_idx in &chunks_to_sample {
                // Query the peer's shard index for this CID
                let shard_index = self
                    .query_max_shard_index(*peer, cid)
                    .await
                    .unwrap_or(None)
                    .unwrap_or(0) as u32;

                let nonce: [u8; 32] = rand::thread_rng().gen();

                let challenge_start = Instant::now();
                match self
                    .send_pdp_challenge(*peer, cid, chunk_idx, shard_index, nonce)
                    .await
                {
                    Ok(response) => {
                        let latency = challenge_start.elapsed();
                        // Verify: we need the shard data to check
                        let verified = if let Some(local_data) =
                            store.get_shard(&cid, chunk_idx, shard_index as u8).ok()
                        {
                            verify_pdp_response(&local_data, &nonce, &response)
                        } else if let Some(fetched) =
                            fetched_shards.get(&(chunk_idx, shard_index as u8))
                        {
                            verify_pdp_response(fetched, &nonce, &response)
                        } else {
                            // Can't verify — we don't have the reference data
                            warn!(
                                "Cannot verify PDP for peer {} shard {} — missing reference",
                                peer, shard_index
                            );
                            false
                        };

                        // Record PDP result in peer scorer
                        if let Some(ref scorer) = self.peer_scorer {
                            let mut s = scorer.lock().await;
                            if verified {
                                s.record_success(peer, latency);
                            } else {
                                s.record_failure(peer);
                            }
                        }

                        pdp_responses.push((
                            *peer,
                            shard_index,
                            if verified { Some(response) } else { None },
                        ));
                    }
                    Err(e) => {
                        debug!("PDP challenge to {} failed: {}", peer, e);
                        // Record timeout in peer scorer (challenge didn't complete)
                        if let Some(ref scorer) = self.peer_scorer {
                            scorer.lock().await.record_timeout(peer);
                        }
                        pdp_responses.push((*peer, shard_index, None));
                    }
                }

                provider_infos.push(ProviderInfo {
                    peer_id: *peer,
                    shard_index,
                });
            }
        }

        // 5. Get tier info
        let tier_info = self.provided_cids.get(&cid).and_then(|s| s.tier_info.clone());

        // Get current max shard index across the network
        let mut max_shard_index: u8 = 0;
        for peer in &other_providers {
            if let Ok(Some(idx)) = self.query_max_shard_index(*peer, cid).await {
                max_shard_index = max_shard_index.max(idx);
            }
        }
        // Also check local max
        if let Some(local_max) = store.max_shard_index_for_content(&cid) {
            max_shard_index = max_shard_index.max(local_max);
        }

        // 6. Run local duty cycle (assess health, heal if needed)
        let mut result = health::run_challenger_duty_local(
            store,
            &manifest,
            &provider_infos,
            &pdp_responses,
            self.local_pubkey,
            tier_info.as_ref(),
            max_shard_index,
        );

        // 7. If healing occurred, announce new shards
        if let Some(ref healing) = result.healing {
            for &idx in &healing.generated_indices {
                info!("Announcing healed shard {} for {}", idx, cid);
                // Announce via DHT
                if let Err(e) = self.announce_provider(cid, &manifest).await {
                    warn!("Failed to announce healed shard: {}", e);
                }
            }
        }

        // 8. Sign and store receipts
        let mut signed_receipts: Vec<StorageReceipt> = Vec::new();
        for mut receipt in result.pdp_results.receipts() {
            if let Some(ref key) = self.signing_key {
                datacraft_core::signing::sign_storage_receipt(&mut receipt, key);
            }
            signed_receipts.push(receipt.clone());
            self.receipt_store.add_issued(receipt);
        }
        if let Some(ref mut cr) = result.challenger_receipt {
            if let Some(ref key) = self.signing_key {
                datacraft_core::signing::sign_storage_receipt(cr, key);
            }
            signed_receipts.push(cr.clone());
            self.receipt_store.add_issued(cr.clone());
        }

        // Persist to disk and broadcast via gossipsub
        if let Some(ref persistent) = self.persistent_store {
            let mut store = persistent.lock().await;
            for receipt in &signed_receipts {
                if let Err(e) = store.add_storage(receipt.clone()) {
                    warn!("Failed to persist storage receipt: {}", e);
                }
            }
            info!(
                "Persisted {} storage receipts for {}",
                signed_receipts.len(),
                cid
            );
        }

        // Broadcast receipts via gossipsub for aggregator collection
        for receipt in &signed_receipts {
            if let Ok(data) = bincode::serialize(receipt) {
                let _ = self.command_tx.send(
                    DataCraftCommand::BroadcastStorageReceipt { receipt_data: data }
                );
            }
        }

        // Advance rotation
        self.rotation.advance(&cid, &sorted);

        // Update last challenged time
        if let Some(state) = self.provided_cids.get_mut(&cid) {
            state.last_challenged = Some(Instant::now());
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Helper methods that communicate with the swarm via commands
    // -----------------------------------------------------------------------

    /// Select a random subset of chunk indices to sample.
    fn select_chunks_to_sample(&self, chunk_count: usize) -> Vec<u32> {
        let mut rng = rand::thread_rng();
        let count = chunk_count.min(MAX_CHUNKS_TO_SAMPLE);
        let mut indices: Vec<u32> = (0..chunk_count as u32).collect();
        indices.shuffle(&mut rng);
        indices.truncate(count);
        indices
    }

    /// Resolve providers for a CID via the swarm.
    async fn resolve_providers(&self, cid: ContentId) -> Result<Vec<PeerId>, String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::ResolveProviders {
                content_id: cid,
                reply_tx: tx,
            })
            .map_err(|e| format!("Failed to send resolve command: {}", e))?;
        rx.await.map_err(|e| format!("Channel closed: {}", e))?
    }

    /// Request a shard from a peer via the swarm.
    async fn request_shard(
        &self,
        peer: PeerId,
        cid: ContentId,
        chunk_index: u32,
        shard_index: u8,
    ) -> Result<Vec<u8>, String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::RequestShard {
                peer_id: peer,
                content_id: cid,
                chunk_index,
                shard_index,
                local_public_key: self.local_pubkey,
                reply_tx: tx,
            })
            .map_err(|e| format!("Failed to send shard request: {}", e))?;
        rx.await.map_err(|e| format!("Channel closed: {}", e))?
    }

    /// Query a peer's max shard index via the swarm.
    async fn query_max_shard_index(
        &self,
        peer: PeerId,
        cid: ContentId,
    ) -> Result<Option<u8>, String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::QueryMaxShardIndex {
                peer_id: peer,
                content_id: cid,
                reply_tx: tx,
            })
            .map_err(|e| format!("Failed to send query: {}", e))?;
        rx.await.map_err(|e| format!("Channel closed: {}", e))?
    }

    /// Announce as provider for a CID via the swarm.
    async fn announce_provider(
        &self,
        cid: ContentId,
        manifest: &ChunkManifest,
    ) -> Result<(), String> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(DataCraftCommand::AnnounceProvider {
                content_id: cid,
                manifest: manifest.clone(),
                reply_tx: tx,
            })
            .map_err(|e| format!("Failed to send announce: {}", e))?;
        rx.await.map_err(|e| format!("Channel closed: {}", e))?
    }

    /// Send a PDP challenge to a peer.
    ///
    /// This uses the RequestShard command as a proxy — in production, this would
    /// open a dedicated `/datacraft/pdp/1.0.0` stream. For now, we simulate PDP
    /// by requesting the shard and computing the proof locally.
    async fn send_pdp_challenge(
        &self,
        peer: PeerId,
        cid: ContentId,
        chunk_index: u32,
        shard_index: u32,
        nonce: [u8; 32],
    ) -> Result<PdpResponse, String> {
        // In production: open a PDP stream, send challenge, get response.
        // For now: request the shard and verify we can compute the proof.
        // This is a simplification — the real PDP protocol doesn't transfer
        // the full shard, just a hash proof.
        let shard_data = self
            .request_shard(peer, cid, chunk_index, shard_index as u8)
            .await?;
        let proof_hash = compute_proof_hash(&shard_data, &nonce);
        Ok(PdpResponse { proof_hash })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdp::ChallengerRotation;

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
    fn test_challenger_manager_register_with_tier() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let peer_id = PeerId::random();
        let mut mgr = ChallengerManager::new(peer_id, [0u8; 32], tx);

        let cid = ContentId([2u8; 32]);
        let tier = TierInfo {
            min_shard_ratio: 3.0,
        };
        mgr.register_cid(cid, Some(tier));

        let state = mgr.provided_cids.get(&cid).unwrap();
        assert!(state.tier_info.is_some());
        assert!((state.tier_info.as_ref().unwrap().min_shard_ratio - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_select_chunks_to_sample() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mgr = ChallengerManager::new(PeerId::random(), [0u8; 32], tx);

        // Small content: all chunks sampled
        let chunks = mgr.select_chunks_to_sample(3);
        assert_eq!(chunks.len(), 3);

        // Large content: capped at MAX_CHUNKS_TO_SAMPLE
        let chunks = mgr.select_chunks_to_sample(100);
        assert_eq!(chunks.len(), MAX_CHUNKS_TO_SAMPLE);

        // All indices should be valid
        for idx in &chunks {
            assert!(*idx < 100);
        }

        // No duplicates
        let unique: std::collections::HashSet<u32> = chunks.iter().copied().collect();
        assert_eq!(unique.len(), chunks.len());
    }

    #[test]
    fn test_rotation_turn_detection() {
        let us = PeerId::random();
        let other1 = PeerId::random();
        let other2 = PeerId::random();

        let cid = ContentId([3u8; 32]);
        let mut tracker = OnlineTimeTracker::new();
        let now = Instant::now();

        // Us online longest
        tracker.observe_at(cid, us, now - Duration::from_secs(300));
        tracker.observe_at(cid, other1, now - Duration::from_secs(200));
        tracker.observe_at(cid, other2, now - Duration::from_secs(100));

        let sorted = tracker.sort_by_online_time(&cid, &[us, other1, other2]);
        assert_eq!(sorted[0], us); // We're first (longest online)

        let rotation = ChallengerRotation::new();
        let challenger = rotation.current_challenger(&cid, &sorted).unwrap();
        assert_eq!(challenger, us); // It's our turn
    }

    #[test]
    fn test_rotation_not_our_turn() {
        let us = PeerId::random();
        let other = PeerId::random();

        let cid = ContentId([4u8; 32]);
        let mut tracker = OnlineTimeTracker::new();
        let now = Instant::now();

        // Other peer online longer
        tracker.observe_at(cid, other, now - Duration::from_secs(300));
        tracker.observe_at(cid, us, now - Duration::from_secs(100));

        let sorted = tracker.sort_by_online_time(&cid, &[us, other]);
        assert_eq!(sorted[0], other); // Other peer is first

        let rotation = ChallengerRotation::new();
        let challenger = rotation.current_challenger(&cid, &sorted).unwrap();
        assert_eq!(challenger, other); // Not our turn
        assert_ne!(challenger, us);
    }

    #[test]
    fn test_due_cid_selection() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut mgr = ChallengerManager::new(PeerId::random(), [0u8; 32], tx);

        let cid1 = ContentId([10u8; 32]);
        let cid2 = ContentId([11u8; 32]);

        mgr.register_cid(cid1, None);
        mgr.register_cid(cid2, None);

        // Both should be due (never challenged)
        let now = Instant::now();
        let due: Vec<ContentId> = mgr
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
        assert_eq!(due.len(), 2);

        // Mark cid1 as recently challenged
        mgr.provided_cids.get_mut(&cid1).unwrap().last_challenged = Some(Instant::now());

        let due: Vec<ContentId> = mgr
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
        assert_eq!(due.len(), 1);
        assert_eq!(due[0], cid2);
    }

    // -----------------------------------------------------------------------
    // StorageReceipt generation, signing, verification, and persistence tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_signed_receipt_generation() {
        use crate::pdp::create_signed_storage_receipt;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let key = SigningKey::generate(&mut OsRng);
        let cid = ContentId([42u8; 32]);
        let receipt = create_signed_storage_receipt(
            cid,
            [1u8; 32],
            key.verifying_key().to_bytes(),
            3,
            [9u8; 32],
            [10u8; 32],
            &key,
        );

        assert_eq!(receipt.content_id, cid);
        assert_eq!(receipt.shard_index, 3);
        assert_eq!(receipt.signature.len(), 64);
        assert!(receipt.timestamp > 0);
    }

    #[test]
    fn test_signed_receipt_verification() {
        use crate::pdp::create_signed_storage_receipt;
        use datacraft_core::signing::verify_storage_receipt;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let key = SigningKey::generate(&mut OsRng);
        let pubkey = key.verifying_key();
        let receipt = create_signed_storage_receipt(
            ContentId([50u8; 32]),
            [1u8; 32],
            pubkey.to_bytes(),
            0,
            [5u8; 32],
            [6u8; 32],
            &key,
        );

        assert!(verify_storage_receipt(&receipt, &pubkey));
    }

    #[test]
    fn test_signed_receipt_tamper_detection() {
        use crate::pdp::create_signed_storage_receipt;
        use datacraft_core::signing::verify_storage_receipt;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let key = SigningKey::generate(&mut OsRng);
        let pubkey = key.verifying_key();
        let mut receipt = create_signed_storage_receipt(
            ContentId([60u8; 32]),
            [1u8; 32],
            pubkey.to_bytes(),
            0,
            [7u8; 32],
            [8u8; 32],
            &key,
        );

        // Tamper with shard_index
        receipt.shard_index = 99;
        assert!(!verify_storage_receipt(&receipt, &pubkey));
    }

    #[test]
    fn test_signed_receipt_wrong_key_fails() {
        use crate::pdp::create_signed_storage_receipt;
        use datacraft_core::signing::verify_storage_receipt;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let key1 = SigningKey::generate(&mut OsRng);
        let key2 = SigningKey::generate(&mut OsRng);

        let receipt = create_signed_storage_receipt(
            ContentId([70u8; 32]),
            [1u8; 32],
            key1.verifying_key().to_bytes(),
            0,
            [0u8; 32],
            [0u8; 32],
            &key1,
        );

        assert!(!verify_storage_receipt(&receipt, &key2.verifying_key()));
    }

    #[test]
    fn test_receipt_persistence_roundtrip() {
        use crate::pdp::create_signed_storage_receipt;
        use crate::receipt_store::PersistentReceiptStore;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;
        use std::path::PathBuf;

        let key = SigningKey::generate(&mut OsRng);
        let cid = ContentId([80u8; 32]);
        let path = std::env::temp_dir().join(format!(
            "challenger-persist-test-{}.bin",
            std::process::id()
        ));

        {
            let mut store = PersistentReceiptStore::new(path.clone()).unwrap();
            for shard in 0..5u32 {
                let receipt = create_signed_storage_receipt(
                    cid,
                    [1u8; 32],
                    key.verifying_key().to_bytes(),
                    shard,
                    [shard as u8; 32],
                    [(shard + 10) as u8; 32],
                    &key,
                );
                assert!(store.add_storage(receipt).unwrap());
            }
            assert_eq!(store.storage_receipt_count(), 5);
        }

        // Reopen and verify persistence
        {
            let store = PersistentReceiptStore::new(path.clone()).unwrap();
            assert_eq!(store.storage_receipt_count(), 5);
            let receipts = store.all_storage_receipts();
            for r in receipts {
                assert_eq!(r.content_id, cid);
                assert_eq!(r.signature.len(), 64);
            }
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_receipt_dedup_in_persistence() {
        use crate::pdp::create_signed_storage_receipt;
        use crate::receipt_store::PersistentReceiptStore;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let key = SigningKey::generate(&mut OsRng);
        let path = std::env::temp_dir().join(format!(
            "challenger-dedup-test-{}.bin",
            std::process::id()
        ));

        let mut store = PersistentReceiptStore::new(path.clone()).unwrap();

        let receipt = create_signed_storage_receipt(
            ContentId([90u8; 32]),
            [1u8; 32],
            key.verifying_key().to_bytes(),
            0,
            [0u8; 32],
            [0u8; 32],
            &key,
        );

        assert!(store.add_storage(receipt.clone()).unwrap());
        assert!(!store.add_storage(receipt).unwrap()); // duplicate
        assert_eq!(store.storage_receipt_count(), 1);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_challenger_manager_with_signing() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let key = ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng);
        let pubkey = key.verifying_key().to_bytes();
        let mut mgr = ChallengerManager::new(PeerId::random(), pubkey, tx);
        mgr.set_signing_key(key);

        assert!(mgr.signing_key.is_some());
        assert!(mgr.persistent_store.is_none());
    }

    #[test]
    fn test_challenger_manager_with_persistent_store() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let path = std::env::temp_dir().join(format!(
            "challenger-mgr-store-{}.bin",
            std::process::id()
        ));
        let store = Arc::new(Mutex::new(
            PersistentReceiptStore::new(path.clone()).unwrap(),
        ));

        let mut mgr = ChallengerManager::new(PeerId::random(), [0u8; 32], tx);
        mgr.set_persistent_store(store);
        assert!(mgr.persistent_store.is_some());

        std::fs::remove_file(&path).ok();
    }
}
