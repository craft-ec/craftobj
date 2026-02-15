//! CID Health Assessment and Self-Healing
//!
//! After PDP verification, assesses CID health (live shards vs required)
//! and triggers self-healing by generating new parity shards when needed.

use datacraft_core::{ChunkManifest, ContentId, StorageReceipt};
use datacraft_store::FsStore;
use libp2p::PeerId;
use tracing::{info, warn};

use crate::pdp::{
    PdpResponse, create_storage_receipt, verify_pdp_response,
};

// ---------------------------------------------------------------------------
// Tier info
// ---------------------------------------------------------------------------

/// Tier information for a funded CID.
#[derive(Debug, Clone)]
pub struct TierInfo {
    /// Minimum shard ratio (live_shards / k). E.g. 2.0 for Lite, 3.0 for Standard.
    pub min_shard_ratio: f64,
}

// ---------------------------------------------------------------------------
// CID Health
// ---------------------------------------------------------------------------

/// Health assessment for a CID after PDP verification.
#[derive(Debug, Clone)]
pub struct CidHealth {
    /// The content ID assessed.
    pub content_id: ContentId,
    /// Data shards needed for reconstruction (from manifest).
    pub k: usize,
    /// Number of providers that passed PDP.
    pub live_shards: usize,
    /// Total providers queried.
    pub total_providers: usize,
    /// Ratio of live shards to k.
    pub shard_ratio: f64,
    /// Tier minimum ratio (None for free CIDs).
    pub tier_minimum: Option<f64>,
    /// Whether healing is needed.
    pub needs_healing: bool,
    /// Number of new shards to generate.
    pub shards_needed: usize,
}

/// Assess health of a CID given PDP results.
///
/// - `live_shards`: number of providers that passed PDP
/// - `total_providers`: total providers challenged
/// - `k`: data shards from manifest
/// - `tier_info`: None for free CIDs, Some for funded CIDs
pub fn assess_health(
    content_id: ContentId,
    k: usize,
    live_shards: usize,
    total_providers: usize,
    tier_info: Option<&TierInfo>,
) -> CidHealth {
    let shard_ratio = if k > 0 {
        live_shards as f64 / k as f64
    } else {
        0.0
    };

    let tier_minimum = tier_info.map(|t| t.min_shard_ratio);

    let (needs_healing, shards_needed) = match tier_info {
        Some(ti) => {
            let required = (ti.min_shard_ratio * k as f64).ceil() as usize;
            if live_shards < required {
                (true, required - live_shards)
            } else {
                (false, 0)
            }
        }
        // Free CIDs: no automatic healing
        None => (false, 0),
    };

    CidHealth {
        content_id,
        k,
        live_shards,
        total_providers,
        shard_ratio,
        tier_minimum,
        needs_healing,
        shards_needed,
    }
}

// ---------------------------------------------------------------------------
// PDP results
// ---------------------------------------------------------------------------

/// Result of a PDP challenge to a single provider.
#[derive(Debug, Clone)]
pub struct ProviderPdpResult {
    pub peer_id: PeerId,
    pub shard_index: u32,
    pub passed: bool,
    pub receipt: Option<StorageReceipt>,
}

/// Result of challenging all providers for a CID.
#[derive(Debug, Clone)]
pub struct PdpRoundResult {
    pub content_id: ContentId,
    pub results: Vec<ProviderPdpResult>,
}

impl PdpRoundResult {
    /// Count providers that passed.
    pub fn passed_count(&self) -> usize {
        self.results.iter().filter(|r| r.passed).count()
    }

    /// Count providers that failed.
    pub fn failed_count(&self) -> usize {
        self.results.iter().filter(|r| !r.passed).count()
    }

    /// Collect all issued receipts.
    pub fn receipts(&self) -> Vec<StorageReceipt> {
        self.results
            .iter()
            .filter_map(|r| r.receipt.clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Healing result
// ---------------------------------------------------------------------------

/// Result of a self-healing operation.
#[derive(Debug, Clone)]
pub struct HealingResult {
    /// Shard indices that were generated and stored.
    pub generated_indices: Vec<u8>,
    /// Number of shards successfully generated.
    pub shards_generated: usize,
    /// Errors encountered during healing (non-fatal).
    pub errors: Vec<String>,
}

/// Generate new parity shards to heal a CID.
///
/// The caller must already have k data shards in the store.
/// `current_max_index` is the highest known shard index across the network.
/// Returns indices of successfully generated shards.
pub fn heal_content(
    store: &FsStore,
    manifest: &ChunkManifest,
    shards_needed: usize,
    current_max_index: u8,
) -> HealingResult {
    use datacraft_client::extension::extend_content;

    let mut generated = Vec::new();
    let mut errors = Vec::new();
    let mut next_index = current_max_index.saturating_add(1);

    for _ in 0..shards_needed {
        if next_index == u8::MAX {
            errors.push("Reached maximum shard index (255)".into());
            break;
        }

        match extend_content(store, manifest, next_index) {
            Ok(()) => {
                info!(
                    "Generated healing shard {} for {}",
                    next_index, manifest.content_id
                );
                generated.push(next_index);
                next_index = next_index.saturating_add(1);
            }
            Err(e) => {
                warn!(
                    "Failed to generate shard {} for {}: {}",
                    next_index, manifest.content_id, e
                );
                errors.push(format!("shard {}: {}", next_index, e));
                next_index = next_index.saturating_add(1);
            }
        }
    }

    HealingResult {
        shards_generated: generated.len(),
        generated_indices: generated,
        errors,
    }
}

// ---------------------------------------------------------------------------
// Challenger duty cycle
// ---------------------------------------------------------------------------

/// Full result of a challenger duty cycle.
#[derive(Debug, Clone)]
pub struct DutyCycleResult {
    /// PDP round results.
    pub pdp_results: PdpRoundResult,
    /// CID health assessment.
    pub health: CidHealth,
    /// Healing result (None if no healing was needed).
    pub healing: Option<HealingResult>,
    /// Receipt issued to the challenger for performing duty.
    pub challenger_receipt: Option<StorageReceipt>,
}

/// Provider info needed for PDP challenge.
#[derive(Debug, Clone)]
pub struct ProviderInfo {
    pub peer_id: PeerId,
    pub shard_index: u32,
}

/// Run the full challenger duty cycle for a CID.
///
/// This is the local/synchronous portion — it does NOT open network streams.
/// The caller is responsible for:
/// 1. Fetching k-1 shards from providers (network)
/// 2. Collecting PDP responses from providers (network)
/// 3. Passing results here for assessment and healing
///
/// For unit-testable orchestration, this function takes pre-collected data.
pub fn run_challenger_duty_local(
    store: &FsStore,
    manifest: &ChunkManifest,
    _providers: &[ProviderInfo],
    pdp_responses: &[(PeerId, u32, Option<PdpResponse>)], // (peer, shard_index, response)
    challenger_pubkey: [u8; 32],
    tier_info: Option<&TierInfo>,
    current_max_shard_index: u8,
) -> DutyCycleResult {
    let content_id = manifest.content_id;
    let k = manifest.k;

    // Step 1-2: Verify PDP responses
    let mut results = Vec::new();
    for (peer_id, shard_index, response) in pdp_responses {
        let passed = if let Some(resp) = response {
            // Challenger needs the shard data to verify
            match store.get_shard(&content_id, 0, *shard_index as u8) {
                Ok(shard_data) => {
                    let nonce = [0u8; 32]; // In production, use the actual nonce from the challenge
                    verify_pdp_response(&shard_data, &nonce, resp)
                }
                Err(_) => {
                    // Challenger doesn't have this shard — can't verify
                    // This shouldn't happen if challenger fetched k shards
                    warn!("Challenger missing shard {} for verification", shard_index);
                    false
                }
            }
        } else {
            // No response = failed
            false
        };

        let receipt = if passed {
            // Derive storage_node pubkey from PeerId (placeholder)
            let storage_node = {
                let bytes = peer_id.to_bytes();
                let mut key = [0u8; 32];
                let len = bytes.len().min(32);
                key[..len].copy_from_slice(&bytes[..len]);
                key
            };
            Some(create_storage_receipt(
                content_id,
                storage_node,
                challenger_pubkey,
                *shard_index,
                [0u8; 32], // nonce placeholder
                [0u8; 32], // proof_hash placeholder
            ))
        } else {
            None
        };

        results.push(ProviderPdpResult {
            peer_id: *peer_id,
            shard_index: *shard_index,
            passed,
            receipt,
        });
    }

    let pdp_results = PdpRoundResult {
        content_id,
        results,
    };

    // Step 3: Assess health
    let live_shards = pdp_results.passed_count();
    let total_providers = pdp_responses.len();
    let health = assess_health(content_id, k, live_shards, total_providers, tier_info);

    info!(
        "CID {} health: {}/{} live (ratio {:.2}x), needs_healing={}, shards_needed={}",
        content_id, health.live_shards, health.k,
        health.shard_ratio, health.needs_healing, health.shards_needed
    );

    // Step 4: Heal if needed
    let healing = if health.needs_healing && health.shards_needed > 0 {
        info!(
            "Healing {} — generating {} new parity shards",
            content_id, health.shards_needed
        );
        Some(heal_content(
            store,
            manifest,
            health.shards_needed,
            current_max_shard_index,
        ))
    } else {
        None
    };

    // Step 5: Challenger receipt (for performing verification duty)
    let challenger_receipt = Some(create_storage_receipt(
        content_id,
        challenger_pubkey,
        challenger_pubkey, // self-issued for duty
        0,                 // challenger's own shard
        [0u8; 32],
        [0u8; 32],
    ));

    DutyCycleResult {
        pdp_results,
        health,
        healing,
        challenger_receipt,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdp::compute_proof_hash;
    use datacraft_core::{default_erasure_config, ContentId};

    #[test]
    fn test_assess_health_funded_healthy() {
        let cid = ContentId([1u8; 32]);
        let tier = TierInfo { min_shard_ratio: 2.0 };
        // k=4, live=10 → ratio=2.5x, minimum=2.0x → healthy
        let health = assess_health(cid, 4, 10, 12, Some(&tier));
        assert_eq!(health.k, 4);
        assert_eq!(health.live_shards, 10);
        assert_eq!(health.total_providers, 12);
        assert!((health.shard_ratio - 2.5).abs() < 0.01);
        assert_eq!(health.tier_minimum, Some(2.0));
        assert!(!health.needs_healing);
        assert_eq!(health.shards_needed, 0);
    }

    #[test]
    fn test_assess_health_funded_needs_healing() {
        let cid = ContentId([2u8; 32]);
        let tier = TierInfo { min_shard_ratio: 3.0 };
        // k=4, live=8 → ratio=2.0x, minimum=3.0x → needs ceil(3.0*4)-8 = 12-8 = 4 shards
        let health = assess_health(cid, 4, 8, 10, Some(&tier));
        assert!(health.needs_healing);
        assert_eq!(health.shards_needed, 4);
    }

    #[test]
    fn test_assess_health_funded_exactly_at_minimum() {
        let cid = ContentId([3u8; 32]);
        let tier = TierInfo { min_shard_ratio: 2.0 };
        // k=4, live=8 → ratio=2.0x, minimum=2.0x → healthy (not below)
        let health = assess_health(cid, 4, 8, 8, Some(&tier));
        assert!(!health.needs_healing);
        assert_eq!(health.shards_needed, 0);
    }

    #[test]
    fn test_assess_health_free_cid() {
        let cid = ContentId([4u8; 32]);
        // Free CID: no tier info → no healing regardless of ratio
        let health = assess_health(cid, 4, 2, 5, None);
        assert_eq!(health.tier_minimum, None);
        assert!(!health.needs_healing);
        assert_eq!(health.shards_needed, 0);
        assert!((health.shard_ratio - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_assess_health_zero_k() {
        let cid = ContentId([5u8; 32]);
        let health = assess_health(cid, 0, 0, 0, None);
        assert_eq!(health.shard_ratio, 0.0);
        assert!(!health.needs_healing);
    }

    #[test]
    fn test_assess_health_fractional_ratio() {
        let cid = ContentId([6u8; 32]);
        let tier = TierInfo { min_shard_ratio: 2.5 };
        // k=4, required=ceil(2.5*4)=10, live=9 → needs 1
        let health = assess_health(cid, 4, 9, 12, Some(&tier));
        assert!(health.needs_healing);
        assert_eq!(health.shards_needed, 1);
    }

    #[test]
    fn test_assess_health_high_tier() {
        let cid = ContentId([7u8; 32]);
        let tier = TierInfo { min_shard_ratio: 10.0 };
        // k=4, required=40, live=20 → needs 20
        let health = assess_health(cid, 4, 20, 25, Some(&tier));
        assert!(health.needs_healing);
        assert_eq!(health.shards_needed, 20);
    }

    #[test]
    fn test_pdp_round_result_counts() {
        let results = PdpRoundResult {
            content_id: ContentId([0u8; 32]),
            results: vec![
                ProviderPdpResult {
                    peer_id: PeerId::random(),
                    shard_index: 0,
                    passed: true,
                    receipt: Some(create_storage_receipt(
                        ContentId([0u8; 32]), [1u8; 32], [2u8; 32], 0, [0u8; 32], [0u8; 32],
                    )),
                },
                ProviderPdpResult {
                    peer_id: PeerId::random(),
                    shard_index: 1,
                    passed: true,
                    receipt: Some(create_storage_receipt(
                        ContentId([0u8; 32]), [3u8; 32], [2u8; 32], 1, [0u8; 32], [0u8; 32],
                    )),
                },
                ProviderPdpResult {
                    peer_id: PeerId::random(),
                    shard_index: 2,
                    passed: false,
                    receipt: None,
                },
            ],
        };
        assert_eq!(results.passed_count(), 2);
        assert_eq!(results.failed_count(), 1);
        assert_eq!(results.receipts().len(), 2);
    }

    #[test]
    fn test_heal_content() {
        use craftec_erasure::ErasureCoder;

        let dir = std::env::temp_dir().join(format!("health-heal-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let config = default_erasure_config();
        let content = b"healing test content with some padding to fill shards nicely.";
        let content_id = ContentId::from_bytes(content);

        let coder = ErasureCoder::with_config(&config).unwrap();
        let shards = coder.encode(content).unwrap();

        // Store data shards (0..k)
        for (i, shard) in shards.iter().enumerate() {
            store.put_shard(&content_id, 0, i as u8, shard).unwrap();
        }

        let manifest = ChunkManifest {
            content_id,
            content_hash: content_id.0,
            k: config.data_shards,
            chunk_size: config.chunk_size,
            chunk_count: 1,
            erasure_config: config.clone(),
            content_size: content.len() as u64,
            creator: String::new(),
            signature: vec![],
        };
        store.put_manifest(&manifest).unwrap();

        // Heal: generate 2 new shards starting after index 7
        let result = heal_content(&store, &manifest, 2, 7);
        assert_eq!(result.shards_generated, 2);
        assert_eq!(result.generated_indices, vec![8, 9]);
        assert!(result.errors.is_empty());

        // Verify the generated shards exist
        assert!(store.has_shard(&content_id, 0, 8));
        assert!(store.has_shard(&content_id, 0, 9));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_duty_cycle_local_no_healing() {
        use craftec_erasure::ErasureCoder;

        let dir = std::env::temp_dir().join(format!("duty-no-heal-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let config = default_erasure_config();
        let content = b"duty cycle test content for health check without healing needed.";
        let content_id = ContentId::from_bytes(content);

        let coder = ErasureCoder::with_config(&config).unwrap();
        let shards = coder.encode(content).unwrap();

        for (i, shard) in shards.iter().enumerate() {
            store.put_shard(&content_id, 0, i as u8, shard).unwrap();
        }

        let manifest = ChunkManifest {
            content_id,
            content_hash: content_id.0,
            k: config.data_shards,
            chunk_size: config.chunk_size,
            chunk_count: 1,
            erasure_config: config.clone(),
            content_size: content.len() as u64,
            creator: String::new(),
            signature: vec![],
        };

        let providers: Vec<ProviderInfo> = (0..8)
            .map(|i| ProviderInfo {
                peer_id: PeerId::random(),
                shard_index: i,
            })
            .collect();

        // All providers pass — build valid responses
        let nonce = [0u8; 32];
        let pdp_responses: Vec<(PeerId, u32, Option<PdpResponse>)> = providers
            .iter()
            .map(|p| {
                let shard_data = store
                    .get_shard(&content_id, 0, p.shard_index as u8)
                    .unwrap();
                let proof_hash = compute_proof_hash(&shard_data, &nonce);
                (p.peer_id, p.shard_index, Some(PdpResponse { proof_hash }))
            })
            .collect();

        let tier = TierInfo { min_shard_ratio: 2.0 };
        let result = run_challenger_duty_local(
            &store,
            &manifest,
            &providers,
            &pdp_responses,
            [99u8; 32],
            Some(&tier),
            7,
        );

        assert_eq!(result.pdp_results.passed_count(), 8);
        assert!(!result.health.needs_healing);
        assert!(result.healing.is_none());
        assert!(result.challenger_receipt.is_some());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_duty_cycle_local_with_healing() {
        use craftec_erasure::ErasureCoder;

        let dir = std::env::temp_dir().join(format!("duty-heal-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let config = default_erasure_config();
        let content = b"duty cycle healing test - some providers will fail PDP check.";
        let content_id = ContentId::from_bytes(content);

        let coder = ErasureCoder::with_config(&config).unwrap();
        let shards = coder.encode(content).unwrap();

        for (i, shard) in shards.iter().enumerate() {
            store.put_shard(&content_id, 0, i as u8, shard).unwrap();
        }

        let manifest = ChunkManifest {
            content_id,
            content_hash: content_id.0,
            k: config.data_shards,
            chunk_size: config.chunk_size,
            chunk_count: 1,
            erasure_config: config.clone(),
            content_size: content.len() as u64,
            creator: String::new(),
            signature: vec![],
        };

        // 8 providers, but 3 fail (return None response)
        let providers: Vec<ProviderInfo> = (0..8)
            .map(|i| ProviderInfo {
                peer_id: PeerId::random(),
                shard_index: i,
            })
            .collect();

        let nonce = [0u8; 32];
        let pdp_responses: Vec<(PeerId, u32, Option<PdpResponse>)> = providers
            .iter()
            .enumerate()
            .map(|(idx, p)| {
                if idx >= 5 {
                    // These 3 providers fail (no response)
                    (p.peer_id, p.shard_index, None)
                } else {
                    let shard_data = store
                        .get_shard(&content_id, 0, p.shard_index as u8)
                        .unwrap();
                    let proof_hash = compute_proof_hash(&shard_data, &nonce);
                    (p.peer_id, p.shard_index, Some(PdpResponse { proof_hash }))
                }
            })
            .collect();

        // Tier requires 3.0x → need ceil(3.0*4) = 12 live shards
        // Only 5 passed → need 12-5 = 7 new shards
        let tier = TierInfo { min_shard_ratio: 3.0 };
        let result = run_challenger_duty_local(
            &store,
            &manifest,
            &providers,
            &pdp_responses,
            [99u8; 32],
            Some(&tier),
            7,
        );

        assert_eq!(result.pdp_results.passed_count(), 5);
        assert_eq!(result.pdp_results.failed_count(), 3);
        assert!(result.health.needs_healing);
        assert_eq!(result.health.shards_needed, 7);

        let healing = result.healing.unwrap();
        assert_eq!(healing.shards_generated, 7);
        assert_eq!(healing.generated_indices, vec![8, 9, 10, 11, 12, 13, 14]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_duty_cycle_free_cid_no_healing() {
        let dir = std::env::temp_dir().join(format!("duty-free-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let config = default_erasure_config();
        let content = b"free cid content - no healing even if shards are low.";
        let content_id = ContentId::from_bytes(content);

        let coder = craftec_erasure::ErasureCoder::with_config(&config).unwrap();
        let shards = coder.encode(content).unwrap();

        for (i, shard) in shards.iter().enumerate() {
            store.put_shard(&content_id, 0, i as u8, shard).unwrap();
        }

        let manifest = ChunkManifest {
            content_id,
            content_hash: content_id.0,
            k: config.data_shards,
            chunk_size: config.chunk_size,
            chunk_count: 1,
            erasure_config: config.clone(),
            content_size: content.len() as u64,
            creator: String::new(),
            signature: vec![],
        };

        // Only 2 of 5 providers pass — but free CID, no healing
        let providers: Vec<ProviderInfo> = (0..5)
            .map(|i| ProviderInfo {
                peer_id: PeerId::random(),
                shard_index: i,
            })
            .collect();

        let nonce = [0u8; 32];
        let pdp_responses: Vec<(PeerId, u32, Option<PdpResponse>)> = providers
            .iter()
            .enumerate()
            .map(|(idx, p)| {
                if idx >= 2 {
                    (p.peer_id, p.shard_index, None)
                } else {
                    let shard_data = store
                        .get_shard(&content_id, 0, p.shard_index as u8)
                        .unwrap();
                    let proof_hash = compute_proof_hash(&shard_data, &nonce);
                    (p.peer_id, p.shard_index, Some(PdpResponse { proof_hash }))
                }
            })
            .collect();

        let result = run_challenger_duty_local(
            &store,
            &manifest,
            &providers,
            &pdp_responses,
            [99u8; 32],
            None, // free CID
            7,
        );

        assert_eq!(result.pdp_results.passed_count(), 2);
        assert!(!result.health.needs_healing);
        assert!(result.healing.is_none());

        std::fs::remove_dir_all(&dir).ok();
    }
}
