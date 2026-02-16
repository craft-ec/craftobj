//! CID Health Assessment and Self-Healing (RLNC model)
//!
//! Health = matrix rank per segment / k.
//! Self-healing creates new pieces via RLNC recombination.

use datacraft_core::{ContentId, ContentManifest, StorageReceipt};
use datacraft_store::FsStore;
use libp2p::PeerId;
use tracing::info;

#[cfg(test)]
use crate::pdp::create_storage_receipt;

// ---------------------------------------------------------------------------
// Tier info
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TierInfo {
    /// Minimum piece ratio (rank / k). E.g. 2.0 for Lite.
    pub min_piece_ratio: f64,
}

// ---------------------------------------------------------------------------
// CID Health
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CidHealth {
    pub content_id: ContentId,
    /// k per full segment.
    pub k: usize,
    /// Number of linearly independent pieces (rank of coefficient matrix).
    pub rank: usize,
    /// Total providers queried.
    pub total_providers: usize,
    /// Ratio of rank to k.
    pub health_ratio: f64,
    pub tier_minimum: Option<f64>,
    pub needs_healing: bool,
    pub pieces_needed: usize,
}

/// Assess health of a CID given rank of coefficient matrix.
pub fn assess_health(
    content_id: ContentId,
    k: usize,
    rank: usize,
    total_providers: usize,
    tier_info: Option<&TierInfo>,
) -> CidHealth {
    let health_ratio = if k > 0 {
        rank as f64 / k as f64
    } else {
        0.0
    };

    let tier_minimum = tier_info.map(|t| t.min_piece_ratio);

    let (needs_healing, pieces_needed) = match tier_info {
        Some(ti) => {
            let required = (ti.min_piece_ratio * k as f64).ceil() as usize;
            if rank < required {
                (true, required - rank)
            } else {
                (false, 0)
            }
        }
        None => (false, 0),
    };

    CidHealth {
        content_id,
        k,
        rank,
        total_providers,
        health_ratio,
        tier_minimum,
        needs_healing,
        pieces_needed,
    }
}

/// Compute the true rank per segment from PDP results using coefficient vector independence.
///
/// Groups passed results by segment_index, collects their coefficient vectors,
/// and calls `craftec_erasure::check_independence()` to get the matrix rank per segment.
pub fn compute_network_rank(results: &[ProviderPdpResult]) -> std::collections::HashMap<u32, usize> {
    let mut by_segment: std::collections::HashMap<u32, Vec<Vec<u8>>> = std::collections::HashMap::new();
    for r in results {
        if r.passed && !r.coefficients.is_empty() {
            by_segment.entry(r.segment_index).or_default().push(r.coefficients.clone());
        }
    }
    by_segment.into_iter().map(|(seg, vecs)| {
        let rank = craftec_erasure::check_independence(&vecs);
        (seg, rank)
    }).collect()
}

/// Compute rank per segment from a full inventory of coefficient vectors.
/// This uses ALL vectors from all providers (not just PDP challenge results).
pub fn compute_rank_from_inventory(
    inventory: &std::collections::HashMap<u32, Vec<Vec<u8>>>,
) -> std::collections::HashMap<u32, usize> {
    inventory
        .iter()
        .map(|(&seg, vecs)| {
            let rank = craftec_erasure::check_independence(vecs);
            (seg, rank)
        })
        .collect()
}

/// Return the minimum rank across all segments (health is only as good as the weakest).
/// If no segments have data, returns 0.
pub fn min_rank_across_segments(rank_map: &std::collections::HashMap<u32, usize>) -> usize {
    rank_map.values().copied().min().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// PDP results
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ProviderPdpResult {
    pub peer_id: PeerId,
    pub segment_index: u32,
    pub piece_id: [u8; 32],
    /// Coefficient vector for this piece (used to compute true rank).
    pub coefficients: Vec<u8>,
    pub passed: bool,
    pub receipt: Option<StorageReceipt>,
}

#[derive(Debug, Clone)]
pub struct PdpRoundResult {
    pub content_id: ContentId,
    pub results: Vec<ProviderPdpResult>,
}

impl PdpRoundResult {
    pub fn passed_count(&self) -> usize {
        self.results.iter().filter(|r| r.passed).count()
    }

    pub fn failed_count(&self) -> usize {
        self.results.iter().filter(|r| !r.passed).count()
    }

    pub fn receipts(&self) -> Vec<StorageReceipt> {
        self.results
            .iter()
            .filter_map(|r| r.receipt.clone())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Healing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct HealingResult {
    pub pieces_generated: usize,
    pub errors: Vec<String>,
}

/// Generate new pieces via RLNC recombination to heal a CID.
pub fn heal_content(
    store: &FsStore,
    manifest: &ContentManifest,
    pieces_needed: usize,
) -> HealingResult {
    let mut generated = 0;
    let mut errors = Vec::new();

    for seg_idx in 0..manifest.segment_count as u32 {
        let piece_ids = match store.list_pieces(&manifest.content_id, seg_idx) {
            Ok(ids) => ids,
            Err(e) => {
                errors.push(format!("segment {}: {}", seg_idx, e));
                continue;
            }
        };

        if piece_ids.len() < 2 {
            errors.push(format!("segment {}: need ≥2 pieces to recombine, have {}", seg_idx, piece_ids.len()));
            continue;
        }

        // Collect existing pieces for recombination
        let mut existing_pieces = Vec::new();
        for pid in &piece_ids {
            if let Ok((data, coeff)) = store.get_piece(&manifest.content_id, seg_idx, pid) {
                existing_pieces.push(craftec_erasure::CodedPiece { data, coefficients: coeff });
            }
        }

        if existing_pieces.len() < 2 {
            continue;
        }

        // Generate new pieces via recombination
        let to_gen = pieces_needed.saturating_sub(generated);
        for _ in 0..to_gen {
            match craftec_erasure::create_piece_from_existing(&existing_pieces) {
                Ok(new_piece) => {
                    let new_pid = datacraft_store::piece_id_from_coefficients(&new_piece.coefficients);
                    match store.store_piece(
                        &manifest.content_id, seg_idx, &new_pid,
                        &new_piece.data, &new_piece.coefficients,
                    ) {
                        Ok(()) => {
                            info!("Generated healing piece for {}/seg{}", manifest.content_id, seg_idx);
                            generated += 1;
                        }
                        Err(e) => {
                            errors.push(format!("store failed: {}", e));
                        }
                    }
                }
                Err(e) => {
                    errors.push(format!("recombination failed: {}", e));
                    break;
                }
            }
        }

        if generated >= pieces_needed {
            break;
        }
    }

    HealingResult {
        pieces_generated: generated,
        errors,
    }
}

// ---------------------------------------------------------------------------
// Provider info
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ProviderInfo {
    pub peer_id: PeerId,
    pub segment_index: u32,
    pub piece_id: [u8; 32],
}

// ---------------------------------------------------------------------------
// Duty cycle
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DutyCycleResult {
    pub pdp_results: PdpRoundResult,
    pub health: CidHealth,
    pub healing: Option<HealingResult>,
    pub challenger_receipt: Option<StorageReceipt>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assess_health_funded_healthy() {
        let cid = ContentId([1u8; 32]);
        let tier = TierInfo { min_piece_ratio: 2.0 };
        let health = assess_health(cid, 4, 10, 12, Some(&tier));
        assert!(!health.needs_healing);
        assert_eq!(health.pieces_needed, 0);
    }

    #[test]
    fn test_assess_health_funded_needs_healing() {
        let cid = ContentId([2u8; 32]);
        let tier = TierInfo { min_piece_ratio: 3.0 };
        let health = assess_health(cid, 4, 8, 10, Some(&tier));
        assert!(health.needs_healing);
        assert_eq!(health.pieces_needed, 4);
    }

    #[test]
    fn test_assess_health_free_cid() {
        let cid = ContentId([4u8; 32]);
        let health = assess_health(cid, 4, 2, 5, None);
        assert!(!health.needs_healing);
    }

    #[test]
    fn test_dependent_vectors_give_rank_1() {
        // Two linearly dependent coefficient vectors should give rank 1, not 2
        let results = vec![
            ProviderPdpResult {
                peer_id: PeerId::random(),
                segment_index: 0,
                piece_id: [1u8; 32],
                coefficients: vec![1, 0, 0],
                passed: true,
                receipt: None,
            },
            ProviderPdpResult {
                peer_id: PeerId::random(),
                segment_index: 0,
                piece_id: [2u8; 32],
                // [2, 0, 0] is a scalar multiple of [1, 0, 0] — linearly dependent
                coefficients: vec![2, 0, 0],
                passed: true,
                receipt: None,
            },
        ];
        let ranks = compute_network_rank(&results);
        assert_eq!(*ranks.get(&0).unwrap(), 1);
    }

    #[test]
    fn test_independent_vectors_give_rank_2() {
        let results = vec![
            ProviderPdpResult {
                peer_id: PeerId::random(),
                segment_index: 0,
                piece_id: [1u8; 32],
                coefficients: vec![1, 0, 0],
                passed: true,
                receipt: None,
            },
            ProviderPdpResult {
                peer_id: PeerId::random(),
                segment_index: 0,
                piece_id: [2u8; 32],
                coefficients: vec![0, 1, 0],
                passed: true,
                receipt: None,
            },
        ];
        let ranks = compute_network_rank(&results);
        assert_eq!(*ranks.get(&0).unwrap(), 2);
    }

    #[test]
    fn test_multi_segment_min_rank() {
        let results = vec![
            // Segment 0: 2 independent vectors → rank 2
            ProviderPdpResult {
                peer_id: PeerId::random(), segment_index: 0,
                piece_id: [1u8; 32], coefficients: vec![1, 0, 0],
                passed: true, receipt: None,
            },
            ProviderPdpResult {
                peer_id: PeerId::random(), segment_index: 0,
                piece_id: [2u8; 32], coefficients: vec![0, 1, 0],
                passed: true, receipt: None,
            },
            // Segment 1: 1 vector → rank 1 (weakest)
            ProviderPdpResult {
                peer_id: PeerId::random(), segment_index: 1,
                piece_id: [3u8; 32], coefficients: vec![1, 0, 0],
                passed: true, receipt: None,
            },
        ];
        let ranks = compute_network_rank(&results);
        assert_eq!(*ranks.get(&0).unwrap(), 2);
        assert_eq!(*ranks.get(&1).unwrap(), 1);
        assert_eq!(min_rank_across_segments(&ranks), 1);
    }

    #[test]
    fn test_healing_triggers_when_rank_below_tier() {
        let cid = ContentId([99u8; 32]);
        let tier = TierInfo { min_piece_ratio: 2.0 };
        // k=3, rank=2 → need 6, have 2 → healing needed
        let health = assess_health(cid, 3, 2, 5, Some(&tier));
        assert!(health.needs_healing);
        assert_eq!(health.pieces_needed, 4); // ceil(2.0 * 3) - 2 = 4
    }

    #[test]
    fn test_pdp_round_result_counts() {
        let piece_id = [5u8; 32];
        let results = PdpRoundResult {
            content_id: ContentId([0u8; 32]),
            results: vec![
                ProviderPdpResult {
                    peer_id: PeerId::random(),
                    segment_index: 0,
                    piece_id,
                    coefficients: vec![1, 0, 0],
                    passed: true,
                    receipt: Some(create_storage_receipt(
                        ContentId([0u8; 32]), [1u8; 32], [2u8; 32], 0, piece_id, [0u8; 32], [0u8; 32],
                    )),
                },
                ProviderPdpResult {
                    peer_id: PeerId::random(),
                    segment_index: 0,
                    piece_id: [6u8; 32],
                    coefficients: vec![],
                    passed: false,
                    receipt: None,
                },
            ],
        };
        assert_eq!(results.passed_count(), 1);
        assert_eq!(results.failed_count(), 1);
        assert_eq!(results.receipts().len(), 1);
    }
}
