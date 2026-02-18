//! Settlement cycle: collects receipts → proves batch → submits to Solana.
//!
//! Runs periodically (e.g., every N PDP rounds or on a timer) to:
//! 1. Drain unsettled receipts from the receipt store
//! 2. Generate a batch proof via the prover (mock or SP1)
//! 3. Submit the proof to the settlement program (dry-run or RPC)

use crate::settlement::SolanaClient;
use craftec_prover::{BatchProof, ProverClient, ProverError};
use craftobj_core::StorageReceipt;
use tracing::{debug, info};

/// Configuration for the settlement cycle.
#[derive(Debug, Clone)]
pub struct SettlementCycleConfig {
    /// Minimum number of receipts before triggering a batch.
    pub min_batch_size: usize,
    /// Pool ID to submit proofs against.
    pub pool_id: [u8; 32],
}

impl Default for SettlementCycleConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 1,
            pool_id: [0u8; 32],
        }
    }
}

/// Result of a single settlement cycle run.
#[derive(Debug)]
pub struct CycleResult {
    /// Number of receipts processed.
    pub receipts_processed: usize,
    /// The batch proof generated (if any).
    pub batch_proof: Option<BatchProof>,
    /// Transaction signature (if submitted).
    pub tx_signature: Option<String>,
}

/// Run one settlement cycle: collect receipts, prove, submit.
///
/// Takes ownership of receipts from the store's snapshot. In production,
/// you'd track which receipts have been settled (e.g., with a watermark).
/// For now, this processes all receipts in the store.
pub async fn run_settlement_cycle(
    receipts: &[StorageReceipt],
    prover: &ProverClient,
    settlement_client: &SolanaClient,
    config: &SettlementCycleConfig,
) -> Result<CycleResult, SettlementCycleError> {
    if receipts.is_empty() {
        debug!("Settlement cycle: no receipts to process");
        return Ok(CycleResult {
            receipts_processed: 0,
            batch_proof: None,
            tx_signature: None,
        });
    }

    if receipts.len() < config.min_batch_size {
        debug!(
            "Settlement cycle: {} receipts < min_batch_size {}",
            receipts.len(),
            config.min_batch_size
        );
        return Ok(CycleResult {
            receipts_processed: 0,
            batch_proof: None,
            tx_signature: None,
        });
    }

    info!(
        "Settlement cycle: proving batch of {} receipts for pool={}",
        receipts.len(),
        hex::encode(&config.pool_id[..8])
    );

    // Step 1: Generate batch proof
    let batch_proof = prover
        .prove_batch(receipts, &config.pool_id)
        .map_err(SettlementCycleError::Prover)?;

    info!(
        "Settlement cycle: proof generated, {} bytes proof, {} bytes public inputs",
        batch_proof.proof_bytes.len(),
        batch_proof.public_inputs.len()
    );

    // Step 2: Submit to settlement program
    let tx_result = settlement_client
        .submit_distribution_proof(&config.pool_id, &batch_proof)
        .await
        .map_err(|e| SettlementCycleError::Settlement(e.to_string()))?;

    info!(
        "Settlement cycle: submitted tx={} confirmed={}",
        tx_result.signature, tx_result.confirmed
    );

    Ok(CycleResult {
        receipts_processed: receipts.len(),
        batch_proof: Some(batch_proof),
        tx_signature: Some(tx_result.signature),
    })
}

/// Errors from the settlement cycle.
#[derive(Debug, thiserror::Error)]
pub enum SettlementCycleError {
    #[error("prover error: {0}")]
    Prover(#[from] ProverError),
    #[error("settlement error: {0}")]
    Settlement(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settlement::SettlementConfig;
    use craftobj_core::ContentId;

    fn make_receipt(node: [u8; 32], challenger: [u8; 32], ts: u64) -> StorageReceipt {
        StorageReceipt {
            content_id: ContentId([0u8; 32]),
            storage_node: node,
            challenger,
            segment_index: 0,
            piece_id: [5u8; 32],
            timestamp: ts,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![0u8; 64],
        }
    }

    #[test]
    fn test_contribution_receipt_impl() {
        use craftec_prover::compute_distribution_entries;

        let receipt = make_receipt([1u8; 32], [2u8; 32], 1000);
        // Verify StorageReceipt works with the prover (which requires ContributionReceipt)
        let entries = compute_distribution_entries(&[receipt]);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, [1u8; 32]); // operator
        assert_eq!(entries[0].1, 1); // weight
    }

    #[tokio::test]
    async fn test_settlement_cycle_empty() {
        let prover = ProverClient::new();
        let config = SettlementCycleConfig::default();
        let settlement = SolanaClient::new(SettlementConfig {
            dry_run: true,
            signing_key: Some([42u8; 32]),
            ..Default::default()
        })
        .unwrap();

        let result = run_settlement_cycle(&[], &prover, &settlement, &config)
            .await
            .unwrap();
        assert_eq!(result.receipts_processed, 0);
        assert!(result.batch_proof.is_none());
    }

    #[tokio::test]
    async fn test_settlement_cycle_below_min_batch() {
        let prover = ProverClient::new();
        let config = SettlementCycleConfig {
            min_batch_size: 10,
            pool_id: [1u8; 32],
        };
        let settlement = SolanaClient::new(SettlementConfig {
            dry_run: true,
            signing_key: Some([42u8; 32]),
            ..Default::default()
        })
        .unwrap();

        let receipts = vec![make_receipt([1u8; 32], [2u8; 32], 1000)];
        let result = run_settlement_cycle(&receipts, &prover, &settlement, &config)
            .await
            .unwrap();
        assert_eq!(result.receipts_processed, 0);
    }

    #[tokio::test]
    async fn test_settlement_cycle_full_pipeline() {
        let prover = ProverClient::new();
        let pool_id = [99u8; 32];
        let config = SettlementCycleConfig {
            min_batch_size: 1,
            pool_id,
        };
        let settlement = SolanaClient::new(SettlementConfig {
            dry_run: true,
            signing_key: Some([42u8; 32]),
            ..Default::default()
        })
        .unwrap();

        let receipts = vec![
            make_receipt([1u8; 32], [2u8; 32], 1000),
            make_receipt([1u8; 32], [2u8; 32], 1001),
            make_receipt([3u8; 32], [2u8; 32], 1002),
        ];

        let result = run_settlement_cycle(&receipts, &prover, &settlement, &config)
            .await
            .unwrap();

        assert_eq!(result.receipts_processed, 3);
        assert!(result.batch_proof.is_some());
        assert!(result.tx_signature.is_some());

        let sig = result.tx_signature.unwrap();
        assert!(sig.starts_with("dry-run:"), "expected dry-run sig, got: {}", sig);

        // Verify the proof is valid
        let proof = result.batch_proof.unwrap();
        let output = prover.verify_batch(&proof).unwrap();
        assert_eq!(output.pool_id, pool_id);
        assert_eq!(output.entry_count, 2); // 2 unique operators
        assert_eq!(output.total_weight, 3); // 3 receipts, weight 1 each
    }

    #[tokio::test]
    async fn test_settlement_cycle_multiple_operators() {
        let prover = ProverClient::new();
        let pool_id = [50u8; 32];
        let config = SettlementCycleConfig {
            min_batch_size: 1,
            pool_id,
        };
        let settlement = SolanaClient::new(SettlementConfig {
            dry_run: true,
            signing_key: Some([42u8; 32]),
            ..Default::default()
        })
        .unwrap();

        // 5 receipts across 3 operators
        let receipts = vec![
            make_receipt([10u8; 32], [2u8; 32], 100),
            make_receipt([10u8; 32], [2u8; 32], 101),
            make_receipt([20u8; 32], [2u8; 32], 102),
            make_receipt([20u8; 32], [2u8; 32], 103),
            make_receipt([30u8; 32], [2u8; 32], 104),
        ];

        let result = run_settlement_cycle(&receipts, &prover, &settlement, &config)
            .await
            .unwrap();

        let proof = result.batch_proof.unwrap();
        let output = prover.verify_batch(&proof).unwrap();
        assert_eq!(output.entry_count, 3);
        assert_eq!(output.total_weight, 5);
    }

    #[test]
    fn test_prover_mock_prove_and_verify() {
        let prover = ProverClient::new();
        let pool_id = [1u8; 32];

        let receipts = vec![
            make_receipt([1u8; 32], [2u8; 32], 1000),
            make_receipt([3u8; 32], [4u8; 32], 2000),
        ];

        let proof = prover.prove_batch(&receipts[..], &pool_id).unwrap();
        let output = prover.verify_batch(&proof).unwrap();

        assert_eq!(output.pool_id, pool_id);
        assert_eq!(output.entry_count, 2);
        assert_eq!(output.total_weight, 2);
    }

    #[tokio::test]
    async fn test_submit_distribution_proof_dry_run() {
        let prover = ProverClient::new();
        let pool_id = [7u8; 32];
        let settlement = SolanaClient::new(SettlementConfig {
            dry_run: true,
            signing_key: Some([42u8; 32]),
            ..Default::default()
        })
        .unwrap();

        let receipts = vec![make_receipt([1u8; 32], [2u8; 32], 1000)];
        let proof = prover.prove_batch(&receipts[..], &pool_id).unwrap();

        let result = settlement
            .submit_distribution_proof(&pool_id, &proof)
            .await
            .unwrap();
        assert!(result.confirmed);
        assert!(result.signature.starts_with("dry-run:"));
    }
}
