//! ZK Prover Aggregator
//!
//! Periodically collects StorageReceipts from the PersistentReceiptStore,
//! batches them, and generates a proof via `craftec-prover` (mock mode).
//! The result is an `AggregatorResult` containing the batch proof,
//! receipt count, and Merkle root — ready for settlement submission.

use std::sync::Arc;
use std::time::Duration;

use craftec_prover::{BatchProof, ProverClient, ProverError};
use craftec_prover_guest_types::DistributionOutput;
use craftobj_core::StorageReceipt;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::events::{DaemonEvent, EventSender};
use crate::receipt_store::PersistentReceiptStore;

/// Result of a single aggregation epoch.
#[derive(Debug, Clone)]
pub struct AggregatorResult {
    /// The batch proof (mock or real).
    pub batch_proof: BatchProof,
    /// Number of receipts included in this batch.
    pub receipt_count: usize,
    /// Merkle root of the distribution tree.
    pub merkle_root: [u8; 32],
    /// Decoded output from the prover.
    pub output: DistributionOutput,
}

/// Configuration for the aggregator.
#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// How often to run aggregation (default: 10 minutes).
    pub epoch_duration: Duration,
    /// Pool ID for the distribution proof.
    pub pool_id: [u8; 32],
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            epoch_duration: Duration::from_secs(600), // 10 minutes
            pool_id: [0u8; 32],
        }
    }
}

/// The Aggregator collects receipts and generates batch proofs.
pub struct Aggregator {
    config: AggregatorConfig,
    prover: ProverClient,
}

impl Aggregator {
    pub fn new(config: AggregatorConfig) -> Self {
        Self {
            config,
            prover: ProverClient::new(),
        }
    }

    /// Collect all storage receipts currently in the store.
    ///
    /// Returns a snapshot of the receipts. In production, these would be
    /// drained/marked as consumed to avoid re-proving.
    pub fn collect_receipts(store: &PersistentReceiptStore) -> Vec<StorageReceipt> {
        store.all_storage_receipts().to_vec()
    }

    /// Build a batch proof from the given receipts.
    ///
    /// Returns `None` if the batch is empty, `Err` on prover failure.
    pub fn build_batch_proof(
        &self,
        receipts: &[StorageReceipt],
    ) -> Result<Option<AggregatorResult>, ProverError> {
        if receipts.is_empty() {
            return Ok(None);
        }

        let proof = self
            .prover
            .prove_batch(receipts, &self.config.pool_id)?;

        let output = self.prover.verify_batch(&proof)?;

        Ok(Some(AggregatorResult {
            batch_proof: proof,
            receipt_count: receipts.len(),
            merkle_root: output.root,
            output,
        }))
    }

    /// Get the configured epoch duration.
    pub fn epoch_duration(&self) -> Duration {
        self.config.epoch_duration
    }

    /// Get the pool ID.
    pub fn pool_id(&self) -> &[u8; 32] {
        &self.config.pool_id
    }
}

/// Run the aggregation loop as a background task.
///
/// Periodically collects receipts, builds a proof, and emits events.
pub async fn run_aggregation_loop(
    receipt_store: Arc<Mutex<PersistentReceiptStore>>,
    event_tx: EventSender,
    config: AggregatorConfig,
) {
    let aggregator = Aggregator::new(config);

    // Initial delay before first aggregation
    tokio::time::sleep(Duration::from_secs(30)).await;

    let mut interval = tokio::time::interval(aggregator.epoch_duration());
    loop {
        interval.tick().await;

        let receipts = {
            let store = receipt_store.lock().await;
            Aggregator::collect_receipts(&store)
        };

        if receipts.is_empty() {
            debug!("Aggregation epoch: no receipts to process");
            continue;
        }

        info!(
            "Aggregation epoch: processing {} receipts",
            receipts.len()
        );

        match aggregator.build_batch_proof(&receipts) {
            Ok(Some(result)) => {
                info!(
                    "Aggregation complete: {} receipts, {} operators, root={}",
                    result.receipt_count,
                    result.output.entry_count,
                    hex::encode(result.merkle_root),
                );
                let _ = event_tx.send(DaemonEvent::AggregationComplete {
                    receipt_count: result.receipt_count,
                    merkle_root: hex::encode(result.merkle_root),
                });
            }
            Ok(None) => {
                debug!("Aggregation epoch: empty batch after collection");
            }
            Err(e) => {
                warn!("Aggregation failed: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craftobj_core::ContentId;

    fn make_test_receipt(node_byte: u8, ts: u64) -> StorageReceipt {
        let mut storage_node = [0u8; 32];
        storage_node[0] = node_byte;
        StorageReceipt {
            content_id: ContentId([0xAA; 32]),
            storage_node,
            challenger: [0xFF; 32],
            segment_index: 0,
            piece_id: [ts as u8; 32],
            timestamp: ts,
            nonce: [0; 32],
            proof_hash: [0; 32],
            signature: vec![],
        }
    }

    #[test]
    fn test_build_batch_proof_success() {
        let config = AggregatorConfig::default();
        let aggregator = Aggregator::new(config);

        let receipts: Vec<StorageReceipt> = (1..=5)
            .map(|i| make_test_receipt(i, 1000 + i as u64))
            .collect();

        let result = aggregator.build_batch_proof(&receipts).unwrap().unwrap();
        assert_eq!(result.receipt_count, 5);
        assert_ne!(result.merkle_root, [0u8; 32]);
        assert_eq!(result.output.entry_count, 5); // 5 unique operators
        assert!(!result.batch_proof.proof_bytes.is_empty());
    }

    #[test]
    fn test_build_batch_proof_empty() {
        let aggregator = Aggregator::new(AggregatorConfig::default());
        let result = aggregator.build_batch_proof(&[]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_build_batch_proof_aggregates_same_operator() {
        let aggregator = Aggregator::new(AggregatorConfig::default());

        // Same operator (node_byte=1) appears 3 times
        let receipts = vec![
            make_test_receipt(1, 1000),
            make_test_receipt(1, 1001),
            make_test_receipt(1, 1002),
        ];

        let result = aggregator.build_batch_proof(&receipts).unwrap().unwrap();
        assert_eq!(result.receipt_count, 3);
        assert_eq!(result.output.entry_count, 1); // 1 unique operator
        assert_eq!(result.output.total_weight, 3); // weight=1 each
    }

    #[test]
    fn test_aggregator_config_defaults() {
        let config = AggregatorConfig::default();
        assert_eq!(config.epoch_duration, Duration::from_secs(600));
        assert_eq!(config.pool_id, [0u8; 32]);
    }

    #[test]
    fn test_collect_receipts_from_store() {
        let path = std::env::temp_dir().join(format!(
            "agg-test-{}.bin",
            std::process::id()
        ));
        let mut store = PersistentReceiptStore::new(path.clone()).unwrap();

        store.add_storage(make_test_receipt(1, 100)).unwrap();
        store.add_storage(make_test_receipt(2, 200)).unwrap();

        let collected = Aggregator::collect_receipts(&store);
        assert_eq!(collected.len(), 2);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_proof_is_verifiable() {
        let aggregator = Aggregator::new(AggregatorConfig {
            pool_id: [0xBB; 32],
            ..Default::default()
        });

        let receipts: Vec<StorageReceipt> = (1..=3)
            .map(|i| make_test_receipt(i, 2000 + i as u64))
            .collect();

        let result = aggregator.build_batch_proof(&receipts).unwrap().unwrap();

        // Verify the proof independently
        let prover = ProverClient::new();
        let output = prover.verify_batch(&result.batch_proof).unwrap();
        assert_eq!(output.pool_id, [0xBB; 32]);
        assert_eq!(output.root, result.merkle_root);
    }
}
