//! Economics: Pool types, tier logic, distribution math, and eviction policy.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{ContentId, StorageReceipt, TransferReceipt};

// ---------------------------------------------------------------------------
// Tiers
// ---------------------------------------------------------------------------

/// Subscription / funding tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Tier {
    Free,
    Lite,
    Standard,
    Pro,
    Enterprise,
}

impl Tier {
    /// Minimum shard health ratio (`live_shards / k`) guaranteed by the tier.
    /// Free tier has no guarantee.
    pub fn min_shard_ratio(&self) -> Option<f64> {
        match self {
            Tier::Free => None,
            Tier::Lite => Some(2.0),
            Tier::Standard => Some(3.0),
            Tier::Pro => Some(5.0),
            Tier::Enterprise => Some(10.0),
        }
    }

    /// Storage quota in bytes.
    pub fn storage_quota(&self) -> u64 {
        match self {
            Tier::Free => 0,
            Tier::Lite => 5 * GB,
            Tier::Standard => 50 * GB,
            Tier::Pro => 500 * GB,
            Tier::Enterprise => 5 * TB,
        }
    }

    /// Bandwidth quota in bytes per epoch (month).
    pub fn bandwidth_quota(&self) -> u64 {
        match self {
            Tier::Free => 0, // best-effort
            Tier::Lite => 50 * GB,
            Tier::Standard => 500 * GB,
            Tier::Pro => 5 * TB,
            Tier::Enterprise => 50 * TB,
        }
    }

    /// Cost per epoch in USDC lamports (1 USDC = 1_000_000 lamports).
    pub fn epoch_cost_usdc(&self) -> u64 {
        match self {
            Tier::Free => 0,
            Tier::Lite => 2 * USDC,
            Tier::Standard => 10 * USDC,
            Tier::Pro => 50 * USDC,
            Tier::Enterprise => 250 * USDC,
        }
    }
}

const GB: u64 = 1_000_000_000;
const TB: u64 = 1_000_000_000_000;
/// 1 USDC in lamports (6 decimals).
const USDC: u64 = 1_000_000;

// ---------------------------------------------------------------------------
// Pool types
// ---------------------------------------------------------------------------

/// Protocol-wide default ratio constants.
pub const DEFAULT_STORAGE_RATIO: f64 = 0.6;
pub const DEFAULT_TRANSFER_RATIO: f64 = 0.4;

/// Per-CID shared pool — no expiry, fluid settlement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentPool {
    pub content_id: ContentId,
    pub tier: Tier,
    /// USDC lamports remaining in the pool.
    pub balance: u64,
    /// Cumulative USDC lamports claimed from this pool.
    pub total_claimed: u64,
    /// Fraction of pool allocated to storage receipts.
    pub storage_ratio: f64,
    /// Fraction of pool allocated to transfer receipts.
    pub transfer_ratio: f64,
}

impl ContentPool {
    pub fn new(content_id: ContentId, tier: Tier, balance: u64) -> Self {
        Self {
            content_id,
            tier,
            balance,
            total_claimed: 0,
            storage_ratio: DEFAULT_STORAGE_RATIO,
            transfer_ratio: DEFAULT_TRANSFER_RATIO,
        }
    }

    /// Whether the pool is funded (has remaining balance).
    pub fn is_funded(&self) -> bool {
        self.balance > 0
    }
}

/// Per-user subscription pool — time-bound.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionPool {
    pub subscriber: [u8; 32],
    pub tier: Tier,
    /// USDC lamports remaining.
    pub balance: u64,
    /// Storage quota in bytes.
    pub storage_quota: u64,
    /// Bandwidth quota in bytes per epoch.
    pub bandwidth_quota: u64,
    /// Storage used in bytes.
    pub storage_used: u64,
    /// Bandwidth used in bytes (this epoch).
    pub bandwidth_used: u64,
    /// Unix timestamp when the subscription started.
    pub start_date: u64,
    /// Unix timestamp when the subscription expires.
    pub expires_at: u64,
    /// Fraction allocated to storage receipts.
    pub storage_ratio: f64,
    /// Fraction allocated to transfer receipts.
    pub transfer_ratio: f64,
}

impl SubscriptionPool {
    pub fn new(subscriber: [u8; 32], tier: Tier, balance: u64, start_date: u64, duration_secs: u64) -> Self {
        Self {
            subscriber,
            tier,
            balance,
            storage_quota: tier.storage_quota(),
            bandwidth_quota: tier.bandwidth_quota(),
            storage_used: 0,
            bandwidth_used: 0,
            start_date,
            expires_at: start_date + duration_secs,
            storage_ratio: DEFAULT_STORAGE_RATIO,
            transfer_ratio: DEFAULT_TRANSFER_RATIO,
        }
    }

    /// Whether the subscription is active at `now` (unix timestamp).
    pub fn is_active(&self, now: u64) -> bool {
        now >= self.start_date && now < self.expires_at && self.balance > 0
    }

    /// Whether the pool is funded.
    pub fn is_funded(&self) -> bool {
        self.balance > 0
    }
}

// ---------------------------------------------------------------------------
// Pool distribution
// ---------------------------------------------------------------------------

/// Public key type alias (32-byte ed25519 pubkey).
pub type PublicKey = [u8; 32];

/// Distribution of receipts across storage and transfer buckets.
#[derive(Debug, Clone, Default)]
pub struct PoolDistribution {
    /// (node pubkey, receipt count) from StorageReceipts.
    pub storage_bucket: Vec<(PublicKey, u64)>,
    /// (node pubkey, total bytes served) from TransferReceipts.
    pub transfer_bucket: Vec<(PublicKey, u64)>,
}

/// Compute distribution from receipts.
///
/// Aggregates StorageReceipts by `storage_node` (count) and
/// TransferReceipts by `server_node` (sum of bytes_served).
pub fn compute_distribution(
    storage_receipts: &[StorageReceipt],
    transfer_receipts: &[TransferReceipt],
) -> PoolDistribution {
    let mut storage_map: HashMap<PublicKey, u64> = HashMap::new();
    for r in storage_receipts {
        *storage_map.entry(r.storage_node).or_default() += 1;
    }

    let mut transfer_map: HashMap<PublicKey, u64> = HashMap::new();
    for r in transfer_receipts {
        *transfer_map.entry(r.server_node).or_default() += r.bytes_served;
    }

    PoolDistribution {
        storage_bucket: storage_map.into_iter().collect(),
        transfer_bucket: transfer_map.into_iter().collect(),
    }
}

/// Compute the USDC lamports claimable by `node` from a pool.
///
/// Storage bucket: `pool_balance * storage_ratio * (node_receipts / total_receipts)`
/// Transfer bucket: `pool_balance * transfer_ratio * (node_bytes / total_bytes)`
pub fn compute_claim(
    node: &PublicKey,
    distribution: &PoolDistribution,
    pool_balance: u64,
    storage_ratio: f64,
    transfer_ratio: f64,
) -> u64 {
    let storage_share = {
        let total: u64 = distribution.storage_bucket.iter().map(|(_, c)| c).sum();
        if total == 0 {
            0
        } else {
            let node_count = distribution
                .storage_bucket
                .iter()
                .find(|(k, _)| k == node)
                .map(|(_, c)| *c)
                .unwrap_or(0);
            ((pool_balance as f64) * storage_ratio * (node_count as f64 / total as f64)) as u64
        }
    };

    let transfer_share = {
        let total: u64 = distribution.transfer_bucket.iter().map(|(_, b)| b).sum();
        if total == 0 {
            0
        } else {
            let node_bytes = distribution
                .transfer_bucket
                .iter()
                .find(|(k, _)| k == node)
                .map(|(_, b)| *b)
                .unwrap_or(0);
            ((pool_balance as f64) * transfer_ratio * (node_bytes as f64 / total as f64)) as u64
        }
    };

    storage_share + transfer_share
}

// ---------------------------------------------------------------------------
// Double-pool detection
// ---------------------------------------------------------------------------

/// Result of double-pool detection for a transfer event.
#[derive(Debug, Clone)]
pub struct DoublePoolClaim {
    /// Amount claimable from the content pool (storage bucket).
    pub content_pool_amount: u64,
    /// Amount claimable from the subscription pool (transfer bucket).
    pub subscription_pool_amount: u64,
}

/// Check if a TransferReceipt qualifies for double-pool earning.
///
/// Returns amounts claimable from each pool if both conditions are met:
/// 1. The content_id has a funded ContentPool
/// 2. The requester has an active SubscriptionPool
pub fn detect_double_pool(
    receipt: &TransferReceipt,
    content_pool: Option<&ContentPool>,
    subscription_pool: Option<&SubscriptionPool>,
    content_distribution: Option<&PoolDistribution>,
    sub_distribution: Option<&PoolDistribution>,
    now: u64,
) -> Option<DoublePoolClaim> {
    let cp = content_pool.filter(|p| p.is_funded() && p.content_id == receipt.content_id)?;
    let sp = subscription_pool.filter(|p| p.is_active(now) && p.subscriber == receipt.requester)?;

    let content_amount = content_distribution
        .map(|d| compute_claim(&receipt.server_node, d, cp.balance, cp.storage_ratio, cp.transfer_ratio))
        .unwrap_or(0);

    let sub_amount = sub_distribution
        .map(|d| compute_claim(&receipt.server_node, d, sp.balance, sp.storage_ratio, sp.transfer_ratio))
        .unwrap_or(0);

    Some(DoublePoolClaim {
        content_pool_amount: content_amount,
        subscription_pool_amount: sub_amount,
    })
}

// ---------------------------------------------------------------------------
// Eviction
// ---------------------------------------------------------------------------

/// Eviction strategy for unfunded CIDs under storage pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Least recently accessed.
    LRU,
    /// Lowest fetch count.
    LeastFetched,
    /// Oldest content by storage time.
    Oldest,
}

/// Metadata about a locally stored CID, used for eviction decisions.
#[derive(Debug, Clone)]
pub struct StoredCid {
    pub content_id: ContentId,
    /// Whether this CID has a funded content pool.
    pub is_funded: bool,
    /// Last access timestamp (unix seconds).
    pub last_accessed: u64,
    /// Total number of fetches served.
    pub fetch_count: u64,
    /// When the CID was first stored locally (unix seconds).
    pub stored_at: u64,
}

/// Returns CIDs ordered by eviction priority (first = evict first).
///
/// Funded CIDs are **never** returned — they earn revenue and should be kept.
pub fn eviction_priority(stored_cids: &[StoredCid], policy: EvictionPolicy) -> Vec<ContentId> {
    let mut unfunded: Vec<&StoredCid> = stored_cids.iter().filter(|c| !c.is_funded).collect();

    match policy {
        EvictionPolicy::LRU => {
            unfunded.sort_by_key(|c| c.last_accessed); // oldest access first
        }
        EvictionPolicy::LeastFetched => {
            unfunded.sort_by_key(|c| c.fetch_count); // fewest fetches first
        }
        EvictionPolicy::Oldest => {
            unfunded.sort_by_key(|c| c.stored_at); // stored earliest first
        }
    }

    unfunded.iter().map(|c| c.content_id).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Tier tests --

    #[test]
    fn test_tier_shard_ratios() {
        assert_eq!(Tier::Free.min_shard_ratio(), None);
        assert_eq!(Tier::Lite.min_shard_ratio(), Some(2.0));
        assert_eq!(Tier::Standard.min_shard_ratio(), Some(3.0));
        assert_eq!(Tier::Pro.min_shard_ratio(), Some(5.0));
        assert_eq!(Tier::Enterprise.min_shard_ratio(), Some(10.0));
    }

    #[test]
    fn test_tier_storage_quotas() {
        assert_eq!(Tier::Free.storage_quota(), 0);
        assert_eq!(Tier::Lite.storage_quota(), 5 * GB);
        assert_eq!(Tier::Standard.storage_quota(), 50 * GB);
        assert_eq!(Tier::Pro.storage_quota(), 500 * GB);
        assert_eq!(Tier::Enterprise.storage_quota(), 5 * TB);
    }

    #[test]
    fn test_tier_bandwidth_quotas() {
        assert_eq!(Tier::Free.bandwidth_quota(), 0);
        assert_eq!(Tier::Lite.bandwidth_quota(), 50 * GB);
        assert_eq!(Tier::Standard.bandwidth_quota(), 500 * GB);
        assert_eq!(Tier::Pro.bandwidth_quota(), 5 * TB);
        assert_eq!(Tier::Enterprise.bandwidth_quota(), 50 * TB);
    }

    #[test]
    fn test_tier_epoch_costs() {
        assert_eq!(Tier::Free.epoch_cost_usdc(), 0);
        assert_eq!(Tier::Lite.epoch_cost_usdc(), 2 * USDC);
        assert_eq!(Tier::Standard.epoch_cost_usdc(), 10 * USDC);
        assert_eq!(Tier::Pro.epoch_cost_usdc(), 50 * USDC);
        assert_eq!(Tier::Enterprise.epoch_cost_usdc(), 250 * USDC);
    }

    #[test]
    fn test_tier_serde_roundtrip() {
        for tier in [Tier::Free, Tier::Lite, Tier::Standard, Tier::Pro, Tier::Enterprise] {
            let json = serde_json::to_string(&tier).unwrap();
            let parsed: Tier = serde_json::from_str(&json).unwrap();
            assert_eq!(tier, parsed);
        }
    }

    // -- ContentPool tests --

    #[test]
    fn test_content_pool_new() {
        let cid = ContentId([1u8; 32]);
        let pool = ContentPool::new(cid, Tier::Pro, 100 * USDC);
        assert!(pool.is_funded());
        assert_eq!(pool.total_claimed, 0);
        assert!((pool.storage_ratio - 0.6).abs() < f64::EPSILON);
        assert!((pool.transfer_ratio - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn test_content_pool_unfunded() {
        let pool = ContentPool::new(ContentId([0u8; 32]), Tier::Free, 0);
        assert!(!pool.is_funded());
    }

    // -- SubscriptionPool tests --

    #[test]
    fn test_subscription_pool_active() {
        let pool = SubscriptionPool::new([1u8; 32], Tier::Standard, 10 * USDC, 1000, 3600);
        assert!(pool.is_active(1000));
        assert!(pool.is_active(2000));
        assert!(!pool.is_active(4600)); // expired
        assert!(!pool.is_active(999)); // not started
    }

    #[test]
    fn test_subscription_pool_expired_balance() {
        let mut pool = SubscriptionPool::new([1u8; 32], Tier::Lite, 5 * USDC, 1000, 3600);
        pool.balance = 0;
        assert!(!pool.is_active(2000)); // has time but no balance
    }

    // -- Distribution tests --

    fn make_storage_receipt(node: [u8; 32], cid: ContentId) -> StorageReceipt {
        StorageReceipt {
            content_id: cid,
            storage_node: node,
            challenger: [0u8; 32],
            shard_index: 0,
            timestamp: 1000,
            nonce: [0u8; 32],
            proof_hash: [0u8; 32],
            signature: vec![],
        }
    }

    fn make_transfer_receipt(node: [u8; 32], requester: [u8; 32], cid: ContentId, bytes: u64) -> TransferReceipt {
        TransferReceipt {
            content_id: cid,
            server_node: node,
            requester,
            shard_index: 0,
            bytes_served: bytes,
            timestamp: 1000,
            signature: vec![],
        }
    }

    #[test]
    fn test_compute_distribution_empty() {
        let d = compute_distribution(&[], &[]);
        assert!(d.storage_bucket.is_empty());
        assert!(d.transfer_bucket.is_empty());
    }

    #[test]
    fn test_compute_distribution_aggregates() {
        let cid = ContentId([1u8; 32]);
        let node_a = [1u8; 32];
        let node_b = [2u8; 32];

        let storage = vec![
            make_storage_receipt(node_a, cid),
            make_storage_receipt(node_a, cid),
            make_storage_receipt(node_b, cid),
        ];
        let transfer = vec![
            make_transfer_receipt(node_a, [10u8; 32], cid, 1000),
            make_transfer_receipt(node_b, [10u8; 32], cid, 3000),
        ];

        let d = compute_distribution(&storage, &transfer);

        let sa: HashMap<_, _> = d.storage_bucket.into_iter().collect();
        assert_eq!(sa[&node_a], 2);
        assert_eq!(sa[&node_b], 1);

        let ta: HashMap<_, _> = d.transfer_bucket.into_iter().collect();
        assert_eq!(ta[&node_a], 1000);
        assert_eq!(ta[&node_b], 3000);
    }

    #[test]
    fn test_compute_claim_proportional() {
        let node_a = [1u8; 32];
        let node_b = [2u8; 32];
        let balance = 1_000_000u64; // 1 USDC

        let dist = PoolDistribution {
            storage_bucket: vec![(node_a, 3), (node_b, 1)],
            transfer_bucket: vec![(node_a, 1000), (node_b, 3000)],
        };

        let claim_a = compute_claim(&node_a, &dist, balance, 0.6, 0.4);
        let claim_b = compute_claim(&node_b, &dist, balance, 0.6, 0.4);

        // Storage: A gets 3/4 of 600k = 450k, B gets 1/4 of 600k = 150k
        // Transfer: A gets 1/4 of 400k = 100k, B gets 3/4 of 400k = 300k
        // A total: 550k, B total: 450k
        assert_eq!(claim_a, 550_000);
        assert_eq!(claim_b, 450_000);
        assert_eq!(claim_a + claim_b, balance);
    }

    #[test]
    fn test_compute_claim_unknown_node() {
        let dist = PoolDistribution {
            storage_bucket: vec![([1u8; 32], 10)],
            transfer_bucket: vec![([1u8; 32], 5000)],
        };
        let unknown = [99u8; 32];
        assert_eq!(compute_claim(&unknown, &dist, 1_000_000, 0.6, 0.4), 0);
    }

    #[test]
    fn test_compute_claim_storage_only() {
        let node = [1u8; 32];
        let dist = PoolDistribution {
            storage_bucket: vec![(node, 5)],
            transfer_bucket: vec![],
        };
        // All storage, no transfer receipts → gets storage share only
        let claim = compute_claim(&node, &dist, 1_000_000, 0.6, 0.4);
        assert_eq!(claim, 600_000); // 100% of storage bucket
    }

    // -- Double-pool tests --

    #[test]
    fn test_double_pool_both_funded() {
        let cid = ContentId([1u8; 32]);
        let server = [10u8; 32];
        let requester = [20u8; 32];

        let receipt = make_transfer_receipt(server, requester, cid, 5000);

        let cp = ContentPool::new(cid, Tier::Pro, 100 * USDC);
        let sp = SubscriptionPool::new(requester, Tier::Standard, 10 * USDC, 1000, 3600);

        let cd = PoolDistribution {
            storage_bucket: vec![(server, 5)],
            transfer_bucket: vec![(server, 5000)],
        };
        let sd = PoolDistribution {
            storage_bucket: vec![(server, 2)],
            transfer_bucket: vec![(server, 5000)],
        };

        let result = detect_double_pool(&receipt, Some(&cp), Some(&sp), Some(&cd), Some(&sd), 2000);
        assert!(result.is_some());
        let claim = result.unwrap();
        assert!(claim.content_pool_amount > 0);
        assert!(claim.subscription_pool_amount > 0);
    }

    #[test]
    fn test_double_pool_no_content_pool() {
        let cid = ContentId([1u8; 32]);
        let receipt = make_transfer_receipt([10u8; 32], [20u8; 32], cid, 5000);
        let sp = SubscriptionPool::new([20u8; 32], Tier::Lite, 5 * USDC, 1000, 3600);

        let result = detect_double_pool(&receipt, None, Some(&sp), None, None, 2000);
        assert!(result.is_none());
    }

    #[test]
    fn test_double_pool_no_subscription() {
        let cid = ContentId([1u8; 32]);
        let receipt = make_transfer_receipt([10u8; 32], [20u8; 32], cid, 5000);
        let cp = ContentPool::new(cid, Tier::Lite, 5 * USDC);

        let result = detect_double_pool(&receipt, Some(&cp), None, None, None, 2000);
        assert!(result.is_none());
    }

    #[test]
    fn test_double_pool_expired_subscription() {
        let cid = ContentId([1u8; 32]);
        let requester = [20u8; 32];
        let receipt = make_transfer_receipt([10u8; 32], requester, cid, 5000);
        let cp = ContentPool::new(cid, Tier::Lite, 5 * USDC);
        let sp = SubscriptionPool::new(requester, Tier::Lite, 5 * USDC, 1000, 100);

        // now=5000 > expires_at=1100
        let result = detect_double_pool(&receipt, Some(&cp), Some(&sp), None, None, 5000);
        assert!(result.is_none());
    }

    // -- Eviction tests --

    #[test]
    fn test_eviction_excludes_funded() {
        let cids = vec![
            StoredCid { content_id: ContentId([1u8; 32]), is_funded: true, last_accessed: 100, fetch_count: 1, stored_at: 50 },
            StoredCid { content_id: ContentId([2u8; 32]), is_funded: false, last_accessed: 200, fetch_count: 5, stored_at: 60 },
            StoredCid { content_id: ContentId([3u8; 32]), is_funded: false, last_accessed: 50, fetch_count: 2, stored_at: 70 },
        ];

        let result = eviction_priority(&cids, EvictionPolicy::LRU);
        assert_eq!(result.len(), 2);
        // Funded CID [1] should never appear
        assert!(!result.contains(&ContentId([1u8; 32])));
    }

    #[test]
    fn test_eviction_lru_order() {
        let cids = vec![
            StoredCid { content_id: ContentId([1u8; 32]), is_funded: false, last_accessed: 300, fetch_count: 1, stored_at: 50 },
            StoredCid { content_id: ContentId([2u8; 32]), is_funded: false, last_accessed: 100, fetch_count: 5, stored_at: 60 },
            StoredCid { content_id: ContentId([3u8; 32]), is_funded: false, last_accessed: 200, fetch_count: 2, stored_at: 70 },
        ];

        let result = eviction_priority(&cids, EvictionPolicy::LRU);
        assert_eq!(result[0], ContentId([2u8; 32])); // accessed earliest
        assert_eq!(result[1], ContentId([3u8; 32]));
        assert_eq!(result[2], ContentId([1u8; 32]));
    }

    #[test]
    fn test_eviction_least_fetched_order() {
        let cids = vec![
            StoredCid { content_id: ContentId([1u8; 32]), is_funded: false, last_accessed: 100, fetch_count: 10, stored_at: 50 },
            StoredCid { content_id: ContentId([2u8; 32]), is_funded: false, last_accessed: 200, fetch_count: 1, stored_at: 60 },
            StoredCid { content_id: ContentId([3u8; 32]), is_funded: false, last_accessed: 300, fetch_count: 5, stored_at: 70 },
        ];

        let result = eviction_priority(&cids, EvictionPolicy::LeastFetched);
        assert_eq!(result[0], ContentId([2u8; 32])); // fewest fetches
        assert_eq!(result[1], ContentId([3u8; 32]));
        assert_eq!(result[2], ContentId([1u8; 32]));
    }

    #[test]
    fn test_eviction_oldest_order() {
        let cids = vec![
            StoredCid { content_id: ContentId([1u8; 32]), is_funded: false, last_accessed: 100, fetch_count: 1, stored_at: 70 },
            StoredCid { content_id: ContentId([2u8; 32]), is_funded: false, last_accessed: 200, fetch_count: 5, stored_at: 50 },
            StoredCid { content_id: ContentId([3u8; 32]), is_funded: false, last_accessed: 300, fetch_count: 2, stored_at: 60 },
        ];

        let result = eviction_priority(&cids, EvictionPolicy::Oldest);
        assert_eq!(result[0], ContentId([2u8; 32])); // stored earliest
        assert_eq!(result[1], ContentId([3u8; 32]));
        assert_eq!(result[2], ContentId([1u8; 32]));
    }

    #[test]
    fn test_eviction_empty() {
        assert!(eviction_priority(&[], EvictionPolicy::LRU).is_empty());
    }

    #[test]
    fn test_eviction_all_funded() {
        let cids = vec![
            StoredCid { content_id: ContentId([1u8; 32]), is_funded: true, last_accessed: 100, fetch_count: 1, stored_at: 50 },
        ];
        assert!(eviction_priority(&cids, EvictionPolicy::LRU).is_empty());
    }
}
