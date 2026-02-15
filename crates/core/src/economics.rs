//! Economics: Creator Pool, tier logic, distribution math, and eviction policy.
//!
//! The economics model uses Creator Pools (one per creator, funding multiple CIDs)
//! with StorageReceipt (PDP) as the ONLY settlement mechanism for storage distribution.
//! Premium egress uses payment channels (see `payment_channel` module).
//! Free egress has no reward — best effort, volunteer.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{ContentId, StorageReceipt};

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
// Creator Pool
// ---------------------------------------------------------------------------

/// Default protocol fee in basis points (e.g. 500 = 5%).
pub const DEFAULT_PROTOCOL_FEE_BPS: u16 = 500;

/// Creator Pool — one per creator, funds storage for all their CIDs.
///
/// The pool is funded by direct USDC deposits from the creator. Pool balance
/// determines content availability — depleted pool → content degrades to free
/// tier (volunteer serving only).
///
/// Distribution is based solely on StorageReceipts (PDP). No transfer/egress
/// settlement — premium egress uses payment channels directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorPool {
    /// Creator's public key (ed25519, 32 bytes).
    pub creator: [u8; 32],
    /// Tier determining minimum shard health ratio for all the creator's CIDs.
    pub tier: Tier,
    /// USDC lamports remaining in the pool.
    pub balance: u64,
    /// Cumulative USDC lamports claimed from this pool.
    pub total_claimed: u64,
    /// Protocol fee in basis points (applied on every claim).
    pub protocol_fee_bps: u16,
    /// CIDs funded by this pool.
    pub funded_cids: Vec<ContentId>,
}

impl CreatorPool {
    /// Create a new creator pool.
    pub fn new(creator: [u8; 32], tier: Tier, balance: u64) -> Self {
        Self {
            creator,
            tier,
            balance,
            total_claimed: 0,
            protocol_fee_bps: DEFAULT_PROTOCOL_FEE_BPS,
            funded_cids: Vec::new(),
        }
    }

    /// Create a new creator pool with a custom protocol fee.
    pub fn with_fee(creator: [u8; 32], tier: Tier, balance: u64, protocol_fee_bps: u16) -> Self {
        Self {
            creator,
            tier,
            balance,
            total_claimed: 0,
            protocol_fee_bps,
            funded_cids: Vec::new(),
        }
    }

    /// Whether the pool is funded (has remaining balance).
    pub fn is_funded(&self) -> bool {
        self.balance > 0
    }

    /// Add a CID to be funded by this pool.
    pub fn add_cid(&mut self, cid: ContentId) {
        if !self.funded_cids.contains(&cid) {
            self.funded_cids.push(cid);
        }
    }

    /// Remove a CID from this pool.
    pub fn remove_cid(&mut self, cid: &ContentId) {
        self.funded_cids.retain(|c| c != cid);
    }

    /// Check if a CID is funded by this pool.
    pub fn funds_cid(&self, cid: &ContentId) -> bool {
        self.funded_cids.contains(cid)
    }
}

// ---------------------------------------------------------------------------
// Pool distribution (StorageReceipt-only)
// ---------------------------------------------------------------------------

/// Public key type alias (32-byte ed25519 pubkey).
pub type PublicKey = [u8; 32];

/// Distribution of StorageReceipts across nodes.
///
/// Only StorageReceipts (PDP) are used for settlement. TransferReceipts
/// are analytics-only and not included in distribution.
#[derive(Debug, Clone, Default)]
pub struct PoolDistribution {
    /// (node pubkey, receipt count) from StorageReceipts.
    pub storage_receipts: Vec<(PublicKey, u64)>,
}

/// Compute distribution from StorageReceipts only.
///
/// Aggregates StorageReceipts by `storage_node` (count). Each PDP pass = 1 receipt.
pub fn compute_distribution(storage_receipts: &[StorageReceipt]) -> PoolDistribution {
    let mut storage_map: HashMap<PublicKey, u64> = HashMap::new();
    for r in storage_receipts {
        *storage_map.entry(r.storage_node).or_default() += 1;
    }

    PoolDistribution {
        storage_receipts: storage_map.into_iter().collect(),
    }
}

/// Compute the USDC lamports claimable by `node` from a creator pool.
///
/// `node_payout = (node_receipts / total_receipts) * pool_balance * (10_000 - fee_bps) / 10_000`
///
/// Protocol fee is deducted from each claim.
pub fn compute_claim(
    node: &PublicKey,
    distribution: &PoolDistribution,
    pool_balance: u64,
    protocol_fee_bps: u16,
) -> u64 {
    let total: u64 = distribution.storage_receipts.iter().map(|(_, c)| c).sum();
    if total == 0 {
        return 0;
    }

    let node_count = distribution
        .storage_receipts
        .iter()
        .find(|(k, _)| k == node)
        .map(|(_, c)| *c)
        .unwrap_or(0);

    if node_count == 0 {
        return 0;
    }

    let gross = ((pool_balance as u128) * (node_count as u128) / (total as u128)) as u64;
    // Deduct protocol fee
    (gross as u128 * (10_000 - protocol_fee_bps as u128) / 10_000) as u64
}

/// Compute the protocol fee amount for a given gross claim.
pub fn protocol_fee(amount: u64, fee_bps: u16) -> u64 {
    (amount as u128 * fee_bps as u128 / 10_000) as u64
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
    /// Whether this CID has a funded creator pool.
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

    // -- CreatorPool tests --

    #[test]
    fn test_creator_pool_new() {
        let pool = CreatorPool::new([1u8; 32], Tier::Pro, 100 * USDC);
        assert!(pool.is_funded());
        assert_eq!(pool.total_claimed, 0);
        assert_eq!(pool.protocol_fee_bps, DEFAULT_PROTOCOL_FEE_BPS);
        assert!(pool.funded_cids.is_empty());
    }

    #[test]
    fn test_creator_pool_unfunded() {
        let pool = CreatorPool::new([0u8; 32], Tier::Free, 0);
        assert!(!pool.is_funded());
    }

    #[test]
    fn test_creator_pool_with_fee() {
        let pool = CreatorPool::with_fee([1u8; 32], Tier::Lite, 50 * USDC, 300);
        assert_eq!(pool.protocol_fee_bps, 300);
    }

    #[test]
    fn test_creator_pool_cid_management() {
        let mut pool = CreatorPool::new([1u8; 32], Tier::Pro, 100 * USDC);
        let cid1 = ContentId([1u8; 32]);
        let cid2 = ContentId([2u8; 32]);

        pool.add_cid(cid1);
        pool.add_cid(cid2);
        assert!(pool.funds_cid(&cid1));
        assert!(pool.funds_cid(&cid2));
        assert_eq!(pool.funded_cids.len(), 2);

        // Adding duplicate is a no-op
        pool.add_cid(cid1);
        assert_eq!(pool.funded_cids.len(), 2);

        pool.remove_cid(&cid1);
        assert!(!pool.funds_cid(&cid1));
        assert!(pool.funds_cid(&cid2));
    }

    #[test]
    fn test_creator_pool_serde_roundtrip() {
        let mut pool = CreatorPool::new([1u8; 32], Tier::Standard, 10 * USDC);
        pool.add_cid(ContentId([5u8; 32]));
        let json = serde_json::to_string(&pool).unwrap();
        let parsed: CreatorPool = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.creator, pool.creator);
        assert_eq!(parsed.tier, pool.tier);
        assert_eq!(parsed.balance, pool.balance);
        assert_eq!(parsed.funded_cids.len(), 1);
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

    #[test]
    fn test_compute_distribution_empty() {
        let d = compute_distribution(&[]);
        assert!(d.storage_receipts.is_empty());
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

        let d = compute_distribution(&storage);

        let sa: HashMap<_, _> = d.storage_receipts.into_iter().collect();
        assert_eq!(sa[&node_a], 2);
        assert_eq!(sa[&node_b], 1);
    }

    #[test]
    fn test_compute_claim_proportional() {
        let node_a = [1u8; 32];
        let node_b = [2u8; 32];
        let balance = 1_000_000u64; // 1 USDC

        let dist = PoolDistribution {
            storage_receipts: vec![(node_a, 3), (node_b, 1)],
        };

        // With 0% fee for easy math
        let claim_a = compute_claim(&node_a, &dist, balance, 0);
        let claim_b = compute_claim(&node_b, &dist, balance, 0);

        // A gets 3/4 = 750k, B gets 1/4 = 250k
        assert_eq!(claim_a, 750_000);
        assert_eq!(claim_b, 250_000);
        assert_eq!(claim_a + claim_b, balance);
    }

    #[test]
    fn test_compute_claim_with_protocol_fee() {
        let node = [1u8; 32];
        let balance = 1_000_000u64;

        let dist = PoolDistribution {
            storage_receipts: vec![(node, 10)],
        };

        // 5% fee (500 bps)
        let claim = compute_claim(&node, &dist, balance, 500);
        // gross = 1_000_000, net = 1_000_000 * 9500 / 10000 = 950_000
        assert_eq!(claim, 950_000);
    }

    #[test]
    fn test_compute_claim_with_10_percent_fee() {
        let node_a = [1u8; 32];
        let node_b = [2u8; 32];
        let balance = 1_000_000u64;

        let dist = PoolDistribution {
            storage_receipts: vec![(node_a, 1), (node_b, 1)],
        };

        // 10% fee (1000 bps)
        let claim_a = compute_claim(&node_a, &dist, balance, 1000);
        let claim_b = compute_claim(&node_b, &dist, balance, 1000);

        // Each gets 500k gross, 450k net
        assert_eq!(claim_a, 450_000);
        assert_eq!(claim_b, 450_000);
    }

    #[test]
    fn test_compute_claim_unknown_node() {
        let dist = PoolDistribution {
            storage_receipts: vec![([1u8; 32], 10)],
        };
        let unknown = [99u8; 32];
        assert_eq!(compute_claim(&unknown, &dist, 1_000_000, 0), 0);
    }

    #[test]
    fn test_compute_claim_empty_distribution() {
        let node = [1u8; 32];
        let dist = PoolDistribution {
            storage_receipts: vec![],
        };
        assert_eq!(compute_claim(&node, &dist, 1_000_000, 0), 0);
    }

    #[test]
    fn test_protocol_fee_calculation() {
        assert_eq!(protocol_fee(1_000_000, 500), 50_000); // 5%
        assert_eq!(protocol_fee(1_000_000, 1000), 100_000); // 10%
        assert_eq!(protocol_fee(1_000_000, 0), 0); // 0%
        assert_eq!(protocol_fee(0, 500), 0); // zero amount
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
        assert_eq!(result[0], ContentId([2u8; 32]));
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
        assert_eq!(result[0], ContentId([2u8; 32]));
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
        assert_eq!(result[0], ContentId([2u8; 32]));
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
