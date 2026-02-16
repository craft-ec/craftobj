//! Eviction manager — monitors disk usage and evicts free CID pieces under storage pressure.

use std::collections::HashSet;

use datacraft_core::economics::{EvictionPolicy};
use datacraft_core::ContentId;
use datacraft_store::FsStore;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

const HIGH_WATERMARK: f64 = 0.90;
const LOW_WATERMARK: f64 = 0.80;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionConfig {
    pub max_storage_bytes: u64,
    pub eviction_policy: EvictionPolicySerde,
    pub enable_eviction: bool,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            max_storage_bytes: 10 * 1_000_000_000,
            eviction_policy: EvictionPolicySerde::LRU,
            enable_eviction: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionPolicySerde {
    LRU,
    LeastFetched,
    Oldest,
}

impl From<EvictionPolicySerde> for EvictionPolicy {
    fn from(s: EvictionPolicySerde) -> Self {
        match s {
            EvictionPolicySerde::LRU => EvictionPolicy::LRU,
            EvictionPolicySerde::LeastFetched => EvictionPolicy::LeastFetched,
            EvictionPolicySerde::Oldest => EvictionPolicy::Oldest,
        }
    }
}

impl From<EvictionPolicy> for EvictionPolicySerde {
    fn from(p: EvictionPolicy) -> Self {
        match p {
            EvictionPolicy::LRU => EvictionPolicySerde::LRU,
            EvictionPolicy::LeastFetched => EvictionPolicySerde::LeastFetched,
            EvictionPolicy::Oldest => EvictionPolicySerde::Oldest,
        }
    }
}

pub struct EvictionManager {
    policy: EvictionPolicy,
    max_storage_bytes: u64,
    funded_cids: HashSet<ContentId>,
    enabled: bool,
}

impl EvictionManager {
    pub fn new(config: &EvictionConfig) -> Self {
        Self {
            policy: config.eviction_policy.into(),
            max_storage_bytes: config.max_storage_bytes,
            funded_cids: HashSet::new(),
            enabled: config.enable_eviction,
        }
    }

    pub fn mark_funded(&mut self, cid: ContentId) {
        self.funded_cids.insert(cid);
    }

    pub fn mark_unfunded(&mut self, cid: &ContentId) {
        self.funded_cids.remove(cid);
    }

    pub fn funded_cids(&self) -> &HashSet<ContentId> {
        &self.funded_cids
    }

    pub fn set_policy(&mut self, policy: EvictionPolicy) {
        self.policy = policy;
    }

    pub fn policy(&self) -> EvictionPolicy {
        self.policy
    }

    pub fn max_storage_bytes(&self) -> u64 {
        self.max_storage_bytes
    }

    /// Check disk usage and evict unfunded CIDs if above threshold.
    pub fn check_and_evict(&self, store: &FsStore) -> Vec<ContentId> {
        if !self.enabled || self.max_storage_bytes == 0 {
            return Vec::new();
        }

        let mut current_usage = match store.disk_usage() {
            Ok(u) => u,
            Err(e) => {
                warn!("Failed to calculate disk usage: {}", e);
                return Vec::new();
            }
        };

        let high_threshold = (self.max_storage_bytes as f64 * HIGH_WATERMARK) as u64;
        if current_usage <= high_threshold {
            debug!(
                "Storage usage {:.1} MB / {:.1} MB — below threshold",
                current_usage as f64 / 1e6,
                self.max_storage_bytes as f64 / 1e6
            );
            return Vec::new();
        }

        info!(
            "Storage pressure: {:.1} MB / {:.1} MB ({:.0}%) — starting eviction",
            current_usage as f64 / 1e6,
            self.max_storage_bytes as f64 / 1e6,
            current_usage as f64 / self.max_storage_bytes as f64 * 100.0
        );

        let all_cids = match store.list_content() {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to list content for eviction: {}", e);
                return Vec::new();
            }
        };

        // Build eviction candidates: unfunded CIDs sorted by disk usage (smallest first)
        let mut candidates: Vec<(ContentId, u64)> = all_cids
            .iter()
            .filter(|cid| !self.funded_cids.contains(cid))
            .map(|cid| (*cid, store.cid_disk_usage(cid)))
            .collect();
        candidates.sort_by_key(|(_, size)| *size);

        let low_threshold = (self.max_storage_bytes as f64 * LOW_WATERMARK) as u64;
        let mut evicted = Vec::new();

        for (cid, cid_size) in candidates {
            if current_usage <= low_threshold {
                break;
            }

            if let Err(e) = store.delete_content(&cid) {
                warn!("Failed to evict {}: {}", cid, e);
                continue;
            }

            current_usage = current_usage.saturating_sub(cid_size);
            info!("Evicted {} ({:.1} KB)", cid, cid_size as f64 / 1e3);
            evicted.push(cid);
        }

        if !evicted.is_empty() {
            info!(
                "Eviction complete: removed {} CIDs, usage now {:.1} MB",
                evicted.len(),
                current_usage as f64 / 1e6
            );
        }

        evicted
    }

    pub fn storage_status(&self, store: &FsStore) -> StorageStatus {
        let used_bytes = store.disk_usage().unwrap_or(0);
        let all_cids = store.list_content().unwrap_or_default();
        let cid_count = all_cids.len() as u64;
        let funded_count = all_cids
            .iter()
            .filter(|c| self.funded_cids.contains(c))
            .count() as u64;

        StorageStatus {
            used_bytes,
            max_bytes: self.max_storage_bytes,
            cid_count,
            funded_count,
            free_count: cid_count - funded_count,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatus {
    pub used_bytes: u64,
    pub max_bytes: u64,
    pub cid_count: u64,
    pub funded_count: u64,
    pub free_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_funded_unfunded() {
        let config = EvictionConfig::default();
        let mut manager = EvictionManager::new(&config);
        let cid = ContentId([1u8; 32]);

        assert!(!manager.funded_cids().contains(&cid));
        manager.mark_funded(cid);
        assert!(manager.funded_cids().contains(&cid));
        manager.mark_unfunded(&cid);
        assert!(!manager.funded_cids().contains(&cid));
    }

    #[test]
    fn test_set_policy() {
        let config = EvictionConfig::default();
        let mut manager = EvictionManager::new(&config);
        assert_eq!(manager.policy(), EvictionPolicy::LRU);
        manager.set_policy(EvictionPolicy::Oldest);
        assert_eq!(manager.policy(), EvictionPolicy::Oldest);
    }

    #[test]
    fn test_no_eviction_below_threshold() {
        let dir = std::env::temp_dir().join(format!("eviction-test-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let config = EvictionConfig {
            max_storage_bytes: 1_000_000,
            eviction_policy: EvictionPolicySerde::LRU,
            enable_eviction: true,
        };
        let manager = EvictionManager::new(&config);

        let evicted = manager.check_and_evict(&store);
        assert!(evicted.is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_eviction_disabled() {
        let dir = std::env::temp_dir().join(format!("eviction-disabled-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let config = EvictionConfig {
            max_storage_bytes: 100,
            eviction_policy: EvictionPolicySerde::LRU,
            enable_eviction: false,
        };
        let manager = EvictionManager::new(&config);

        let evicted = manager.check_and_evict(&store);
        assert!(evicted.is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }
}
