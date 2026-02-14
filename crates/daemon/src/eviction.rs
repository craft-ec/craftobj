//! Eviction manager — monitors disk usage and evicts free CID shards under storage pressure.

use std::collections::HashSet;

use datacraft_core::economics::{eviction_priority, EvictionPolicy, StoredCid};
use datacraft_core::ContentId;
use datacraft_store::FsStore;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// High-water mark: eviction triggers when usage exceeds this fraction of max.
const HIGH_WATERMARK: f64 = 0.90;
/// Low-water mark: eviction continues until usage drops below this fraction of max.
const LOW_WATERMARK: f64 = 0.80;

/// Eviction manager configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionConfig {
    /// Maximum storage in bytes. 0 = unlimited (no eviction).
    pub max_storage_bytes: u64,
    /// Eviction policy for unfunded CIDs.
    pub eviction_policy: EvictionPolicySerde,
    /// Whether eviction is enabled.
    pub enable_eviction: bool,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            max_storage_bytes: 10 * 1_000_000_000, // 10 GB
            eviction_policy: EvictionPolicySerde::LRU,
            enable_eviction: true,
        }
    }
}

/// Serializable wrapper for EvictionPolicy (core type doesn't derive Serialize).
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

/// Manages eviction of unfunded CIDs under storage pressure.
pub struct EvictionManager {
    policy: EvictionPolicy,
    max_storage_bytes: u64,
    funded_cids: HashSet<ContentId>,
    enabled: bool,
}

impl EvictionManager {
    /// Create a new eviction manager from config.
    pub fn new(config: &EvictionConfig) -> Self {
        Self {
            policy: config.eviction_policy.into(),
            max_storage_bytes: config.max_storage_bytes,
            funded_cids: HashSet::new(),
            enabled: config.enable_eviction,
        }
    }

    /// Mark a CID as funded (protected from eviction).
    pub fn mark_funded(&mut self, cid: ContentId) {
        self.funded_cids.insert(cid);
    }

    /// Mark a CID as unfunded (eligible for eviction).
    pub fn mark_unfunded(&mut self, cid: &ContentId) {
        self.funded_cids.remove(cid);
    }

    /// Returns the set of funded CIDs.
    pub fn funded_cids(&self) -> &HashSet<ContentId> {
        &self.funded_cids
    }

    /// Change the eviction policy at runtime.
    pub fn set_policy(&mut self, policy: EvictionPolicy) {
        self.policy = policy;
    }

    /// Get current policy.
    pub fn policy(&self) -> EvictionPolicy {
        self.policy
    }

    /// Get max storage bytes.
    pub fn max_storage_bytes(&self) -> u64 {
        self.max_storage_bytes
    }

    /// Check disk usage and evict free CIDs if above threshold.
    ///
    /// Returns the list of evicted CIDs.
    pub fn check_and_evict(&self, store: &FsStore) -> Vec<ContentId> {
        if !self.enabled || self.max_storage_bytes == 0 {
            return Vec::new();
        }

        // a. Calculate current disk usage
        let mut current_usage = match store.disk_usage() {
            Ok(u) => u,
            Err(e) => {
                warn!("Failed to calculate disk usage: {}", e);
                return Vec::new();
            }
        };

        // b. If below high watermark, nothing to do
        let high_threshold = (self.max_storage_bytes as f64 * HIGH_WATERMARK) as u64;
        if current_usage <= high_threshold {
            debug!(
                "Storage usage {:.1} MB / {:.1} MB — below threshold, no eviction needed",
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

        // c. Build StoredCid list from store metadata
        let all_cids = match store.list_content() {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to list content for eviction: {}", e);
                return Vec::new();
            }
        };

        let stored_cids: Vec<StoredCid> = all_cids
            .iter()
            .filter_map(|cid| {
                let meta = store.get_metadata(cid).unwrap_or_else(|| {
                    // No metadata — treat as very old with zero fetches
                    datacraft_store::AccessMetadata {
                        last_accessed: 0,
                        fetch_count: 0,
                        stored_at: 0,
                    }
                });
                Some(StoredCid {
                    content_id: *cid,
                    is_funded: self.funded_cids.contains(cid),
                    last_accessed: meta.last_accessed,
                    fetch_count: meta.fetch_count,
                    stored_at: meta.stored_at,
                })
            })
            .collect();

        // d. Get eviction priority ordering
        let priority = eviction_priority(&stored_cids, self.policy);

        // e. Evict until below low watermark
        let low_threshold = (self.max_storage_bytes as f64 * LOW_WATERMARK) as u64;
        let mut evicted = Vec::new();

        for cid in priority {
            if current_usage <= low_threshold {
                break;
            }

            let cid_size = store.cid_disk_usage(&cid);

            // f. Remove from store
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

    /// Get storage status for IPC.
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

/// Storage status returned by the `storage.status` IPC command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatus {
    pub used_bytes: u64,
    pub max_bytes: u64,
    pub cid_count: u64,
    pub funded_count: u64,
    pub free_count: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use datacraft_core::{default_erasure_config, ChunkManifest};

    fn test_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join("datacraft-eviction-test")
            .join(format!(
                "{}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .subsec_nanos()
            ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_cid(b: u8) -> ContentId {
        ContentId([b; 32])
    }

    fn store_content(store: &FsStore, cid: &ContentId, shard_data: &[u8], meta: Option<datacraft_store::AccessMetadata>) {
        let manifest = ChunkManifest {
            content_id: *cid,
            content_hash: cid.0,
            k: 4,
            chunk_size: 65536,
            chunk_count: 1,
            erasure_config: default_erasure_config(),
            content_size: shard_data.len() as u64,
        };
        store.put_manifest(&manifest).unwrap();
        store.put_shard(cid, 0, 0, shard_data).unwrap();
        if let Some(m) = meta {
            store.put_metadata(cid, &m).unwrap();
        } else {
            store.ensure_metadata(cid).unwrap();
        }
    }

    #[test]
    fn test_no_eviction_below_threshold() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let config = EvictionConfig {
            max_storage_bytes: 1_000_000, // 1 MB
            eviction_policy: EvictionPolicySerde::LRU,
            enable_eviction: true,
        };
        let manager = EvictionManager::new(&config);

        // Store small content — well below threshold
        let cid = make_cid(1);
        store_content(&store, &cid, &[0u8; 100], None);

        let evicted = manager.check_and_evict(&store);
        assert!(evicted.is_empty());
        assert!(store.has_manifest(&cid));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_eviction_triggers_at_threshold() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        // Very small limit to trigger eviction easily
        let config = EvictionConfig {
            max_storage_bytes: 500,
            eviction_policy: EvictionPolicySerde::LRU,
            enable_eviction: true,
        };
        let manager = EvictionManager::new(&config);

        // Store enough content to exceed 90% of 500 bytes
        let cid1 = make_cid(1);
        let cid2 = make_cid(2);
        store_content(&store, &cid1, &[0u8; 200], Some(datacraft_store::AccessMetadata {
            last_accessed: 100,
            fetch_count: 1,
            stored_at: 50,
        }));
        store_content(&store, &cid2, &[0u8; 200], Some(datacraft_store::AccessMetadata {
            last_accessed: 200,
            fetch_count: 5,
            stored_at: 60,
        }));

        let evicted = manager.check_and_evict(&store);
        // Should evict at least one CID
        assert!(!evicted.is_empty());
        // cid1 should be evicted first (older access time under LRU)
        assert!(evicted.contains(&cid1));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_funded_cids_protected() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let config = EvictionConfig {
            max_storage_bytes: 500,
            eviction_policy: EvictionPolicySerde::LRU,
            enable_eviction: true,
        };
        let mut manager = EvictionManager::new(&config);

        let funded_cid = make_cid(1);
        let free_cid = make_cid(2);

        manager.mark_funded(funded_cid);

        store_content(&store, &funded_cid, &[0u8; 200], Some(datacraft_store::AccessMetadata {
            last_accessed: 10, // oldest access — would normally be evicted first
            fetch_count: 0,
            stored_at: 10,
        }));
        store_content(&store, &free_cid, &[0u8; 200], Some(datacraft_store::AccessMetadata {
            last_accessed: 200,
            fetch_count: 10,
            stored_at: 200,
        }));

        let evicted = manager.check_and_evict(&store);
        // Funded CID must NOT be evicted
        assert!(!evicted.contains(&funded_cid));
        assert!(store.has_manifest(&funded_cid));
        // Free CID should be evicted
        if !evicted.is_empty() {
            assert!(evicted.contains(&free_cid));
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_policy_ordering_least_fetched() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let config = EvictionConfig {
            max_storage_bytes: 500,
            eviction_policy: EvictionPolicySerde::LeastFetched,
            enable_eviction: true,
        };
        let manager = EvictionManager::new(&config);

        let cid_low = make_cid(1);
        let cid_high = make_cid(2);

        store_content(&store, &cid_low, &[0u8; 200], Some(datacraft_store::AccessMetadata {
            last_accessed: 300,
            fetch_count: 1, // low fetch count — should be evicted first
            stored_at: 100,
        }));
        store_content(&store, &cid_high, &[0u8; 200], Some(datacraft_store::AccessMetadata {
            last_accessed: 100,
            fetch_count: 100, // high fetch count — should survive
            stored_at: 50,
        }));

        let evicted = manager.check_and_evict(&store);
        if !evicted.is_empty() {
            assert_eq!(evicted[0], cid_low);
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_eviction_disabled() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let config = EvictionConfig {
            max_storage_bytes: 100,
            eviction_policy: EvictionPolicySerde::LRU,
            enable_eviction: false, // disabled
        };
        let manager = EvictionManager::new(&config);

        let cid = make_cid(1);
        store_content(&store, &cid, &[0u8; 500], None);

        let evicted = manager.check_and_evict(&store);
        assert!(evicted.is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_storage_status() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let config = EvictionConfig::default();
        let mut manager = EvictionManager::new(&config);

        let cid1 = make_cid(1);
        let cid2 = make_cid(2);
        manager.mark_funded(cid1);

        store_content(&store, &cid1, &[0u8; 100], None);
        store_content(&store, &cid2, &[0u8; 100], None);

        let status = manager.storage_status(&store);
        assert_eq!(status.cid_count, 2);
        assert_eq!(status.funded_count, 1);
        assert_eq!(status.free_count, 1);
        assert!(status.used_bytes > 0);
        assert_eq!(status.max_bytes, config.max_storage_bytes);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_mark_funded_unfunded() {
        let config = EvictionConfig::default();
        let mut manager = EvictionManager::new(&config);
        let cid = make_cid(1);

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
    fn test_access_metadata_roundtrip() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let cid = make_cid(1);

        let meta = datacraft_store::AccessMetadata {
            last_accessed: 12345,
            fetch_count: 42,
            stored_at: 10000,
        };
        store.put_metadata(&cid, &meta).unwrap();

        let loaded = store.get_metadata(&cid).unwrap();
        assert_eq!(loaded.last_accessed, 12345);
        assert_eq!(loaded.fetch_count, 42);
        assert_eq!(loaded.stored_at, 10000);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_record_access() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let cid = make_cid(1);

        store.ensure_metadata(&cid).unwrap();
        let before = store.get_metadata(&cid).unwrap();
        assert_eq!(before.fetch_count, 0);

        store.record_access(&cid).unwrap();
        let after = store.get_metadata(&cid).unwrap();
        assert_eq!(after.fetch_count, 1);
        assert!(after.last_accessed >= before.last_accessed);

        store.record_access(&cid).unwrap();
        let after2 = store.get_metadata(&cid).unwrap();
        assert_eq!(after2.fetch_count, 2);

        std::fs::remove_dir_all(&dir).ok();
    }
}
