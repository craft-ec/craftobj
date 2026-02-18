//! Eviction manager — monitors disk usage and evicts free CID pieces under storage pressure.
//!
//! Three degradation paths:
//! 1. Funded → Free: pool depleted → content loses funded status (tracked externally)
//! 2. Hot → Normal: demand drops → scaling nodes leave naturally (no explicit eviction)
//! 3. Free → Death: storage pressure → LRU eviction → rank < k → retirement

use std::collections::HashMap;
use std::time::Instant;

use craftobj_core::ContentId;
use craftobj_store::FsStore;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

const HIGH_WATERMARK: f64 = 0.90;
const LOW_WATERMARK: f64 = 0.80;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictionConfig {
    pub max_storage_bytes: u64,
    pub enable_eviction: bool,
}

impl Default for EvictionConfig {
    fn default() -> Self {
        Self {
            max_storage_bytes: 10 * 1_000_000_000,
            enable_eviction: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Content status tracking
// ---------------------------------------------------------------------------

/// Per-CID status for eviction decisions.
#[derive(Debug, Clone)]
pub struct ContentStatus {
    /// Funded content is protected from eviction.
    pub funded: bool,
    /// Last time this content was accessed (fetched/served).
    pub last_accessed: Instant,
    /// Last successful PDP pass (if any).
    pub last_pdp_pass: Option<Instant>,
}

impl ContentStatus {
    pub fn new_free() -> Self {
        Self {
            funded: false,
            last_accessed: Instant::now(),
            last_pdp_pass: None,
        }
    }

    pub fn new_funded() -> Self {
        Self {
            funded: true,
            last_accessed: Instant::now(),
            last_pdp_pass: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Eviction result
// ---------------------------------------------------------------------------

/// Reason a CID was evicted or retired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvictionReason {
    /// Storage pressure — LRU free content evicted.
    StoragePressure,
    /// Segment rank dropped below k — content unrecoverable.
    Unrecoverable { segment: u32, rank: usize, k: usize },
}

impl std::fmt::Display for EvictionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StoragePressure => write!(f, "storage pressure (LRU)"),
            Self::Unrecoverable { segment, rank, k } => {
                write!(f, "unrecoverable: segment {} rank {} < k {}", segment, rank, k)
            }
        }
    }
}

/// Result of an eviction cycle.
#[derive(Debug, Default)]
pub struct EvictionResult {
    pub evicted: Vec<(ContentId, EvictionReason)>,
    pub retired: Vec<(ContentId, EvictionReason)>,
}

// ---------------------------------------------------------------------------
// Eviction manager
// ---------------------------------------------------------------------------

pub struct EvictionManager {
    max_storage_bytes: u64,
    enabled: bool,
    /// Per-CID status (funded, last_accessed, etc.)
    statuses: HashMap<ContentId, ContentStatus>,
}

impl EvictionManager {
    pub fn new(config: &EvictionConfig) -> Self {
        Self {
            max_storage_bytes: config.max_storage_bytes,
            enabled: config.enable_eviction,
            statuses: HashMap::new(),
        }
    }

    // -- Status tracking --

    /// Track a CID with the given status.
    pub fn track(&mut self, cid: ContentId, status: ContentStatus) {
        self.statuses.insert(cid, status);
    }

    /// Remove tracking for a CID (after eviction/retirement).
    pub fn untrack(&mut self, cid: &ContentId) {
        self.statuses.remove(cid);
    }

    /// Mark a CID as funded (protected from eviction).
    pub fn mark_funded(&mut self, cid: ContentId) {
        self.statuses
            .entry(cid)
            .and_modify(|s| s.funded = true)
            .or_insert_with(ContentStatus::new_funded);
    }

    /// Mark a CID as unfunded (eligible for eviction under pressure).
    pub fn mark_unfunded(&mut self, cid: &ContentId) {
        if let Some(s) = self.statuses.get_mut(cid) {
            s.funded = false;
        }
    }

    /// Record an access (fetch/serve) for a CID.
    pub fn record_access(&mut self, cid: &ContentId) {
        if let Some(s) = self.statuses.get_mut(cid) {
            s.last_accessed = Instant::now();
        }
    }

    /// Record a successful PDP pass for a CID.
    pub fn record_pdp_pass(&mut self, cid: &ContentId) {
        if let Some(s) = self.statuses.get_mut(cid) {
            s.last_pdp_pass = Some(Instant::now());
        }
    }

    pub fn is_funded(&self, cid: &ContentId) -> bool {
        self.statuses.get(cid).is_some_and(|s| s.funded)
    }

    pub fn status(&self, cid: &ContentId) -> Option<&ContentStatus> {
        self.statuses.get(cid)
    }

    pub fn max_storage_bytes(&self) -> u64 {
        self.max_storage_bytes
    }

    // -- Storage pressure check --

    /// Returns true if disk usage exceeds the high watermark.
    pub fn is_under_pressure(&self, store: &FsStore) -> bool {
        if self.max_storage_bytes == 0 {
            return false;
        }
        let used = store.disk_usage().unwrap_or(0);
        let threshold = (self.max_storage_bytes as f64 * HIGH_WATERMARK) as u64;
        used > threshold
    }

    // -- Eviction cycle --

    /// Run one eviction cycle: evict free LRU content under storage pressure.
    ///
    /// Returns the list of evicted CIDs and reasons.
    pub fn check_and_evict(&mut self, store: &FsStore) -> Vec<(ContentId, EvictionReason)> {
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

        // Build eviction candidates: unfunded CIDs sorted by last_accessed (oldest first = LRU)
        let mut candidates: Vec<(ContentId, Instant, u64)> = self
            .statuses
            .iter()
            .filter(|(_, status)| !status.funded)
            .map(|(cid, status)| (*cid, status.last_accessed, store.cid_disk_usage(cid)))
            .collect();
        // Sort by last_accessed ascending (oldest first)
        candidates.sort_by_key(|(_, accessed, _)| *accessed);

        let low_threshold = (self.max_storage_bytes as f64 * LOW_WATERMARK) as u64;
        let mut evicted = Vec::new();

        for (cid, _, cid_size) in candidates {
            if current_usage <= low_threshold {
                break;
            }

            if let Err(e) = store.delete_content(&cid) {
                warn!("Failed to evict {}: {}", cid, e);
                continue;
            }

            current_usage = current_usage.saturating_sub(cid_size);
            info!("Evicted {} ({:.1} KB) — storage pressure LRU", cid, cid_size as f64 / 1e3);
            self.statuses.remove(&cid);
            evicted.push((cid, EvictionReason::StoragePressure));
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

    // -- Retirement detection --

    /// Check if any segment of a CID has rank < k (unrecoverable).
    ///
    /// `segment_ranks` maps segment index → rank (from PDP health assessment).
    /// Returns the first unrecoverable segment found, if any.
    pub fn check_retirement(
        &self,
        _cid: &ContentId,
        k: usize,
        segment_ranks: &HashMap<u32, usize>,
    ) -> Option<EvictionReason> {
        for (&segment, &rank) in segment_ranks {
            if rank < k {
                return Some(EvictionReason::Unrecoverable { segment, rank, k });
            }
        }
        None
    }

    /// Retire a CID: remove local pieces and untrack.
    pub fn retire(&mut self, cid: &ContentId, store: &FsStore) -> Result<(), String> {
        if let Err(e) = store.delete_content(cid) {
            warn!("Failed to delete retired content {}: {}", cid, e);
            return Err(format!("delete failed: {}", e));
        }
        self.statuses.remove(cid);
        info!("Retired unrecoverable content {}", cid);
        Ok(())
    }

    // -- Full maintenance cycle --

    /// Run eviction + retirement checks. Returns combined results.
    ///
    /// `segment_ranks_by_cid`: for each CID, the rank per segment from latest PDP.
    pub fn run_maintenance(
        &mut self,
        store: &FsStore,
        k_by_cid: &HashMap<ContentId, usize>,
        segment_ranks_by_cid: &HashMap<ContentId, HashMap<u32, usize>>,
    ) -> EvictionResult {
        let mut result = EvictionResult::default();

        // 1. Retirement detection (rank < k)
        let cids_to_check: Vec<ContentId> = segment_ranks_by_cid.keys().copied().collect();
        for cid in cids_to_check {
            let k = match k_by_cid.get(&cid) {
                Some(&k) => k,
                None => continue,
            };
            let ranks = match segment_ranks_by_cid.get(&cid) {
                Some(r) => r,
                None => continue,
            };
            if let Some(reason) = self.check_retirement(&cid, k, ranks) {
                warn!("Content {} is unrecoverable: {}", cid, reason);
                let _ = self.retire(&cid, store);
                result.retired.push((cid, reason));
            }
        }

        // 2. Storage pressure eviction (LRU free content)
        let evicted = self.check_and_evict(store);
        result.evicted = evicted;

        result
    }

    /// Storage status summary.
    pub fn storage_status(&self, store: &FsStore) -> StorageStatus {
        let used_bytes = store.disk_usage().unwrap_or(0);
        let total = self.statuses.len() as u64;
        let funded_count = self.statuses.values().filter(|s| s.funded).count() as u64;

        StorageStatus {
            used_bytes,
            max_bytes: self.max_storage_bytes,
            cid_count: total,
            funded_count,
            free_count: total - funded_count,
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cid(byte: u8) -> ContentId {
        ContentId([byte; 32])
    }

    fn make_store() -> (FsStore, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("eviction-test-{}-{}", std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let store = FsStore::new(&dir).unwrap();
        (store, dir)
    }

    #[test]
    fn test_track_and_funded_status() {
        let config = EvictionConfig::default();
        let mut manager = EvictionManager::new(&config);
        let cid = make_cid(1);

        assert!(!manager.is_funded(&cid));
        manager.mark_funded(cid);
        assert!(manager.is_funded(&cid));
        manager.mark_unfunded(&cid);
        assert!(!manager.is_funded(&cid));
    }

    #[test]
    fn test_no_eviction_below_threshold() {
        let (store, dir) = make_store();
        let config = EvictionConfig {
            max_storage_bytes: 1_000_000,
            enable_eviction: true,
        };
        let mut manager = EvictionManager::new(&config);
        let evicted = manager.check_and_evict(&store);
        assert!(evicted.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_eviction_disabled() {
        let (store, dir) = make_store();
        let config = EvictionConfig {
            max_storage_bytes: 100,
            enable_eviction: false,
        };
        let mut manager = EvictionManager::new(&config);
        let evicted = manager.check_and_evict(&store);
        assert!(evicted.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_lru_ordering_oldest_evicted_first() {
        let (store, dir) = make_store();

        // Create three CIDs with content on disk
        let cid_old = make_cid(1);
        let cid_mid = make_cid(2);
        let cid_new = make_cid(3);

        // Write some data for each CID
        for (i, cid) in [&cid_old, &cid_mid, &cid_new].iter().enumerate() {
            let piece_id = [i as u8; 32];
            store.store_piece(cid, 0, &piece_id, &vec![0u8; 1000], &[1, 2, 3]).unwrap();
        }

        let config = EvictionConfig {
            // Set max very low to trigger pressure
            max_storage_bytes: 100,
            enable_eviction: true,
        };
        let mut manager = EvictionManager::new(&config);

        // Track with staggered access times
        let now = Instant::now();
        manager.track(cid_old, ContentStatus {
            funded: false,
            last_accessed: now - std::time::Duration::from_secs(300),
            last_pdp_pass: None,
        });
        manager.track(cid_mid, ContentStatus {
            funded: false,
            last_accessed: now - std::time::Duration::from_secs(100),
            last_pdp_pass: None,
        });
        manager.track(cid_new, ContentStatus {
            funded: false,
            last_accessed: now,
            last_pdp_pass: None,
        });

        let evicted = manager.check_and_evict(&store);

        // Should evict oldest first
        assert!(!evicted.is_empty());
        assert_eq!(evicted[0].0, cid_old, "oldest should be evicted first");
        if evicted.len() > 1 {
            assert_eq!(evicted[1].0, cid_mid, "middle should be evicted second");
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_funded_content_protected() {
        let (store, dir) = make_store();

        let cid_funded = make_cid(1);
        let cid_free = make_cid(2);

        // Write data
        for (i, cid) in [&cid_funded, &cid_free].iter().enumerate() {
            let piece_id = [i as u8; 32];
            store.store_piece(cid, 0, &piece_id, &vec![0u8; 1000], &[1, 2, 3]).unwrap();
        }

        let config = EvictionConfig {
            max_storage_bytes: 100, // very low → pressure
            enable_eviction: true,
        };
        let mut manager = EvictionManager::new(&config);

        let old = Instant::now() - std::time::Duration::from_secs(1000);
        // Funded content is older but should NOT be evicted
        manager.track(cid_funded, ContentStatus {
            funded: true,
            last_accessed: old,
            last_pdp_pass: None,
        });
        manager.track(cid_free, ContentStatus {
            funded: false,
            last_accessed: Instant::now(),
            last_pdp_pass: None,
        });

        let evicted = manager.check_and_evict(&store);

        // Only free content should be evicted
        for (cid, _) in &evicted {
            assert_ne!(*cid, cid_funded, "funded content must not be evicted");
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_retirement_detection() {
        let config = EvictionConfig::default();
        let manager = EvictionManager::new(&config);
        let cid = make_cid(1);

        // Segment 0 has rank 50, k=100 → unrecoverable
        let mut ranks = HashMap::new();
        ranks.insert(0, 50);
        ranks.insert(1, 120);

        let reason = manager.check_retirement(&cid, 100, &ranks);
        assert!(reason.is_some());
        match reason.unwrap() {
            EvictionReason::Unrecoverable { segment, rank, k } => {
                assert_eq!(segment, 0);
                assert_eq!(rank, 50);
                assert_eq!(k, 100);
            }
            _ => panic!("expected Unrecoverable"),
        }
    }

    #[test]
    fn test_retirement_healthy_content() {
        let config = EvictionConfig::default();
        let manager = EvictionManager::new(&config);
        let cid = make_cid(1);

        // All segments healthy
        let mut ranks = HashMap::new();
        ranks.insert(0, 100);
        ranks.insert(1, 150);

        let reason = manager.check_retirement(&cid, 100, &ranks);
        assert!(reason.is_none());
    }

    #[test]
    fn test_storage_pressure_threshold() {
        let (store, dir) = make_store();

        // No data → no pressure
        let config = EvictionConfig {
            max_storage_bytes: 1_000_000,
            enable_eviction: true,
        };
        let manager = EvictionManager::new(&config);
        assert!(!manager.is_under_pressure(&store));

        // With max_storage_bytes = 0 → no pressure (disabled)
        let config_zero = EvictionConfig {
            max_storage_bytes: 0,
            enable_eviction: true,
        };
        let manager_zero = EvictionManager::new(&config_zero);
        assert!(!manager_zero.is_under_pressure(&store));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_record_access_updates_time() {
        let config = EvictionConfig::default();
        let mut manager = EvictionManager::new(&config);
        let cid = make_cid(1);

        let old = Instant::now() - std::time::Duration::from_secs(1000);
        manager.track(cid, ContentStatus {
            funded: false,
            last_accessed: old,
            last_pdp_pass: None,
        });

        let before = manager.status(&cid).unwrap().last_accessed;
        std::thread::sleep(std::time::Duration::from_millis(10));
        manager.record_access(&cid);
        let after = manager.status(&cid).unwrap().last_accessed;

        assert!(after > before);
    }

    #[test]
    fn test_full_maintenance_cycle() {
        let (store, dir) = make_store();
        let config = EvictionConfig {
            max_storage_bytes: 1_000_000,
            enable_eviction: true,
        };
        let mut manager = EvictionManager::new(&config);

        let cid = make_cid(1);
        manager.track(cid, ContentStatus::new_free());

        // Segment rank < k → should retire
        let mut k_map = HashMap::new();
        k_map.insert(cid, 100usize);
        let mut ranks_map = HashMap::new();
        let mut seg_ranks = HashMap::new();
        seg_ranks.insert(0u32, 50usize);
        ranks_map.insert(cid, seg_ranks);

        let result = manager.run_maintenance(&store, &k_map, &ranks_map);
        assert_eq!(result.retired.len(), 1);
        assert_eq!(result.retired[0].0, cid);

        std::fs::remove_dir_all(&dir).ok();
    }
}
