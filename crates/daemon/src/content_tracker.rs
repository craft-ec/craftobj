//! Content lifecycle tracker
//!
//! Tracks the lifecycle stage of published content: chunked → announced → distributing → distributed.
//! Persists state to disk so re-announcement works across daemon restarts.

use std::collections::HashMap;
use std::path::PathBuf;

use datacraft_core::{ChunkManifest, ContentId};
use datacraft_store::FsStore;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Default re-announcement threshold in seconds (20 minutes).
pub const DEFAULT_REANNOUNCE_THRESHOLD_SECS: u64 = 1200;

/// Lifecycle stage of content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContentStage {
    /// Locally stored, not yet announced to DHT.
    Chunked,
    /// DHT provider record published.
    Announced,
    /// Shards being pushed to storage peers.
    Distributing,
    /// Minimum shard redundancy achieved.
    Distributed,
    /// Some shards lost, needs healing.
    Degraded,
}

impl std::fmt::Display for ContentStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Chunked => write!(f, "chunked"),
            Self::Announced => write!(f, "announced"),
            Self::Distributing => write!(f, "distributing"),
            Self::Distributed => write!(f, "distributed"),
            Self::Degraded => write!(f, "degraded"),
        }
    }
}

/// State of a tracked content item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentState {
    pub content_id: ContentId,
    pub stage: ContentStage,
    pub total_shards: usize,
    pub local_shards: usize,
    pub remote_shards: usize,
    pub provider_count: usize,
    pub last_announced: Option<u64>,
    pub last_checked: Option<u64>,
    pub created_at: u64,
    pub encrypted: bool,
    pub name: String,
    pub size: u64,
}

/// Persistent content lifecycle tracker.
pub struct ContentTracker {
    states: HashMap<ContentId, ContentState>,
    path: PathBuf,
    reannounce_threshold_secs: u64,
}

impl ContentTracker {
    /// Create a new tracker, loading persisted state from disk.
    pub fn new(data_dir: &std::path::Path) -> Self {
        Self::with_threshold(data_dir, DEFAULT_REANNOUNCE_THRESHOLD_SECS)
    }

    /// Create a new tracker with a custom reannounce threshold.
    pub fn with_threshold(data_dir: &std::path::Path, reannounce_threshold_secs: u64) -> Self {
        let path = data_dir.join("content_tracker.json");
        let states = Self::load_from(&path).unwrap_or_default();
        debug!("ContentTracker loaded {} entries from {:?}", states.len(), path);
        Self { states, path, reannounce_threshold_secs }
    }

    /// Track newly published content (initial stage = Chunked).
    pub fn track_published(
        &mut self,
        content_id: ContentId,
        manifest: &ChunkManifest,
        name: String,
        encrypted: bool,
    ) {
        let total_shards = manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards;
        let local_shards = total_shards; // just published, all shards are local
        let now = now_secs();

        let state = ContentState {
            content_id,
            stage: ContentStage::Chunked,
            total_shards,
            local_shards,
            remote_shards: 0,
            provider_count: 0,
            last_announced: None,
            last_checked: None,
            created_at: now,
            encrypted,
            name,
            size: manifest.content_size,
        };

        self.states.insert(content_id, state);
        self.save();
    }

    /// Mark content as announced to DHT.
    pub fn mark_announced(&mut self, content_id: &ContentId) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.stage = ContentStage::Announced;
            state.last_announced = Some(now_secs());
            self.save();
        }
    }

    /// Update remote shard progress; transitions stage to Distributing or Distributed.
    pub fn update_shard_progress(&mut self, content_id: &ContentId, remote_shards: usize) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.remote_shards = remote_shards;
            if remote_shards >= state.total_shards {
                state.stage = ContentStage::Distributed;
            } else if remote_shards > 0 && state.stage != ContentStage::Degraded {
                state.stage = ContentStage::Distributing;
            }
            state.last_checked = Some(now_secs());
            self.save();
        }
    }

    /// Update known provider count from DHT resolution.
    pub fn update_provider_count(&mut self, content_id: &ContentId, count: usize) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.provider_count = count;
            state.last_checked = Some(now_secs());
            self.save();
        }
    }

    /// Mark content as degraded (health check failed).
    pub fn mark_degraded(&mut self, content_id: &ContentId) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.stage = ContentStage::Degraded;
            self.save();
        }
    }

    /// Get state for a specific content ID.
    pub fn get(&self, content_id: &ContentId) -> Option<&ContentState> {
        self.states.get(content_id)
    }

    /// List all tracked content.
    pub fn list(&self) -> Vec<ContentState> {
        self.states.values().cloned().collect()
    }

    /// Remove tracking for a content ID.
    pub fn remove(&mut self, content_id: &ContentId) {
        self.states.remove(content_id);
        self.save();
    }

    /// Content that needs (re-)announcement: stage is Chunked, or announced > threshold ago.
    pub fn needs_announcement(&self) -> Vec<ContentId> {
        let now = now_secs();
        self.states
            .values()
            .filter(|s| {
                s.stage == ContentStage::Chunked
                    || match s.last_announced {
                        Some(ts) => now.saturating_sub(ts) > self.reannounce_threshold_secs,
                        None => true,
                    }
            })
            .map(|s| s.content_id)
            .collect()
    }

    /// Content that has local shards but insufficient remote distribution.
    pub fn needs_distribution(&self) -> Vec<ContentId> {
        self.states
            .values()
            .filter(|s| s.local_shards > 0 && s.remote_shards < s.total_shards)
            .map(|s| s.content_id)
            .collect()
    }

    /// Import existing content from the store that isn't already tracked.
    /// Called on startup to catch content published before the tracker existed.
    pub fn import_from_store(&mut self, store: &FsStore) -> usize {
        let existing = match store.list_content() {
            Ok(ids) => ids,
            Err(e) => {
                warn!("Failed to list store content for import: {}", e);
                return 0;
            }
        };

        let mut imported = 0;
        for cid in existing {
            if self.states.contains_key(&cid) {
                continue; // already tracked
            }
            match store.get_manifest(&cid) {
                Ok(manifest) => {
                    let total_shards = manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards;
                    let state = ContentState {
                        content_id: cid,
                        stage: ContentStage::Chunked, // treat as fresh — needs announcement
                        total_shards,
                        local_shards: total_shards,
                        remote_shards: 0,
                        provider_count: 0,
                        last_announced: None,
                        last_checked: None,
                        created_at: now_secs(),
                        encrypted: false, // can't tell from manifest alone
                        name: cid.to_hex()[..12].to_string(),
                        size: manifest.content_size,
                    };
                    self.states.insert(cid, state);
                    imported += 1;
                }
                Err(e) => {
                    debug!("Skipping import of {}: {}", cid, e);
                }
            }
        }

        if imported > 0 {
            info!("Imported {} existing content items into tracker", imported);
            self.save();
        }

        imported
    }

    /// Build a human-readable status summary for a content item.
    pub fn status_summary(&self, content_id: &ContentId) -> Option<(ContentState, String)> {
        let state = self.states.get(content_id)?;
        let summary = match state.stage {
            ContentStage::Chunked => {
                "Stored locally only — needs to be announced to DHT so storage nodes can find it".to_string()
            }
            ContentStage::Announced => {
                if state.remote_shards == 0 {
                    format!("Announced to DHT — waiting for distribution to storage nodes ({} local shards ready to push)", state.local_shards)
                } else {
                    format!("Announced — {}/{} shards distributed, needs more storage nodes", state.remote_shards, state.total_shards)
                }
            }
            ContentStage::Distributing => {
                format!("Distributing — {}/{} shards pushed to storage nodes ({} providers)", state.remote_shards, state.total_shards, state.provider_count)
            }
            ContentStage::Distributed => {
                format!("Fully distributed — {}/{} shards on {} storage nodes ✓", state.remote_shards, state.total_shards, state.provider_count)
            }
            ContentStage::Degraded => {
                format!("DEGRADED — only {}/{} shards available, needs self-healing", state.remote_shards, state.total_shards)
            }
        };
        Some((state.clone(), summary))
    }

    /// Persist state to disk.
    pub fn save(&self) {
        let entries: Vec<&ContentState> = self.states.values().collect();
        match serde_json::to_string_pretty(&entries) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&self.path, json) {
                    warn!("Failed to save content tracker: {}", e);
                }
            }
            Err(e) => {
                warn!("Failed to serialize content tracker: {}", e);
            }
        }
    }

    /// Load state from disk.
    fn load_from(path: &std::path::Path) -> Option<HashMap<ContentId, ContentState>> {
        let data = std::fs::read_to_string(path).ok()?;
        let entries: Vec<ContentState> = serde_json::from_str(&data).ok()?;
        let mut map = HashMap::new();
        for entry in entries {
            map.insert(entry.content_id, entry);
        }
        Some(map)
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datacraft_core::{default_erasure_config, ContentId};

    fn test_manifest(content_id: ContentId) -> ChunkManifest {
        let config = default_erasure_config();
        ChunkManifest {
            content_id,
            content_hash: content_id.0,
            k: config.data_shards,
            chunk_size: config.chunk_size,
            chunk_count: 2,
            erasure_config: config,
            content_size: 100_000,
            creator: String::new(),
            signature: vec![],
        }
    }

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "content-tracker-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_track_published() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"test content");
        let manifest = test_manifest(cid);

        tracker.track_published(cid, &manifest, "test.txt".into(), false);

        let state = tracker.get(&cid).unwrap();
        assert_eq!(state.stage, ContentStage::Chunked);
        assert_eq!(state.total_shards, 8); // 4 data + 4 parity
        assert_eq!(state.local_shards, 8);
        assert_eq!(state.remote_shards, 0);
        assert_eq!(state.name, "test.txt");
        assert!(!state.encrypted);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_mark_announced() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"announce test");
        let manifest = test_manifest(cid);

        tracker.track_published(cid, &manifest, "file.bin".into(), false);
        tracker.mark_announced(&cid);

        let state = tracker.get(&cid).unwrap();
        assert_eq!(state.stage, ContentStage::Announced);
        assert!(state.last_announced.is_some());
    }

    #[test]
    fn test_shard_progress() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"shard progress");
        let manifest = test_manifest(cid);

        tracker.track_published(cid, &manifest, "file.bin".into(), false);
        tracker.mark_announced(&cid);

        // Partial distribution
        tracker.update_shard_progress(&cid, 4);
        assert_eq!(tracker.get(&cid).unwrap().stage, ContentStage::Distributing);

        // Full distribution
        tracker.update_shard_progress(&cid, 8);
        assert_eq!(tracker.get(&cid).unwrap().stage, ContentStage::Distributed);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_needs_announcement() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"needs announce");
        let manifest = test_manifest(cid);

        tracker.track_published(cid, &manifest, "file.bin".into(), false);

        let needs = tracker.needs_announcement();
        assert!(needs.contains(&cid));

        tracker.mark_announced(&cid);
        // Just announced, shouldn't need re-announcement yet
        let needs = tracker.needs_announcement();
        assert!(!needs.contains(&cid));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_needs_distribution() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"needs dist");
        let manifest = test_manifest(cid);

        tracker.track_published(cid, &manifest, "file.bin".into(), false);

        let needs = tracker.needs_distribution();
        assert!(needs.contains(&cid));

        tracker.update_shard_progress(&cid, 8);
        let needs = tracker.needs_distribution();
        assert!(!needs.contains(&cid));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_persistence_roundtrip() {
        let dir = temp_dir();
        let cid = ContentId::from_bytes(b"persist test");
        let manifest = test_manifest(cid);

        {
            let mut tracker = ContentTracker::new(&dir);
            tracker.track_published(cid, &manifest, "persist.txt".into(), true);
            tracker.mark_announced(&cid);
        }

        // Reload from disk
        let tracker = ContentTracker::new(&dir);
        let state = tracker.get(&cid).unwrap();
        assert_eq!(state.stage, ContentStage::Announced);
        assert_eq!(state.name, "persist.txt");
        assert!(state.encrypted);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);

        let cid1 = ContentId::from_bytes(b"content 1");
        let cid2 = ContentId::from_bytes(b"content 2");
        tracker.track_published(cid1, &test_manifest(cid1), "a.txt".into(), false);
        tracker.track_published(cid2, &test_manifest(cid2), "b.txt".into(), true);

        assert_eq!(tracker.list().len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_remove() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"remove me");
        tracker.track_published(cid, &test_manifest(cid), "gone.txt".into(), false);

        assert!(tracker.get(&cid).is_some());
        tracker.remove(&cid);
        assert!(tracker.get(&cid).is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_mark_degraded() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"degraded");
        tracker.track_published(cid, &test_manifest(cid), "bad.txt".into(), false);
        tracker.mark_degraded(&cid);

        assert_eq!(tracker.get(&cid).unwrap().stage, ContentStage::Degraded);

        std::fs::remove_dir_all(&dir).ok();
    }
}
