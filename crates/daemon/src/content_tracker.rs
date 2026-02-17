//! Content lifecycle tracker
//!
//! Tracks the lifecycle stage of content: stored → announced → distributing → distributed.
//! Uses piece counts per segment instead of shard counts.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use datacraft_core::{ContentManifest, ContentId};
use datacraft_store::FsStore;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Default re-announcement threshold in seconds (20 minutes).
pub const DEFAULT_REANNOUNCE_THRESHOLD_SECS: u64 = 1200;

/// Role this node plays for a piece of content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContentRole {
    Publisher,
    StorageProvider,
}

impl Default for ContentRole {
    fn default() -> Self {
        Self::Publisher
    }
}

/// Lifecycle stage of content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContentStage {
    Stored,
    Announced,
    Distributing,
    Distributed,
    Degraded,
}

impl std::fmt::Display for ContentStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stored => write!(f, "stored"),
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
    /// Total pieces stored locally (across all segments).
    pub local_pieces: usize,
    /// Number of segments.
    pub segment_count: usize,
    /// k per full segment (pieces needed for reconstruction).
    pub k: usize,
    pub provider_count: usize,
    pub last_announced: Option<u64>,
    pub last_checked: Option<u64>,
    pub created_at: u64,
    pub encrypted: bool,
    pub name: String,
    pub size: u64,
    #[serde(default)]
    pub role: ContentRole,
    /// Whether the initial 2-per-peer push has been completed.
    #[serde(default)]
    pub initial_push_done: bool,
}

/// Persistent content lifecycle tracker.
pub struct ContentTracker {
    states: HashMap<ContentId, ContentState>,
    path: PathBuf,
    reannounce_threshold_secs: u64,
    /// In-memory provider tracking per CID (from DHT discovery / gossipsub).
    /// Not persisted — rebuilt at runtime from network events.
    providers: HashMap<ContentId, HashSet<PeerId>>,
}

impl ContentTracker {
    pub fn new(data_dir: &std::path::Path) -> Self {
        Self::with_threshold(data_dir, DEFAULT_REANNOUNCE_THRESHOLD_SECS)
    }

    pub fn with_threshold(data_dir: &std::path::Path, reannounce_threshold_secs: u64) -> Self {
        let path = data_dir.join("content_tracker.json");
        let mut states = Self::load_from(&path).unwrap_or_default();
        debug!("ContentTracker loaded {} entries from {:?}", states.len(), path);
        Self { states, path, reannounce_threshold_secs, providers: HashMap::new() }
    }

    /// Track newly published content.
    pub fn track_published(
        &mut self,
        content_id: ContentId,
        manifest: &ContentManifest,
        name: String,
        encrypted: bool,
    ) {
        let k = manifest.k();
        // Count initial pieces: k + parity per segment
        let initial_parity = 20; // default
        let local_pieces = manifest.segment_count * (k + initial_parity);
        let now = now_secs();

        let state = ContentState {
            content_id,
            stage: ContentStage::Stored,
            local_pieces,
            segment_count: manifest.segment_count,
            k,
            provider_count: 0,
            last_announced: None,
            last_checked: None,
            created_at: now,
            encrypted,
            name,
            size: manifest.total_size,
            role: ContentRole::Publisher,
            initial_push_done: false,
        };

        self.states.insert(content_id, state);
        self.save();
    }

    /// Mark initial push as complete for a CID.
    pub fn mark_initial_push_done(&mut self, content_id: &ContentId) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.initial_push_done = true;
            self.save();
        }
    }

    /// Track content this node is storing on behalf of the network.
    pub fn track_stored(
        &mut self,
        content_id: ContentId,
        manifest: &ContentManifest,
    ) {
        if let Some(existing) = self.states.get(&content_id) {
            if existing.role == ContentRole::Publisher {
                return;
            }
        }

        let k = manifest.k();
        let now = now_secs();

        let state = ContentState {
            content_id,
            stage: ContentStage::Distributed,
            local_pieces: 0,
            segment_count: manifest.segment_count,
            k,
            provider_count: 2, // At minimum: publisher + self
            last_announced: None,
            last_checked: None,
            created_at: now,
            encrypted: false,
            name: String::new(),
            size: manifest.total_size,
            role: ContentRole::StorageProvider,
            initial_push_done: true, // storage providers don't do initial push
        };

        self.states.insert(content_id, state);
        self.save();
    }

    /// Increment local piece count (called when a piece is received via push).
    pub fn increment_local_pieces(&mut self, content_id: &ContentId) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.local_pieces += 1;
        }
    }

    /// Decrement local piece count by the given amount (called after pushing pieces out).
    pub fn decrement_local_pieces(&mut self, content_id: &ContentId, count: usize) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.local_pieces = state.local_pieces.saturating_sub(count);
            self.save();
        }
    }

    pub fn mark_announced(&mut self, content_id: &ContentId) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.stage = ContentStage::Announced;
            state.last_announced = Some(now_secs());
            self.save();
        }
    }

    pub fn update_provider_count(&mut self, content_id: &ContentId, count: usize) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.provider_count = count;
            state.last_checked = Some(now_secs());
            self.save();
        }
    }

    /// Record a provider PeerId for a CID (from DHT discovery or gossipsub).
    pub fn add_provider(&mut self, content_id: &ContentId, peer: PeerId) {
        let set = self.providers.entry(*content_id).or_default();
        set.insert(peer);
        // Keep provider_count in sync with actual known providers
        if let Some(state) = self.states.get_mut(content_id) {
            if set.len() > state.provider_count {
                state.provider_count = set.len();
            }
        }
    }

    /// Get known provider PeerIds for a CID.
    pub fn get_providers(&self, content_id: &ContentId) -> Vec<PeerId> {
        self.providers.get(content_id)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn mark_degraded(&mut self, content_id: &ContentId) {
        if let Some(state) = self.states.get_mut(content_id) {
            state.stage = ContentStage::Degraded;
            self.save();
        }
    }

    pub fn get(&self, content_id: &ContentId) -> Option<&ContentState> {
        self.states.get(content_id)
    }

    pub fn list(&self) -> Vec<ContentState> {
        self.states.values().cloned().collect()
    }

    pub fn remove(&mut self, content_id: &ContentId) {
        self.states.remove(content_id);
        self.save();
    }

    pub fn needs_announcement(&self) -> Vec<ContentId> {
        let now = now_secs();
        self.states
            .values()
            .filter(|s| {
                s.stage == ContentStage::Stored
                    || match s.last_announced {
                        Some(ts) => now.saturating_sub(ts) > self.reannounce_threshold_secs,
                        None => true,
                    }
            })
            .map(|s| s.content_id)
            .collect()
    }

    /// CIDs that need initial push (publisher, not yet pushed).
    pub fn needs_distribution(&self) -> Vec<ContentId> {
        self.states
            .values()
            .filter(|s| s.role == ContentRole::Publisher && !s.initial_push_done && s.local_pieces > 0)
            .map(|s| s.content_id)
            .collect()
    }

    /// CIDs where this node holds > 2 pieces and initial push is done.
    /// Used for pressure equalization (Distribution Function 2).
    pub fn needs_equalization(&self) -> Vec<ContentId> {
        self.states
            .values()
            .filter(|s| s.initial_push_done && s.local_pieces > 2)
            .map(|s| s.content_id)
            .collect()
    }

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
                continue;
            }
            match store.get_manifest(&cid) {
                Ok(manifest) => {
                    // Count actual pieces in store
                    let mut local_pieces = 0;
                    for seg in 0..manifest.segment_count as u32 {
                        local_pieces += store.list_pieces(&cid, seg).unwrap_or_default().len();
                    }
                    let state = ContentState {
                        content_id: cid,
                        stage: ContentStage::Stored,
                        local_pieces,
                        segment_count: manifest.segment_count,
                        k: manifest.k(),
                        provider_count: 0,
                        last_announced: None,
                        last_checked: None,
                        created_at: now_secs(),
                        encrypted: false,
                        name: cid.to_hex()[..12].to_string(),
                        size: manifest.total_size,
                        role: ContentRole::Publisher,
                        initial_push_done: false,
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

    pub fn status_summary(&self, content_id: &ContentId) -> Option<(ContentState, String)> {
        let state = self.states.get(content_id)?;
        let summary = match state.stage {
            ContentStage::Stored => {
                "Stored locally — needs DHT announcement".to_string()
            }
            ContentStage::Announced => {
                format!("Announced — {} local pieces, {} providers", state.local_pieces, state.provider_count)
            }
            ContentStage::Distributing => {
                format!("Distributing — {} local pieces ({} providers)", state.local_pieces, state.provider_count)
            }
            ContentStage::Distributed => {
                format!("Fully distributed — {} providers ✓", state.provider_count)
            }
            ContentStage::Degraded => {
                format!("DEGRADED — insufficient pieces, needs repair")
            }
        };
        Some((state.clone(), summary))
    }

    /// Sync tracker state with actual disk contents.
    /// Resets stale remote state and validates local piece counts.
    pub fn sync_with_store(&mut self, store: &FsStore) {
        let cids: Vec<ContentId> = self.states.keys().copied().collect();
        for cid in cids {
            let actual_pieces = {
                let manifest = match store.get_manifest(&cid) {
                    Ok(m) => m,
                    Err(_) => {
                        warn!("Tracker has {} but no manifest on disk, removing", cid);
                        self.states.remove(&cid);
                        continue;
                    }
                };
                let mut count = 0usize;
                for seg in 0..manifest.segment_count as u32 {
                    count += store.list_pieces(&cid, seg).unwrap_or_default().len();
                }
                count
            };

            if let Some(state) = self.states.get_mut(&cid) {
                if state.local_pieces != actual_pieces {
                    info!("Sync: {} local_pieces {} -> {} (disk)", cid, state.local_pieces, actual_pieces);
                    state.local_pieces = actual_pieces;
                    if state.role == ContentRole::Publisher {
                        state.initial_push_done = false;
                    }
                }
            }
        }
        self.save();
        info!("Content tracker synced with store");
    }

    pub fn save(&self) {
        let entries: Vec<&ContentState> = self.states.values().collect();
        match serde_json::to_string_pretty(&entries) {
            Ok(json) => {
                // Backup current file before writing
                if self.path.exists() {
                    let bak = self.path.with_extension("json.bak");
                    if let Err(e) = std::fs::copy(&self.path, &bak) {
                        debug!("Failed to create backup: {}", e);
                    }
                }
                // Atomic write: tmp + rename
                let tmp = self.path.with_extension("json.tmp");
                if let Err(e) = std::fs::write(&tmp, &json) {
                    warn!("Failed to write content tracker tmp: {}", e);
                    return;
                }
                if let Err(e) = std::fs::rename(&tmp, &self.path) {
                    warn!("Failed to rename content tracker tmp: {}", e);
                    return;
                }
                // fsync
                if let Ok(f) = std::fs::File::open(&self.path) {
                    if let Err(e) = f.sync_all() {
                        debug!("Failed to fsync content tracker: {}", e);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to serialize content tracker: {}", e);
            }
        }
    }

    fn load_from(path: &std::path::Path) -> Option<HashMap<ContentId, ContentState>> {
        if let Some(map) = Self::try_parse_file(path) {
            return Some(map);
        }
        let bak = path.with_extension("json.bak");
        if bak.exists() {
            warn!("Primary content tracker corrupt, trying backup");
            if let Some(map) = Self::try_parse_file(&bak) {
                if let Err(e) = std::fs::copy(&bak, path) {
                    warn!("Failed to restore backup: {}", e);
                }
                return Some(map);
            }
            warn!("Backup also corrupt");
        }
        None
    }

    fn try_parse_file(path: &std::path::Path) -> Option<HashMap<ContentId, ContentState>> {
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

    fn test_manifest(content_id: ContentId) -> ContentManifest {
        ContentManifest {
            content_id,
            content_hash: content_id.0,
            segment_size: 10_240_000,
            piece_size: 102_400,
            segment_count: 1,
            total_size: 100_000,
            creator: String::new(),
            signature: vec![],
        }
    }

    fn temp_dir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "content-tracker-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed),
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
        assert_eq!(state.stage, ContentStage::Stored);
        assert!(state.local_pieces > 0);
        assert_eq!(state.name, "test.txt");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_mark_announced() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"announce test");
        tracker.track_published(cid, &test_manifest(cid), "file.bin".into(), false);
        tracker.mark_announced(&cid);

        let state = tracker.get(&cid).unwrap();
        assert_eq!(state.stage, ContentStage::Announced);
        assert!(state.last_announced.is_some());
    }

    #[test]
    fn test_persistence_roundtrip() {
        let dir = temp_dir();
        let cid = ContentId::from_bytes(b"persist test");

        {
            let mut tracker = ContentTracker::new(&dir);
            tracker.track_published(cid, &test_manifest(cid), "persist.txt".into(), true);
            tracker.mark_announced(&cid);
        }

        let tracker = ContentTracker::new(&dir);
        let state = tracker.get(&cid).unwrap();
        assert_eq!(state.stage, ContentStage::Announced);
        assert_eq!(state.name, "persist.txt");
        assert!(state.encrypted);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_needs_announcement() {
        let dir = temp_dir();
        let mut tracker = ContentTracker::new(&dir);
        let cid = ContentId::from_bytes(b"needs announce");
        tracker.track_published(cid, &test_manifest(cid), "file.bin".into(), false);

        assert!(tracker.needs_announcement().contains(&cid));

        tracker.mark_announced(&cid);
        assert!(!tracker.needs_announcement().contains(&cid));

        std::fs::remove_dir_all(&dir).ok();
    }
}
