//! DataCraft Store
//!
//! Filesystem content-addressed storage for DataCraft.
//!
//! Layout:
//! ```text
//! <data_dir>/
//!   chunks/<cid_hex>/<chunk_index>/<shard_index>
//!   manifests/<cid_hex>.json
//!   pins.json
//! ```

use std::path::{Path, PathBuf};

use datacraft_core::{ChunkManifest, ContentId, DataCraftError, Result};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Content-addressed filesystem store.
pub struct FsStore {
    data_dir: PathBuf,
}

impl FsStore {
    /// Create a new FsStore at the given directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(data_dir.join("chunks"))?;
        std::fs::create_dir_all(data_dir.join("manifests"))?;
        Ok(Self { data_dir })
    }

    /// Store an erasure-coded shard on disk.
    pub fn put_shard(
        &self,
        content_id: &ContentId,
        chunk_index: u32,
        shard_index: u8,
        data: &[u8],
    ) -> Result<()> {
        let dir = self
            .data_dir
            .join("chunks")
            .join(content_id.to_hex())
            .join(chunk_index.to_string());
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(shard_index.to_string());
        std::fs::write(&path, data)?;
        debug!(
            "Stored shard {}/{}/{} ({} bytes)",
            content_id,
            chunk_index,
            shard_index,
            data.len()
        );
        Ok(())
    }

    /// Read a shard from disk.
    pub fn get_shard(
        &self,
        content_id: &ContentId,
        chunk_index: u32,
        shard_index: u8,
    ) -> Result<Vec<u8>> {
        let path = self
            .data_dir
            .join("chunks")
            .join(content_id.to_hex())
            .join(chunk_index.to_string())
            .join(shard_index.to_string());
        if !path.exists() {
            return Err(DataCraftError::ContentNotFound(format!(
                "shard {}/{}/{}",
                content_id, chunk_index, shard_index
            )));
        }
        Ok(std::fs::read(&path)?)
    }

    /// Check if a shard exists.
    pub fn has_shard(
        &self,
        content_id: &ContentId,
        chunk_index: u32,
        shard_index: u8,
    ) -> bool {
        self.data_dir
            .join("chunks")
            .join(content_id.to_hex())
            .join(chunk_index.to_string())
            .join(shard_index.to_string())
            .exists()
    }

    /// Store a chunk manifest.
    pub fn put_manifest(&self, manifest: &ChunkManifest) -> Result<()> {
        let path = self
            .data_dir
            .join("manifests")
            .join(format!("{}.json", manifest.content_id.to_hex()));
        let json = serde_json::to_string_pretty(manifest)
            .map_err(|e| DataCraftError::ManifestError(e.to_string()))?;
        std::fs::write(&path, json)?;
        debug!("Stored manifest for {}", manifest.content_id);
        Ok(())
    }

    /// Retrieve a chunk manifest.
    pub fn get_manifest(&self, content_id: &ContentId) -> Result<ChunkManifest> {
        let path = self
            .data_dir
            .join("manifests")
            .join(format!("{}.json", content_id.to_hex()));
        if !path.exists() {
            return Err(DataCraftError::ContentNotFound(format!(
                "manifest for {}",
                content_id
            )));
        }
        let json = std::fs::read_to_string(&path)?;
        serde_json::from_str(&json)
            .map_err(|e| DataCraftError::ManifestError(e.to_string()))
    }

    /// Check if a manifest exists.
    pub fn has_manifest(&self, content_id: &ContentId) -> bool {
        self.data_dir
            .join("manifests")
            .join(format!("{}.json", content_id.to_hex()))
            .exists()
    }

    /// List all content IDs that have manifests stored.
    pub fn list_content(&self) -> Result<Vec<ContentId>> {
        let manifest_dir = self.data_dir.join("manifests");
        let mut result = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&manifest_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(hex) = name.strip_suffix(".json") {
                    if let Ok(cid) = ContentId::from_hex(hex) {
                        result.push(cid);
                    }
                }
            }
        }
        Ok(result)
    }

    /// Delete all data (chunks + manifest) for a content ID.
    pub fn delete_content(&self, content_id: &ContentId) -> Result<()> {
        let chunk_dir = self.data_dir.join("chunks").join(content_id.to_hex());
        if chunk_dir.exists() {
            std::fs::remove_dir_all(&chunk_dir)?;
        }
        let manifest_path = self
            .data_dir
            .join("manifests")
            .join(format!("{}.json", content_id.to_hex()));
        if manifest_path.exists() {
            std::fs::remove_file(&manifest_path)?;
        }
        debug!("Deleted content {}", content_id);
        Ok(())
    }

    /// Returns the base data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Return the maximum shard index stored locally for a given content/chunk pair.
    /// Returns `None` if no shards are stored for this chunk.
    pub fn max_shard_index(&self, content_id: &ContentId, chunk_index: u32) -> Option<u8> {
        let chunk_dir = self
            .data_dir
            .join("chunks")
            .join(content_id.to_hex())
            .join(chunk_index.to_string());
        if !chunk_dir.exists() {
            return None;
        }
        let mut max: Option<u8> = None;
        if let Ok(entries) = std::fs::read_dir(&chunk_dir) {
            for entry in entries.flatten() {
                if let Ok(idx) = entry.file_name().to_string_lossy().parse::<u8>() {
                    max = Some(max.map_or(idx, |m: u8| m.max(idx)));
                }
            }
        }
        max
    }

    /// Return the maximum shard index across all chunks for a content ID.
    /// Returns `None` if no shards exist.
    pub fn max_shard_index_for_content(&self, content_id: &ContentId) -> Option<u8> {
        let cid_dir = self.data_dir.join("chunks").join(content_id.to_hex());
        if !cid_dir.exists() {
            return None;
        }
        let mut global_max: Option<u8> = None;
        if let Ok(entries) = std::fs::read_dir(&cid_dir) {
            for entry in entries.flatten() {
                if let Ok(chunk_idx) = entry.file_name().to_string_lossy().parse::<u32>() {
                    if let Some(m) = self.max_shard_index(content_id, chunk_idx) {
                        global_max = Some(global_max.map_or(m, |g: u8| g.max(m)));
                    }
                }
            }
        }
        global_max
    }
}

/// Manages pinned content IDs.
///
/// Pinned content is never garbage collected.
pub struct PinManager {
    pins_path: PathBuf,
    pins: PinState,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct PinState {
    pinned: Vec<String>,
}

impl PinManager {
    /// Create or load pin state from a directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        let pins_path = data_dir.join("pins.json");
        let pins = if pins_path.exists() {
            let json = std::fs::read_to_string(&pins_path)?;
            serde_json::from_str(&json).unwrap_or_default()
        } else {
            PinState::default()
        };
        Ok(Self { pins_path, pins })
    }

    /// Pin a content ID (mark for persistent storage).
    pub fn pin(&mut self, content_id: &ContentId) -> Result<()> {
        let hex = content_id.to_hex();
        if !self.pins.pinned.contains(&hex) {
            self.pins.pinned.push(hex);
            self.save()?;
        }
        Ok(())
    }

    /// Unpin a content ID (mark for GC).
    pub fn unpin(&mut self, content_id: &ContentId) -> Result<()> {
        let hex = content_id.to_hex();
        self.pins.pinned.retain(|p| p != &hex);
        self.save()?;
        Ok(())
    }

    /// Check if a content ID is pinned.
    pub fn is_pinned(&self, content_id: &ContentId) -> bool {
        let hex = content_id.to_hex();
        self.pins.pinned.contains(&hex)
    }

    /// List all pinned content IDs.
    pub fn list_pinned(&self) -> Vec<ContentId> {
        self.pins
            .pinned
            .iter()
            .filter_map(|hex| ContentId::from_hex(hex).ok())
            .collect()
    }

    fn save(&self) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.pins)
            .map_err(|e| DataCraftError::StorageError(e.to_string()))?;
        std::fs::write(&self.pins_path, json)?;
        Ok(())
    }
}

/// Garbage collector: removes unpinned content when storage exceeds threshold.
pub struct GarbageCollector;

impl GarbageCollector {
    /// Run GC sweep. Removes unpinned content until storage is below `max_bytes`.
    pub fn sweep(
        store: &FsStore,
        pin_manager: &PinManager,
        max_bytes: u64,
    ) -> Result<usize> {
        let all_content = store.list_content()?;
        let mut removed = 0;

        // Calculate current usage (rough estimate from manifest content_size)
        let mut total: u64 = 0;
        let mut candidates: Vec<(ContentId, u64)> = Vec::new();
        for cid in &all_content {
            if let Ok(manifest) = store.get_manifest(cid) {
                total += manifest.content_size;
                if !pin_manager.is_pinned(cid) {
                    candidates.push((*cid, manifest.content_size));
                }
            }
        }

        if total <= max_bytes {
            return Ok(0);
        }

        // Remove unpinned content until under threshold
        for (cid, size) in candidates {
            if total <= max_bytes {
                break;
            }
            store.delete_content(&cid)?;
            total -= size;
            removed += 1;
            warn!("GC removed unpinned content {}", cid);
        }

        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datacraft_core::default_erasure_config;

    fn test_dir() -> PathBuf {
        let dir = std::env::temp_dir()
            .join("datacraft-store-test")
            .join(uuid());
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn uuid() -> String {
        use std::time::SystemTime;
        let t = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        format!("{}-{}", t.as_secs(), t.subsec_nanos())
    }

    #[test]
    fn test_shard_roundtrip() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"hello");

        store.put_shard(&cid, 0, 0, b"shard data").unwrap();
        assert!(store.has_shard(&cid, 0, 0));
        assert!(!store.has_shard(&cid, 0, 1));

        let data = store.get_shard(&cid, 0, 0).unwrap();
        assert_eq!(data, b"shard data");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_manifest_roundtrip() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"test content");

        let manifest = ChunkManifest {
            content_id: cid,
            content_hash: cid.0,
            k: 4,
            chunk_size: 65536,
            chunk_count: 1,
            erasure_config: default_erasure_config(),
            content_size: 1000,
            creator: String::new(),
            signature: vec![],
        };

        store.put_manifest(&manifest).unwrap();
        assert!(store.has_manifest(&cid));

        let loaded = store.get_manifest(&cid).unwrap();
        assert_eq!(loaded.content_id, cid);
        assert_eq!(loaded.content_size, 1000);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_and_delete_content() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();

        let cid1 = ContentId::from_bytes(b"file1");
        let cid2 = ContentId::from_bytes(b"file2");

        for cid in [&cid1, &cid2] {
            let manifest = ChunkManifest {
                content_id: *cid,
                content_hash: cid.0,
                k: 4,
                chunk_size: 65536,
                chunk_count: 1,
                erasure_config: default_erasure_config(),
                content_size: 100,
            creator: String::new(),
            signature: vec![],
            };
            store.put_manifest(&manifest).unwrap();
            store.put_shard(cid, 0, 0, b"data").unwrap();
        }

        let content = store.list_content().unwrap();
        assert_eq!(content.len(), 2);

        store.delete_content(&cid1).unwrap();
        let content = store.list_content().unwrap();
        assert_eq!(content.len(), 1);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_pin_manager() {
        let dir = test_dir();
        let mut pm = PinManager::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"pinned");

        assert!(!pm.is_pinned(&cid));
        pm.pin(&cid).unwrap();
        assert!(pm.is_pinned(&cid));

        let pinned = pm.list_pinned();
        assert_eq!(pinned.len(), 1);

        pm.unpin(&cid).unwrap();
        assert!(!pm.is_pinned(&cid));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_pin_persistence() {
        let dir = test_dir();
        let cid = ContentId::from_bytes(b"persist");

        {
            let mut pm = PinManager::new(&dir).unwrap();
            pm.pin(&cid).unwrap();
        }

        // Reload
        let pm = PinManager::new(&dir).unwrap();
        assert!(pm.is_pinned(&cid));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_gc_sweep() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let mut pm = PinManager::new(&dir).unwrap();

        let cid1 = ContentId::from_bytes(b"gc1");
        let cid2 = ContentId::from_bytes(b"gc2");
        let cid3 = ContentId::from_bytes(b"gc3");

        // Create 3 items, pin one
        for cid in [&cid1, &cid2, &cid3] {
            let manifest = ChunkManifest {
                content_id: *cid,
                content_hash: cid.0,
                k: 4,
                chunk_size: 65536,
                chunk_count: 1,
                erasure_config: default_erasure_config(),
                content_size: 100,
            creator: String::new(),
            signature: vec![],
            };
            store.put_manifest(&manifest).unwrap();
        }
        pm.pin(&cid1).unwrap();

        // GC with 150 byte threshold: 300 total, need to remove 2 unpinned to get to 100
        let removed = GarbageCollector::sweep(&store, &pm, 150).unwrap();
        assert_eq!(removed, 2);

        // Pinned item still exists
        assert!(store.has_manifest(&cid1));

        std::fs::remove_dir_all(&dir).ok();
    }
}
