//! DataCraft Store
//!
//! Filesystem content-addressed storage for DataCraft.
//!
//! Layout:
//! ```text
//! <data_dir>/
//!   pieces/<cid_hex>/<segment_idx>/<piece_id_hex>.data
//!   pieces/<cid_hex>/<segment_idx>/<piece_id_hex>.coeff
//!   manifests/<cid_hex>.json
//!   pins.json
//! ```
//!
//! Piece identity: `piece_id = SHA-256(coefficient_vector)`.

use std::path::{Path, PathBuf};

use datacraft_core::{ContentId, ContentManifest, DataCraftError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

pub mod merkle;

/// Compute piece_id from a coefficient vector: SHA-256(coefficients).
pub fn piece_id_from_coefficients(coefficients: &[u8]) -> [u8; 32] {
    let hash = Sha256::digest(coefficients);
    let mut id = [0u8; 32];
    id.copy_from_slice(&hash);
    id
}

/// Content-addressed filesystem store using RLNC piece model.
pub struct FsStore {
    data_dir: PathBuf,
}

impl FsStore {
    /// Create a new FsStore at the given directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(data_dir.join("pieces"))?;
        std::fs::create_dir_all(data_dir.join("manifests"))?;
        Ok(Self { data_dir })
    }

    /// Return the base data directory path.
    pub fn base_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Store a coded piece (data + coefficient vector) on disk.
    ///
    /// `piece_id` is SHA-256 of the coefficient vector.
    pub fn store_piece(
        &self,
        content_id: &ContentId,
        segment_index: u32,
        piece_id: &[u8; 32],
        data: &[u8],
        coefficients: &[u8],
    ) -> Result<()> {
        let dir = self.piece_dir(content_id, segment_index);
        std::fs::create_dir_all(&dir)?;
        let id_hex = hex::encode(piece_id);
        std::fs::write(dir.join(format!("{id_hex}.data")), data)?;
        std::fs::write(dir.join(format!("{id_hex}.coeff")), coefficients)?;
        debug!(
            "Stored piece {}/{}/{} ({} bytes data, {} bytes coeff)",
            content_id,
            segment_index,
            &id_hex[..8],
            data.len(),
            coefficients.len()
        );
        Ok(())
    }

    /// Read a piece (data + coefficient vector) from disk.
    pub fn get_piece(
        &self,
        content_id: &ContentId,
        segment_index: u32,
        piece_id: &[u8; 32],
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let dir = self.piece_dir(content_id, segment_index);
        let id_hex = hex::encode(piece_id);
        let data_path = dir.join(format!("{id_hex}.data"));
        let coeff_path = dir.join(format!("{id_hex}.coeff"));
        if !data_path.exists() {
            return Err(DataCraftError::ContentNotFound(format!(
                "piece {}/{}/{}",
                content_id, segment_index, &id_hex[..8]
            )));
        }
        let data = std::fs::read(&data_path)?;
        let coefficients = std::fs::read(&coeff_path)?;
        Ok((data, coefficients))
    }

    /// Check if a piece exists.
    pub fn has_piece(
        &self,
        content_id: &ContentId,
        segment_index: u32,
        piece_id: &[u8; 32],
    ) -> bool {
        let dir = self.piece_dir(content_id, segment_index);
        let id_hex = hex::encode(piece_id);
        dir.join(format!("{id_hex}.data")).exists()
    }

    /// Get a random piece for a segment (for "give me any piece" requests).
    ///
    /// Returns `(piece_id, data, coefficients)` or `None` if no pieces stored.
    #[allow(clippy::type_complexity)]
    pub fn get_random_piece(
        &self,
        content_id: &ContentId,
        segment_index: u32,
    ) -> Result<Option<([u8; 32], Vec<u8>, Vec<u8>)>> {
        let pieces = self.list_pieces(content_id, segment_index)?;
        if pieces.is_empty() {
            return Ok(None);
        }
        // Pick a pseudo-random piece based on current time
        let idx = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as usize
            % pieces.len();
        let piece_id = pieces[idx];
        let (data, coefficients) = self.get_piece(content_id, segment_index, &piece_id)?;
        Ok(Some((piece_id, data, coefficients)))
    }

    /// List all piece IDs stored for a given content/segment.
    pub fn list_pieces(
        &self,
        content_id: &ContentId,
        segment_index: u32,
    ) -> Result<Vec<[u8; 32]>> {
        let dir = self.piece_dir(content_id, segment_index);
        let mut result = Vec::new();
        if !dir.exists() {
            return Ok(result);
        }
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(hex_str) = name.strip_suffix(".data") {
                    if let Ok(bytes) = hex::decode(hex_str) {
                        if bytes.len() == 32 {
                            let mut id = [0u8; 32];
                            id.copy_from_slice(&bytes);
                            result.push(id);
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    /// Store a content manifest.
    pub fn store_manifest(&self, manifest: &ContentManifest) -> Result<()> {
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

    /// Retrieve a content manifest.
    pub fn get_manifest(&self, content_id: &ContentId) -> Result<ContentManifest> {
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

    /// List all segment indices for a content ID.
    pub fn list_segments(&self, content_id: &ContentId) -> Result<Vec<u32>> {
        let dir = self.data_dir.join("pieces").join(content_id.to_hex());
        let mut result = Vec::new();
        if !dir.exists() {
            return Ok(result);
        }
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_dir() {
                        if let Some(name) = entry.file_name().to_str() {
                            if let Ok(idx) = name.parse::<u32>() {
                                result.push(idx);
                            }
                        }
                    }
                }
            }
        }
        result.sort();
        Ok(result)
    }

    /// List all content IDs that have pieces stored (scans piece directory).
    pub fn list_content_with_pieces(&self) -> Result<Vec<ContentId>> {
        let pieces_dir = self.data_dir.join("pieces");
        let mut result = Vec::new();
        if !pieces_dir.exists() {
            return Ok(result);
        }
        if let Ok(entries) = std::fs::read_dir(&pieces_dir) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_dir() {
                        if let Some(hex_str) = entry.file_name().to_str() {
                            if let Ok(cid) = ContentId::from_hex(hex_str) {
                                result.push(cid);
                            }
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    /// List all content IDs that have manifests stored.
    pub fn list_content(&self) -> Result<Vec<ContentId>> {
        let manifest_dir = self.data_dir.join("manifests");
        let mut result = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&manifest_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(hex_str) = name.strip_suffix(".json") {
                    if let Ok(cid) = ContentId::from_hex(hex_str) {
                        result.push(cid);
                    }
                }
            }
        }
        Ok(result)
    }

    /// Delete a single piece from disk.
    pub fn delete_piece(
        &self,
        content_id: &ContentId,
        segment_index: u32,
        piece_id: &[u8; 32],
    ) -> Result<()> {
        let dir = self.piece_dir(content_id, segment_index);
        let id_hex = hex::encode(piece_id);
        let data_path = dir.join(format!("{id_hex}.data"));
        let coeff_path = dir.join(format!("{id_hex}.coeff"));
        if data_path.exists() {
            std::fs::remove_file(&data_path)?;
        }
        if coeff_path.exists() {
            std::fs::remove_file(&coeff_path)?;
        }
        debug!("Deleted piece {}/{}/{}", content_id, segment_index, &id_hex[..8]);
        Ok(())
    }

    /// Delete all data (pieces + manifest) for a content ID.
    pub fn delete_content(&self, content_id: &ContentId) -> Result<()> {
        let piece_dir = self.data_dir.join("pieces").join(content_id.to_hex());
        if piece_dir.exists() {
            std::fs::remove_dir_all(&piece_dir)?;
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

    /// Verify integrity of all stored pieces.
    ///
    /// Scans every piece, recomputes `piece_id = SHA-256(coefficients)`,
    /// and removes pieces where the stored ID doesn't match.
    /// Returns the number of corrupted pieces removed.
    pub fn verify_integrity(&self) -> Result<usize> {
        let mut removed = 0;
        let content_ids = self.list_content_with_pieces()?;
        for cid in &content_ids {
            let segments = self.list_segments(cid)?;
            for seg in segments {
                let pieces = self.list_pieces(cid, seg)?;
                for pid in pieces {
                    match self.get_piece(cid, seg, &pid) {
                        Ok((_data, coefficients)) => {
                            let expected_id = piece_id_from_coefficients(&coefficients);
                            if expected_id != pid {
                                warn!(
                                    "Corrupted piece {}/{}/{}: expected {}, removing",
                                    cid, seg, hex::encode(&pid[..8]), hex::encode(&expected_id[..8])
                                );
                                let dir = self.piece_dir(cid, seg);
                                let id_hex = hex::encode(pid);
                                let _ = std::fs::remove_file(dir.join(format!("{id_hex}.data")));
                                let _ = std::fs::remove_file(dir.join(format!("{id_hex}.coeff")));
                                removed += 1;
                            }
                        }
                        Err(e) => {
                            warn!("Failed to read piece {}/{}/{}: {}, removing", cid, seg, hex::encode(&pid[..8]), e);
                            let dir = self.piece_dir(cid, seg);
                            let id_hex = hex::encode(pid);
                            let _ = std::fs::remove_file(dir.join(format!("{id_hex}.data")));
                            let _ = std::fs::remove_file(dir.join(format!("{id_hex}.coeff")));
                            removed += 1;
                        }
                    }
                }
            }
        }
        if removed > 0 {
            warn!("Integrity check: removed {} corrupted pieces", removed);
        }
        Ok(removed)
    }

    /// Returns the base data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// List all pinned content IDs by reading from the pins file in the data directory.
    pub fn list_pinned(&self) -> Vec<ContentId> {
        let pins_path = self.data_dir.join("pins.json");
        if !pins_path.exists() {
            return Vec::new();
        }
        match PinManager::new(&self.data_dir) {
            Ok(pm) => pm.list_pinned(),
            Err(_) => Vec::new(),
        }
    }

    /// Calculate total disk usage of all stored pieces and manifests in bytes.
    pub fn disk_usage(&self) -> Result<u64> {
        let mut total = 0u64;
        let pieces_dir = self.data_dir.join("pieces");
        if pieces_dir.exists() {
            total += dir_size(&pieces_dir)?;
        }
        let manifests_dir = self.data_dir.join("manifests");
        if manifests_dir.exists() {
            total += dir_size(&manifests_dir)?;
        }
        Ok(total)
    }

    /// Calculate disk usage for a specific content ID.
    pub fn cid_disk_usage(&self, content_id: &ContentId) -> u64 {
        let mut total = 0u64;
        let piece_dir = self.data_dir.join("pieces").join(content_id.to_hex());
        if piece_dir.exists() {
            total += dir_size(&piece_dir).unwrap_or(0);
        }
        let manifest_path = self.data_dir.join("manifests").join(format!("{}.json", content_id.to_hex()));
        if manifest_path.exists() {
            total += std::fs::metadata(&manifest_path).map(|m| m.len()).unwrap_or(0);
        }
        total
    }

    /// Helper: path to a segment's piece directory.
    fn piece_dir(&self, content_id: &ContentId, segment_index: u32) -> PathBuf {
        self.data_dir
            .join("pieces")
            .join(content_id.to_hex())
            .join(segment_index.to_string())
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

        let mut total: u64 = 0;
        let mut candidates: Vec<(ContentId, u64)> = Vec::new();
        for cid in &all_content {
            if let Ok(manifest) = store.get_manifest(cid) {
                total += manifest.total_size;
                if !pin_manager.is_pinned(cid) {
                    candidates.push((*cid, manifest.total_size));
                }
            }
        }

        if total <= max_bytes {
            return Ok(0);
        }

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

/// Recursively calculate the size of a directory.
fn dir_size(path: &std::path::Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn make_manifest(cid: ContentId) -> ContentManifest {
        ContentManifest {
            content_id: cid,
            content_hash: cid.0,
            segment_size: 10_485_760,
            piece_size: 262_144,
            segment_count: 1,
            total_size: 1000,
            creator: String::new(),
            signature: vec![],
        }
    }

    #[test]
    fn test_piece_roundtrip() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"hello");

        let coefficients = vec![1u8, 0, 0, 0];
        let piece_id = piece_id_from_coefficients(&coefficients);
        let data = b"piece data";

        store.store_piece(&cid, 0, &piece_id, data, &coefficients).unwrap();
        assert!(store.has_piece(&cid, 0, &piece_id));

        let (got_data, got_coeff) = store.get_piece(&cid, 0, &piece_id).unwrap();
        assert_eq!(got_data, data);
        assert_eq!(got_coeff, coefficients);

        // Non-existent piece
        let fake_id = [0u8; 32];
        assert!(!store.has_piece(&cid, 0, &fake_id));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_pieces() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"list test");

        let coeff1 = vec![1u8, 0, 0];
        let coeff2 = vec![0u8, 1, 0];
        let id1 = piece_id_from_coefficients(&coeff1);
        let id2 = piece_id_from_coefficients(&coeff2);

        store.store_piece(&cid, 0, &id1, b"d1", &coeff1).unwrap();
        store.store_piece(&cid, 0, &id2, b"d2", &coeff2).unwrap();

        let pieces = store.list_pieces(&cid, 0).unwrap();
        assert_eq!(pieces.len(), 2);
        assert!(pieces.contains(&id1));
        assert!(pieces.contains(&id2));

        // Empty segment
        let empty = store.list_pieces(&cid, 1).unwrap();
        assert!(empty.is_empty());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_get_random_piece() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"random test");

        // Empty → None
        let none = store.get_random_piece(&cid, 0).unwrap();
        assert!(none.is_none());

        let coeff = vec![42u8, 7, 3];
        let id = piece_id_from_coefficients(&coeff);
        store.store_piece(&cid, 0, &id, b"data", &coeff).unwrap();

        let (got_id, got_data, got_coeff) = store.get_random_piece(&cid, 0).unwrap().unwrap();
        assert_eq!(got_id, id);
        assert_eq!(got_data, b"data");
        assert_eq!(got_coeff, coeff);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_manifest_roundtrip() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"test content");
        let manifest = make_manifest(cid);

        store.store_manifest(&manifest).unwrap();
        assert!(store.has_manifest(&cid));

        let loaded = store.get_manifest(&cid).unwrap();
        assert_eq!(loaded.content_id, cid);
        assert_eq!(loaded.total_size, 1000);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_and_delete_content() {
        let dir = test_dir();
        let store = FsStore::new(&dir).unwrap();

        let cid1 = ContentId::from_bytes(b"file1");
        let cid2 = ContentId::from_bytes(b"file2");

        for cid in [&cid1, &cid2] {
            store.store_manifest(&make_manifest(*cid)).unwrap();
            let coeff = vec![1u8, 2, 3];
            let id = piece_id_from_coefficients(&coeff);
            store.store_piece(cid, 0, &id, b"data", &coeff).unwrap();
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

        for cid in [&cid1, &cid2, &cid3] {
            let mut manifest = make_manifest(*cid);
            manifest.total_size = 100;
            store.store_manifest(&manifest).unwrap();
        }
        pm.pin(&cid1).unwrap();

        // 300 total, threshold 150: remove 2 unpinned
        let removed = GarbageCollector::sweep(&store, &pm, 150).unwrap();
        assert_eq!(removed, 2);

        // Pinned item still exists
        assert!(store.has_manifest(&cid1));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_piece_id_from_coefficients() {
        let coeff = vec![1, 2, 3, 4];
        let id = piece_id_from_coefficients(&coeff);
        assert_eq!(id.len(), 32);
        // Deterministic
        assert_eq!(id, piece_id_from_coefficients(&coeff));
        // Different input → different ID
        assert_ne!(id, piece_id_from_coefficients(&[5, 6, 7, 8]));
    }
}
