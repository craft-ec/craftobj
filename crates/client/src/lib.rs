//! CraftObj Client
//!
//! High-level orchestration API for publishing, fetching, pinning content.
//!
//! # Lifecycle
//!
//! - `publish(path, options)` → encrypt? → CID → segment & RLNC encode → store pieces → manifest
//! - `reconstruct(cid)` → manifest → decode segments → verify hash → decrypt? → write
//! - `pin(cid)` / `unpin(cid)` / `list()` / `status()`

pub mod fetch;

// Re-export connection pool types for use by the daemon handler.
pub use fetch::{ConnectionPool, FetchConfig, PieceRequester, ProviderId};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use craftec_erasure::{
    check_independence, segmenter, CodedPiece,
    ErasureConfig, generate_segment_hashes, ContentVerificationRecord,
};
use craftobj_core::{
    ContentId, ContentManifest, CraftObjError, PIECE_SIZE, PublishOptions, Result,
    SEGMENT_SIZE,
};
use craftobj_store::{piece_id_from_coefficients, FsStore, PinManager};
use tracing::info;

/// Published content result.
#[derive(Debug, Clone)]
pub struct PublishResult {
    /// Content ID (hash of content, or ciphertext if encrypted).
    pub content_id: ContentId,
    /// Encryption key if encrypted (caller must store this).
    pub encryption_key: Option<Vec<u8>>,
    /// Total size in bytes.
    pub total_size: u64,
    /// Number of segments.
    pub segment_count: usize,
    /// Cached content record (for daemon to announce).
    pub manifest: ContentManifest,
}

/// Content info returned by list().
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ContentInfo {
    pub content_id: ContentId,
    pub total_size: u64,
    pub segment_count: usize,
    pub pinned: bool,
}

/// Node status information.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeStatus {
    pub stored_bytes: u64,
    pub content_count: usize,
    pub piece_count: usize,
    pub pinned_count: usize,
}

// NOTE: RevocationResult removed — revoke/rotate is COM layer concern.

/// CraftObj client for local operations (publish, fetch, pin/unpin).
///
/// Network operations (announce to DHT, download from peers) are handled
/// by the daemon, which wraps this client with a running swarm.
pub struct CraftObjClient {
    store: FsStore,
    pin_manager: PinManager,
}

impl CraftObjClient {
    /// Create a new client with the given data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        let store = FsStore::new(&data_dir)?;
        let pin_manager = PinManager::new(&data_dir)?;
        Ok(Self { store, pin_manager })
    }

    /// Publish content from raw bytes.
    ///
    /// Steps: encrypt? → CID → segment & RLNC encode → store pieces + manifest.
    pub fn publish_bytes(
        &mut self,
        data: &[u8],
        options: &PublishOptions,
    ) -> Result<PublishResult> {
        if data.is_empty() {
            return Err(CraftObjError::StorageError("empty data".into()));
        }

        // Optionally encrypt
        let (content_bytes, encryption_key) = if options.encrypted {
            let key = generate_content_key();
            let encrypted = encrypt_content(data, &key)?;
            (encrypted, Some(key))
        } else {
            (data.to_vec(), None)
        };

        // CID = SHA-256 of what we store
        let content_id = ContentId::from_bytes(&content_bytes);
        let total_size = content_bytes.len() as u64;

        let config = ErasureConfig {
            piece_size: PIECE_SIZE,
            segment_size: SEGMENT_SIZE,
            ..Default::default()
        };

        // Segment and RLNC encode
        let encoded_segments = segmenter::segment_and_encode(&content_bytes, &config)
            .map_err(|e| CraftObjError::ErasureError(e.to_string()))?;

        let segment_count = encoded_segments.len();

        info!(
            "Publishing {} ({} bytes, {} segments, piece_size={})",
            content_id, total_size, segment_count, config.piece_size,
        );

        // ── Compute vtags (homomorphic verification tags) ──────────────────
        // For each segment, the first k pieces have identity coefficient vectors
        // (source pieces). We compute homomorphic hashes from these, enabling
        // any coded piece to be verified without decoding.
        let mut all_segment_hashes = Vec::with_capacity(segment_count);
        for (seg_idx, pieces) in &encoded_segments {
            let k = config.k_for_segment(
                if (*seg_idx as usize + 1) < segment_count {
                    config.segment_size
                } else {
                    content_bytes.len() - *seg_idx as usize * config.segment_size
                },
            );
            let source_pieces = &pieces[..k];
            // Deterministic seed: SHA-256(content_id || segment_index)
            let seed = {
                use sha2::{Sha256, Digest};
                let mut hasher = Sha256::new();
                hasher.update(&content_id.0);
                hasher.update(&(*seg_idx as u32).to_le_bytes());
                let hash = hasher.finalize();
                let mut s = [0u8; 32];
                s.copy_from_slice(&hash);
                s
            };
            all_segment_hashes.push(generate_segment_hashes(seed, source_pieces));
        }

        let vtag_record = ContentVerificationRecord {
            file_size: total_size,
            segment_hashes: all_segment_hashes,
        };
        let vtag_blob = serde_json::to_vec(&vtag_record)
            .map_err(|e| CraftObjError::StorageError(format!("vtag serialization: {}", e)))?;
        let vtags_cid_bytes = {
            use sha2::{Sha256, Digest};
            let hash = Sha256::digest(&vtag_blob);
            let mut cid = [0u8; 32];
            cid.copy_from_slice(&hash);
            cid
        };

        // Store vtag blob locally — transferred as a raw blob (not pieces)
        // alongside content. Nodes fetch it by vtags_cid when they receive
        // pieces and need to verify them.
        self.store.store_vtag_blob(&vtags_cid_bytes, &vtag_blob)?;
        info!("Vtag blob {} ({} bytes)", hex::encode(&vtags_cid_bytes[..4]), vtag_blob.len());

        // Store all pieces with self-describing headers (vtags_cid set)
        for (seg_idx, pieces) in &encoded_segments {
            let k = config.k_for_segment(
                if (*seg_idx as usize + 1) < segment_count {
                    config.segment_size
                } else {
                    content_bytes.len() - *seg_idx as usize * config.segment_size
                },
            );
            for piece in pieces {
                let pid = piece_id_from_coefficients(&piece.coefficients);
                let header = craftobj_core::PieceHeader {
                    content_id,
                    total_size,
                    segment_idx: *seg_idx,
                    segment_count: segment_count as u32,
                    k: k as u32,
                    vtags_cid: Some(vtags_cid_bytes),
                    coefficients: piece.coefficients.clone(),
                };
                self.store.store_piece_with_header(
                    &content_id,
                    *seg_idx,
                    &pid,
                    &piece.data,
                    &piece.coefficients,
                    &header,
                )?;
            }
        }

        // Build and store content record (cached summary)
        let manifest = ContentManifest {
            content_id,
            total_size,
            vtags_cid: Some(vtags_cid_bytes),
        };
        self.store.store_record(&manifest)?;

        // Auto-pin published content
        self.pin_manager.pin(&content_id)?;

        info!("Published {} (vtags={})", content_id, hex::encode(&vtags_cid_bytes[..4]));

        Ok(PublishResult {
            content_id,
            encryption_key,
            total_size,
            segment_count,
            manifest,
        })
    }

    /// Publish a file: read, hash, segment, RLNC-encode, store locally.
    pub fn publish(&mut self, path: &Path, options: &PublishOptions) -> Result<PublishResult> {
        let data = std::fs::read(path)?;
        self.publish_bytes(&data, options)
    }

    /// Publish a file with creator signing.
    pub fn publish_signed(
        &mut self,
        path: &Path,
        options: &PublishOptions,
        _keypair: &ed25519_dalek::SigningKey,
    ) -> Result<PublishResult> {
        // Creator signing has been moved to the application layer (COM/SQL).
        self.publish(path, options)
    }

    /// Unpin content — kernel-level removal signal.
    ///
    /// This does NOT immediately delete pieces. It unpins the content so that
    /// HealthScan will not repair it. Over time, pieces naturally degrade.
    /// For explicit force-delete, see `delete_all_pieces()`.
    pub fn unpin_content(&mut self, content_id: &ContentId) -> Result<()> {
        let _ = self.pin_manager.unpin(content_id);
        info!("Unpinned {}", content_id);
        Ok(())
    }

    /// Reconstruct content from locally stored pieces using RLNC decoding.
    ///
    /// Reads piece headers for metadata (total_size, segment_count, k).
    /// Falls back to manifest if no piece headers exist (legacy content).
    pub fn reconstruct(
        &self,
        content_id: &ContentId,
        dest: &Path,
        encryption_key: Option<&[u8]>,
    ) -> Result<()> {
        // Try piece header first (manifest-free path)
        let (total_size, segment_count, _default_k) =
            if let Ok(Some(header)) = self.store.get_any_piece_header(content_id) {
                (
                    header.total_size as usize,
                    header.segment_count as usize,
                    header.k as usize,
                )
            } else {
                // Legacy fallback: use manifest
                let manifest = self.store.get_record(content_id)?;
                (
                    manifest.total_size as usize,
                    manifest.segment_count(),
                    manifest.k(),
                )
            };

        let config = ErasureConfig {
            piece_size: PIECE_SIZE,
            segment_size: SEGMENT_SIZE,
            ..Default::default()
        };

        let mut segments: BTreeMap<u32, Vec<CodedPiece>> = BTreeMap::new();

        for seg_idx in 0..segment_count as u32 {
            let seg_size = if (seg_idx as usize + 1) < segment_count {
                SEGMENT_SIZE
            } else {
                total_size - seg_idx as usize * SEGMENT_SIZE
            };
            let k = config.k_for_segment(seg_size);
            let piece_ids = self.store.list_pieces(content_id, seg_idx)?;

            let mut collected: Vec<CodedPiece> = Vec::new();
            for pid in &piece_ids {
                let (data, coefficients) = self.store.get_piece(content_id, seg_idx, pid)?;
                collected.push(CodedPiece { data, coefficients });

                // Check if we have enough independent pieces
                let coeff_vecs: Vec<Vec<u8>> =
                    collected.iter().map(|p| p.coefficients.clone()).collect();
                let rank = check_independence(&coeff_vecs);
                if rank >= k {
                    break;
                }
            }

            segments.insert(seg_idx, collected);
        }

        let reconstructed = segmenter::decode_and_reassemble(
            &segments,
            segment_count as u32,
            &config,
            total_size,
        )
        .map_err(|e| CraftObjError::ErasureError(e.to_string()))?;

        // Verify hash
        let expected_id = ContentId::from_bytes(&reconstructed);
        if expected_id != *content_id {
            return Err(CraftObjError::ContentNotFound(
                "hash mismatch after reconstruction".into(),
            ));
        }

        // Decrypt if key provided
        let final_data = if let Some(key) = encryption_key {
            decrypt_content(&reconstructed, key)?
        } else {
            reconstructed
        };

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(dest, &final_data)?;
        info!("Reconstructed {} to {:?}", content_id, dest);

        Ok(())
    }

    /// Pin content for persistent storage.
    pub fn pin(&mut self, content_id: &ContentId) -> Result<()> {
        self.pin_manager.pin(content_id)
    }

    /// Unpin content (mark for GC).
    pub fn unpin(&mut self, content_id: &ContentId) -> Result<()> {
        self.pin_manager.unpin(content_id)
    }

    /// Check if content is pinned.
    pub fn is_pinned(&self, content_id: &ContentId) -> bool {
        self.pin_manager.is_pinned(content_id)
    }

    /// List all locally stored content.
    pub fn list(&self) -> Result<Vec<ContentInfo>> {
        let content_ids = self.store.list_content()?;
        let mut result = Vec::new();
        for cid in content_ids {
            if let Ok(manifest) = self.store.get_record(&cid) {
                result.push(ContentInfo {
                    content_id: cid,
                    total_size: manifest.total_size,
                    segment_count: manifest.segment_count(),
                    pinned: self.pin_manager.is_pinned(&cid),
                });
            }
        }
        Ok(result)
    }

    /// Get node status.
    pub fn status(&self) -> Result<NodeStatus> {
        let content = self.store.list_content()?;
        let mut stored_bytes = 0u64;
        let mut piece_count = 0usize;
        for cid in &content {
            if let Ok(manifest) = self.store.get_record(cid) {
                stored_bytes += manifest.total_size;
                for seg in 0..manifest.segment_count() as u32 {
                    piece_count += self.store.list_pieces(cid, seg).unwrap_or_default().len();
                }
            }
        }
        Ok(NodeStatus {
            stored_bytes,
            content_count: content.len(),
            piece_count,
            pinned_count: self.pin_manager.list_pinned().len(),
        })
    }

    // NOTE: publish_with_pre, grant_access, grant_access_direct,
    // reconstruct_with_pre, and revoke_and_rotate removed.
    // Access control and PRE are COM/SQL layer concerns.


    /// Access the underlying store.
    pub fn store(&self) -> &FsStore {
        &self.store
    }

    /// Access the pin manager.
    pub fn pin_manager(&self) -> &PinManager {
        &self.pin_manager
    }
}

/// Generate a random 32-byte content encryption key.
fn generate_content_key() -> Vec<u8> {
    use rand::RngCore;
    let mut key = vec![0u8; 32];
    rand::thread_rng().fill_bytes(&mut key);
    key
}

/// Encrypt content with ChaCha20-Poly1305 using a 32-byte key.
fn encrypt_content(data: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    use chacha20poly1305::{
        aead::{Aead, KeyInit},
        ChaCha20Poly1305, Nonce,
    };

    let cipher = ChaCha20Poly1305::new_from_slice(key)
        .map_err(|e| CraftObjError::EncryptionError(e.to_string()))?;

    let mut nonce_bytes = [0u8; 12];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| CraftObjError::EncryptionError(e.to_string()))?;

    let mut result = Vec::with_capacity(12 + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt content encrypted with encrypt_content.
fn decrypt_content(data: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    use chacha20poly1305::{
        aead::{Aead, KeyInit},
        ChaCha20Poly1305, Nonce,
    };

    if data.len() < 12 {
        return Err(CraftObjError::EncryptionError("data too short".into()));
    }

    let (nonce_bytes, ciphertext) = data.split_at(12);
    let cipher = ChaCha20Poly1305::new_from_slice(key)
        .map_err(|e| CraftObjError::EncryptionError(e.to_string()))?;
    let nonce = Nonce::from_slice(nonce_bytes);

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| CraftObjError::EncryptionError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir() -> PathBuf {
        use rand::RngCore;
        let mut rng_bytes = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut rng_bytes);
        let dir = std::env::temp_dir()
            .join("craftobj-client-test")
            .join(format!(
                "{}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                hex::encode(rng_bytes)
            ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_encrypt_decrypt() {
        let key = generate_content_key();
        let plaintext = b"hello craftobj encryption";
        let encrypted = encrypt_content(plaintext, &key).unwrap();
        assert_ne!(&encrypted, plaintext);
        let decrypted = decrypt_content(&encrypted, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_publish_and_reconstruct() {
        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();

        let file_path = dir.join("input.txt");
        let content = b"hello craftobj world! this is test content for publishing.";
        std::fs::write(&file_path, content).unwrap();

        let result = client
            .publish(&file_path, &PublishOptions::default())
            .unwrap();
        assert_eq!(result.total_size, content.len() as u64);
        assert!(result.encryption_key.is_none());
        assert_eq!(result.segment_count, 1);

        let output_path = dir.join("output.txt");
        client
            .reconstruct(&result.content_id, &output_path, None)
            .unwrap();
        let reconstructed = std::fs::read(&output_path).unwrap();
        assert_eq!(reconstructed, content);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_publish_encrypted() {
        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();

        let file_path = dir.join("secret.txt");
        std::fs::write(&file_path, b"secret data").unwrap();

        let result = client
            .publish(
                &file_path,
                &PublishOptions { encrypted: true },
            )
            .unwrap();
        assert!(result.encryption_key.is_some());

        let output_path = dir.join("decrypted.txt");
        client
            .reconstruct(
                &result.content_id,
                &output_path,
                result.encryption_key.as_deref(),
            )
            .unwrap();
        assert_eq!(std::fs::read(&output_path).unwrap(), b"secret data");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_publish_encrypted_fetch_without_key_gets_ciphertext() {
        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();

        let file_path = dir.join("secret2.txt");
        let plaintext = b"this is secret content that should be encrypted";
        std::fs::write(&file_path, plaintext).unwrap();

        let result = client
            .publish(
                &file_path,
                &PublishOptions { encrypted: true },
            )
            .unwrap();

        let output_no_key = dir.join("no_key_output.bin");
        client
            .reconstruct(&result.content_id, &output_no_key, None)
            .unwrap();
        let raw = std::fs::read(&output_no_key).unwrap();
        assert_ne!(raw, plaintext);
        assert!(raw.len() > plaintext.len());

        let output_with_key = dir.join("with_key_output.bin");
        client
            .reconstruct(
                &result.content_id,
                &output_with_key,
                result.encryption_key.as_deref(),
            )
            .unwrap();
        assert_eq!(std::fs::read(&output_with_key).unwrap(), plaintext);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_pin_unpin() {
        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"pin test");

        client.pin(&cid).unwrap();
        assert!(client.is_pinned(&cid));

        client.unpin(&cid).unwrap();
        assert!(!client.is_pinned(&cid));

        std::fs::remove_dir_all(&dir).ok();
    }

    // NOTE: test_publish_with_pre_and_grant_access and
    // test_revoke_and_rotate_full_round_trip removed — PRE/access is COM layer.

    #[test]
    fn test_list_and_status() {
        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();

        let file_path = dir.join("list_test.txt");
        std::fs::write(&file_path, b"list test content").unwrap();

        client
            .publish(&file_path, &PublishOptions::default())
            .unwrap();

        let items = client.list().unwrap();
        assert_eq!(items.len(), 1);
        assert!(items[0].pinned);

        let status = client.status().unwrap();
        assert_eq!(status.content_count, 1);
        assert!(status.stored_bytes > 0);
        assert!(status.piece_count > 0);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_publish_signed_and_verify() {
        use ed25519_dalek::SigningKey;

        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();
        let keypair = SigningKey::generate(&mut rand::thread_rng());

        let file_path = dir.join("signed_test.txt");
        std::fs::write(&file_path, b"signed content").unwrap();

        let result = client
            .publish_signed(&file_path, &PublishOptions::default(), &keypair)
            .unwrap();

        // publish_signed now delegates to publish (signing is COM layer)
        let manifest = client.store().get_record(&result.content_id).unwrap();
        assert_eq!(manifest.content_id, result.content_id);
        assert_eq!(manifest.total_size, result.total_size);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_unpin_content() {
        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();

        let file_path = dir.join("to_unpin.txt");
        std::fs::write(&file_path, b"content to unpin").unwrap();

        let result = client
            .publish(&file_path, &PublishOptions::default())
            .unwrap();
        assert!(client.is_pinned(&result.content_id));

        client.unpin_content(&result.content_id).unwrap();
        assert!(!client.is_pinned(&result.content_id));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_publish_bytes_directly() {
        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();

        let content = b"direct byte publish test";
        let result = client
            .publish_bytes(content, &PublishOptions::default())
            .unwrap();

        let output = dir.join("output.bin");
        client
            .reconstruct(&result.content_id, &output, None)
            .unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), content);

        std::fs::remove_dir_all(&dir).ok();
    }
}
