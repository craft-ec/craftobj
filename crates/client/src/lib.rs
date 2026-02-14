//! DataCraft Client
//!
//! High-level orchestration API for publishing, fetching, pinning content.
//!
//! # Lifecycle
//!
//! - `publish(path, options)` → hash → chunk → erasure encode → store → announce → CID
//! - `fetch(cid, dest)` → resolve → download manifest → download shards → decode → verify → write
//! - `pin(cid)` / `unpin(cid)` / `list()` / `status()`

use std::path::{Path, PathBuf};

use craftec_erasure::ErasureCoder;
use datacraft_core::{
    ChunkManifest, ContentId, DataCraftError, PublishOptions, Result,
    default_erasure_config,
};
use datacraft_store::{FsStore, PinManager};
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
    /// Number of chunks.
    pub chunk_count: u32,
}

/// Content info returned by list().
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ContentInfo {
    pub content_id: ContentId,
    pub total_size: u64,
    pub chunk_count: u32,
    pub encrypted: bool,
    pub pinned: bool,
}

/// Node status information.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeStatus {
    pub stored_bytes: u64,
    pub content_count: usize,
    pub pinned_count: usize,
}

/// DataCraft client for local operations (publish, fetch, pin/unpin).
///
/// Network operations (announce to DHT, download from peers) are handled
/// by the daemon, which wraps this client with a running swarm.
pub struct DataCraftClient {
    store: FsStore,
    pin_manager: PinManager,
}

impl DataCraftClient {
    /// Create a new client with the given data directory.
    pub fn new(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let data_dir = data_dir.into();
        let store = FsStore::new(&data_dir)?;
        let pin_manager = PinManager::new(&data_dir)?;
        Ok(Self { store, pin_manager })
    }

    /// Publish a file: hash, chunk, erasure-encode, store locally.
    ///
    /// Returns the ContentId. Network announcement is done by the daemon.
    pub fn publish(&mut self, path: &Path, options: &PublishOptions) -> Result<PublishResult> {
        let data = std::fs::read(path)?;
        if data.is_empty() {
            return Err(DataCraftError::StorageError("empty file".into()));
        }

        let config = options
            .erasure_config
            .unwrap_or_else(default_erasure_config);

        // Optionally encrypt
        let (content_bytes, encryption_key) = if options.encrypted {
            let key = generate_content_key();
            let encrypted = encrypt_content(&data, &key)?;
            (encrypted, Some(key))
        } else {
            (data.clone(), None)
        };

        // Compute CID (hash of what we store — plaintext or ciphertext)
        let content_id = ContentId::from_bytes(&content_bytes);
        let total_size = content_bytes.len() as u64;

        // Chunk the content
        let chunks = chunk_data(&content_bytes, config.chunk_size);
        let chunk_count = chunks.len() as u32;
        let chunk_sizes: Vec<usize> = chunks.iter().map(|c| c.len()).collect();

        info!(
            "Publishing {} ({} bytes, {} chunks, erasure {}/{})",
            content_id,
            total_size,
            chunk_count,
            config.data_shards,
            config.parity_shards,
        );

        // Erasure-encode each chunk and store shards
        let coder = ErasureCoder::with_config(&config)
            .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;
        for (chunk_idx, chunk) in chunks.iter().enumerate() {
            let shards = coder
                .encode(chunk)
                .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;
            for (shard_idx, shard) in shards.iter().enumerate() {
                self.store
                    .put_shard(&content_id, chunk_idx as u32, shard_idx as u8, shard)?;
            }
        }

        // Store manifest
        let manifest = ChunkManifest {
            content_id,
            total_size,
            chunk_count,
            chunk_size: config.chunk_size,
            erasure_config: config,
            encrypted: options.encrypted,
            chunk_sizes,
        };
        self.store.put_manifest(&manifest)?;

        // Auto-pin published content
        self.pin_manager.pin(&content_id)?;

        info!("Published {} successfully", content_id);

        Ok(PublishResult {
            content_id,
            encryption_key,
            total_size,
            chunk_count,
        })
    }

    /// Reconstruct content from locally stored shards.
    ///
    /// For network fetching (downloading missing shards from peers),
    /// the daemon handles orchestration before calling this.
    pub fn reconstruct(
        &self,
        content_id: &ContentId,
        dest: &Path,
        encryption_key: Option<&[u8]>,
    ) -> Result<()> {
        let manifest = self.store.get_manifest(content_id)?;
        let coder = ErasureCoder::with_config(&manifest.erasure_config)
            .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;
        let total_shards = manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards;

        let mut reconstructed = Vec::with_capacity(manifest.total_size as usize);

        for chunk_idx in 0..manifest.chunk_count {
            let chunk_size = manifest.chunk_sizes[chunk_idx as usize];

            // Gather available shards
            let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(total_shards);
            for shard_idx in 0..total_shards {
                match self.store.get_shard(content_id, chunk_idx, shard_idx as u8) {
                    Ok(data) => shards.push(Some(data)),
                    Err(_) => shards.push(None),
                }
            }

            // Decode
            let decoded = coder
                .decode(&mut shards, chunk_size)
                .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;
            reconstructed.extend_from_slice(&decoded);
        }

        // Verify hash
        let expected_id = ContentId::from_bytes(&reconstructed);
        if expected_id != *content_id {
            return Err(DataCraftError::ContentNotFound(
                "hash mismatch after reconstruction".into(),
            ));
        }

        // Decrypt if needed
        let final_data = if manifest.encrypted {
            let key = encryption_key.ok_or_else(|| {
                DataCraftError::EncryptionError("encryption key required".into())
            })?;
            decrypt_content(&reconstructed, key)?
        } else {
            reconstructed
        };

        // Write to destination
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
            if let Ok(manifest) = self.store.get_manifest(&cid) {
                result.push(ContentInfo {
                    content_id: cid,
                    total_size: manifest.total_size,
                    chunk_count: manifest.chunk_count,
                    encrypted: manifest.encrypted,
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
        for cid in &content {
            if let Ok(manifest) = self.store.get_manifest(cid) {
                stored_bytes += manifest.total_size;
            }
        }
        Ok(NodeStatus {
            stored_bytes,
            content_count: content.len(),
            pinned_count: self.pin_manager.list_pinned().len(),
        })
    }

    /// Access the underlying store.
    pub fn store(&self) -> &FsStore {
        &self.store
    }

    /// Access the pin manager.
    pub fn pin_manager(&self) -> &PinManager {
        &self.pin_manager
    }
}

/// Split data into fixed-size chunks (last may be smaller).
fn chunk_data(data: &[u8], chunk_size: usize) -> Vec<Vec<u8>> {
    data.chunks(chunk_size).map(|c| c.to_vec()).collect()
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
        .map_err(|e| DataCraftError::EncryptionError(e.to_string()))?;

    // Generate random nonce
    let mut nonce_bytes = [0u8; 12];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| DataCraftError::EncryptionError(e.to_string()))?;

    // Prepend nonce to ciphertext
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
        return Err(DataCraftError::EncryptionError("data too short".into()));
    }

    let (nonce_bytes, ciphertext) = data.split_at(12);
    let cipher = ChaCha20Poly1305::new_from_slice(key)
        .map_err(|e| DataCraftError::EncryptionError(e.to_string()))?;
    let nonce = Nonce::from_slice(nonce_bytes);

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| DataCraftError::EncryptionError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir() -> PathBuf {
        use rand::RngCore;
        let mut rng_bytes = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut rng_bytes);
        let dir = std::env::temp_dir()
            .join("datacraft-client-test")
            .join(format!("{}-{}", std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
                hex::encode(rng_bytes)));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn test_chunk_data() {
        let data = vec![1u8; 100];
        let chunks = chunk_data(&data, 30);
        assert_eq!(chunks.len(), 4); // 30+30+30+10
        assert_eq!(chunks[0].len(), 30);
        assert_eq!(chunks[3].len(), 10);
    }

    #[test]
    fn test_encrypt_decrypt() {
        let key = generate_content_key();
        let plaintext = b"hello datacraft encryption";
        let encrypted = encrypt_content(plaintext, &key).unwrap();
        assert_ne!(&encrypted, plaintext);
        let decrypted = decrypt_content(&encrypted, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_publish_and_reconstruct() {
        let dir = test_dir();
        let mut client = DataCraftClient::new(&dir).unwrap();

        // Create test file
        let file_path = dir.join("input.txt");
        let content = b"hello datacraft world! this is test content for publishing.";
        std::fs::write(&file_path, content).unwrap();

        // Publish
        let result = client
            .publish(&file_path, &PublishOptions::default())
            .unwrap();
        assert_eq!(result.total_size, content.len() as u64);
        assert!(result.encryption_key.is_none());

        // Reconstruct
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
        let mut client = DataCraftClient::new(&dir).unwrap();

        let file_path = dir.join("secret.txt");
        std::fs::write(&file_path, b"secret data").unwrap();

        let result = client
            .publish(
                &file_path,
                &PublishOptions {
                    encrypted: true,
                    erasure_config: None,
                },
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
    fn test_pin_unpin() {
        let dir = test_dir();
        let mut client = DataCraftClient::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"pin test");

        // Published content is auto-pinned, but we can manually pin/unpin
        client.pin(&cid).unwrap();
        assert!(client.is_pinned(&cid));

        client.unpin(&cid).unwrap();
        assert!(!client.is_pinned(&cid));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_and_status() {
        let dir = test_dir();
        let mut client = DataCraftClient::new(&dir).unwrap();

        let file_path = dir.join("list_test.txt");
        std::fs::write(&file_path, b"list test content").unwrap();

        client.publish(&file_path, &PublishOptions::default()).unwrap();

        let items = client.list().unwrap();
        assert_eq!(items.len(), 1);
        assert!(items[0].pinned);

        let status = client.status().unwrap();
        assert_eq!(status.content_count, 1);
        assert!(status.stored_bytes > 0);

        std::fs::remove_dir_all(&dir).ok();
    }
}
