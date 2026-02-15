//! DataCraft Client
//!
//! High-level orchestration API for publishing, fetching, pinning content.
//!
//! # Lifecycle
//!
//! - `publish(path, options)` → hash → chunk → erasure encode → store → announce → CID
//! - `fetch(cid, dest)` → resolve → download manifest → download shards → decode → verify → write
//! - `pin(cid)` / `unpin(cid)` / `list()` / `status()`

pub mod extension;

use std::path::{Path, PathBuf};

use craftec_erasure::ErasureCoder;
use datacraft_core::{
    ChunkManifest, ContentId, DataCraftError, PublishOptions, Result,
    access::{self, AccessEntry},
    default_erasure_config,
    pre::{self, EncryptedContentKey, ReEncryptedKey, ReKeyEntry},
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
    pub chunk_count: usize,
}

/// Content info returned by list().
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ContentInfo {
    pub content_id: ContentId,
    pub total_size: u64,
    pub chunk_count: usize,
    pub pinned: bool,
}

/// Node status information.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NodeStatus {
    pub stored_bytes: u64,
    pub content_count: usize,
    pub pinned_count: usize,
}

/// Result of a revoke-and-rotate operation.
#[derive(Debug, Clone)]
pub struct RevocationResult {
    /// New content ID (hash of re-encrypted content).
    pub new_content_id: ContentId,
    /// New encryption key.
    pub new_encryption_key: Vec<u8>,
    /// Total size of re-encrypted content.
    pub new_total_size: u64,
    /// Number of chunks.
    pub new_chunk_count: usize,
    /// Content key encrypted to creator (for DHT AccessList).
    pub encrypted_content_key: pre::EncryptedContentKey,
    /// Re-key entries + re-encrypted keys for remaining authorized users.
    pub re_grants: Vec<(pre::ReKeyEntry, pre::ReEncryptedKey)>,
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
        let chunk_count = chunks.len();

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
            content_hash: content_id.0,
            k: config.data_shards,
            chunk_size: config.chunk_size,
            chunk_count,
            erasure_config: config,
            content_size: total_size,
            creator: String::new(),
            signature: vec![],
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

        let mut reconstructed = Vec::with_capacity(manifest.content_size as usize);

        for chunk_idx in 0..manifest.chunk_count as u32 {
            // Compute the actual data size for this chunk
            let chunk_size = if (chunk_idx as usize) < manifest.chunk_count - 1 {
                manifest.chunk_size
            } else {
                // Last chunk: remaining bytes
                manifest.content_size as usize - (chunk_idx as usize * manifest.chunk_size)
            };

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

        // Decrypt if key provided
        let final_data = if let Some(key) = encryption_key {
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
                    total_size: manifest.content_size,
                    chunk_count: manifest.chunk_count,
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
                stored_bytes += manifest.content_size;
            }
        }
        Ok(NodeStatus {
            stored_bytes,
            content_count: content.len(),
            pinned_count: self.pin_manager.list_pinned().len(),
        })
    }

    /// Publish with PRE: encrypts content and stores encrypted content key
    /// for the creator. Returns the encrypted content key alongside the publish result.
    ///
    /// The encrypted content key should be stored in DHT metadata.
    pub fn publish_with_pre(
        &mut self,
        path: &Path,
        options: &PublishOptions,
        creator_keypair: &ed25519_dalek::SigningKey,
    ) -> Result<(PublishResult, EncryptedContentKey)> {
        let mut opts = options.clone();
        opts.encrypted = true;

        let result = self.publish(path, &opts)?;
        let content_key = result
            .encryption_key
            .as_ref()
            .ok_or_else(|| DataCraftError::EncryptionError("no encryption key".into()))?;

        let encrypted_ck = pre::encrypt_content_key(creator_keypair, content_key)?;
        Ok((result, encrypted_ck))
    }

    /// Grant access to a recipient: generates a re-encryption key and
    /// re-encrypted content key that the recipient can decrypt.
    ///
    /// Returns (ReKeyEntry, ReEncryptedKey) to store in DHT.
    pub fn grant_access(
        &self,
        creator_keypair: &ed25519_dalek::SigningKey,
        recipient_pubkey: &ed25519_dalek::VerifyingKey,
        content_key: &[u8],
    ) -> Result<(ReKeyEntry, ReEncryptedKey)> {
        let re_key = pre::generate_re_key(creator_keypair, recipient_pubkey)?;
        let re_encrypted = pre::re_encrypt_with_content_key(content_key, &re_key)?;
        let entry = ReKeyEntry {
            recipient_did: recipient_pubkey.to_bytes(),
            re_key,
        };
        Ok((entry, re_encrypted))
    }

    /// Grant access via AccessList (direct key encryption, not PRE).
    ///
    /// Creates an AccessEntry for the recipient.
    pub fn grant_access_direct(
        &self,
        content_id: &ContentId,
        creator_keypair: &ed25519_dalek::SigningKey,
        recipient_pubkey: &ed25519_dalek::VerifyingKey,
        content_key: &[u8],
    ) -> Result<AccessEntry> {
        access::grant_access(content_id, creator_keypair, recipient_pubkey, content_key)
    }

    /// Reconstruct content using a re-encrypted content key (PRE flow).
    ///
    /// The recipient fetches the re-encrypted key from DHT, decrypts it
    /// with their private key, then uses the content key to decrypt content.
    pub fn reconstruct_with_pre(
        &self,
        content_id: &ContentId,
        dest: &Path,
        re_encrypted_key: &ReEncryptedKey,
        recipient_keypair: &ed25519_dalek::SigningKey,
        creator_pubkey: &ed25519_dalek::VerifyingKey,
    ) -> Result<()> {
        let content_key = pre::decrypt_re_encrypted(
            re_encrypted_key,
            recipient_keypair,
            creator_pubkey,
        )?;
        self.reconstruct(content_id, dest, Some(&content_key))
    }

    /// Revoke a user's access and rotate the content key.
    ///
    /// This is the secure revocation flow:
    /// 1. Generate a new content key
    /// 2. Decrypt the original content, re-encrypt with the new key → new CID
    /// 3. Re-grant access to all remaining authorized users with the new key
    /// 4. Return the new publish result, encrypted content key, and re-key entries for remaining users
    ///
    /// The caller is responsible for:
    /// - Removing the revoked user's re-key from DHT (tombstone)
    /// - Storing the new AccessList + re-keys in DHT
    /// - Announcing the new CID to DHT
    pub fn revoke_and_rotate(
        &mut self,
        old_content_id: &ContentId,
        old_content_key: &[u8],
        creator_keypair: &ed25519_dalek::SigningKey,
        revoked_pubkey: &ed25519_dalek::VerifyingKey,
        all_authorized: &[ed25519_dalek::VerifyingKey],
    ) -> Result<RevocationResult> {
        // 1. Reconstruct original plaintext
        let manifest = self.store.get_manifest(old_content_id)?;
        let coder = ErasureCoder::with_config(&manifest.erasure_config)
            .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;
        let total_shards =
            manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards;

        let mut ciphertext = Vec::with_capacity(manifest.content_size as usize);
        for chunk_idx in 0..manifest.chunk_count as u32 {
            let chunk_size = if (chunk_idx as usize) < manifest.chunk_count - 1 {
                manifest.chunk_size
            } else {
                manifest.content_size as usize - (chunk_idx as usize * manifest.chunk_size)
            };
            let mut shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(total_shards);
            for shard_idx in 0..total_shards {
                match self.store.get_shard(old_content_id, chunk_idx, shard_idx as u8) {
                    Ok(data) => shards.push(Some(data)),
                    Err(_) => shards.push(None),
                }
            }
            let decoded = coder
                .decode(&mut shards, chunk_size)
                .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;
            ciphertext.extend_from_slice(&decoded);
        }

        // Decrypt to get plaintext
        let plaintext = decrypt_content(&ciphertext, old_content_key)?;

        // 2. Generate new content key and re-encrypt
        let new_key = generate_content_key();
        let new_ciphertext = encrypt_content(&plaintext, &new_key)?;
        let new_content_id = ContentId::from_bytes(&new_ciphertext);
        let total_size = new_ciphertext.len() as u64;

        // Chunk and erasure-encode
        let config = manifest.erasure_config;
        let chunks = chunk_data(&new_ciphertext, config.chunk_size);
        let chunk_count = chunks.len();
        let new_coder = ErasureCoder::with_config(&config)
            .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;

        for (chunk_idx, chunk) in chunks.iter().enumerate() {
            let shards = new_coder
                .encode(chunk)
                .map_err(|e| DataCraftError::ErasureError(e.to_string()))?;
            for (shard_idx, shard) in shards.iter().enumerate() {
                self.store
                    .put_shard(&new_content_id, chunk_idx as u32, shard_idx as u8, shard)?;
            }
        }

        let new_manifest = ChunkManifest {
            content_id: new_content_id,
            content_hash: new_content_id.0,
            k: config.data_shards,
            chunk_size: config.chunk_size,
            chunk_count,
            erasure_config: config,
            content_size: total_size,
            creator: String::new(),
            signature: vec![],
        };
        self.store.put_manifest(&new_manifest)?;
        self.pin_manager.pin(&new_content_id)?;

        // 3. Encrypt content key to creator
        let encrypted_ck = pre::encrypt_content_key(creator_keypair, &new_key)?;

        // 4. Re-grant access to remaining users (excluding revoked)
        let revoked_bytes = revoked_pubkey.to_bytes();
        let mut re_grants = Vec::new();
        for pubkey in all_authorized {
            if pubkey.to_bytes() == revoked_bytes {
                continue;
            }
            let (entry, re_encrypted) = self.grant_access(creator_keypair, pubkey, &new_key)?;
            re_grants.push((entry, re_encrypted));
        }

        info!(
            "Revoked access and rotated key: {} -> {} ({} remaining users)",
            old_content_id, new_content_id, re_grants.len()
        );

        Ok(RevocationResult {
            new_content_id,
            new_encryption_key: new_key,
            new_total_size: total_size,
            new_chunk_count: chunk_count,
            encrypted_content_key: encrypted_ck,
            re_grants,
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
    fn test_publish_encrypted_fetch_without_key_gets_ciphertext() {
        let dir = test_dir();
        let mut client = DataCraftClient::new(&dir).unwrap();

        let file_path = dir.join("secret2.txt");
        let plaintext = b"this is secret content that should be encrypted";
        std::fs::write(&file_path, plaintext).unwrap();

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

        // Reconstruct WITHOUT key — should get ciphertext (not plaintext)
        let output_no_key = dir.join("no_key_output.bin");
        client
            .reconstruct(&result.content_id, &output_no_key, None)
            .unwrap();
        let raw = std::fs::read(&output_no_key).unwrap();
        assert_ne!(raw, plaintext);
        // The raw data should be nonce (12) + ciphertext
        assert!(raw.len() > plaintext.len());

        // Reconstruct WITH key — should get plaintext
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
    fn test_publish_with_pre_and_grant_access() {
        use ed25519_dalek::SigningKey;

        let dir = test_dir();
        let mut client = DataCraftClient::new(&dir).unwrap();

        let creator = SigningKey::generate(&mut rand::thread_rng());
        let recipient = SigningKey::generate(&mut rand::thread_rng());

        // Publish with PRE
        let file_path = dir.join("pre_test.txt");
        let content = b"pre-encrypted content for access control testing";
        std::fs::write(&file_path, content).unwrap();

        let (result, _encrypted_ck) = client
            .publish_with_pre(&file_path, &PublishOptions::default(), &creator)
            .unwrap();

        let content_key = result.encryption_key.as_ref().unwrap();

        // Grant access to recipient
        let (_re_key_entry, re_encrypted) = client
            .grant_access(&creator, &recipient.verifying_key(), content_key)
            .unwrap();

        // Recipient decrypts using PRE
        let output = dir.join("pre_output.txt");
        client
            .reconstruct_with_pre(
                &result.content_id,
                &output,
                &re_encrypted,
                &recipient,
                &creator.verifying_key(),
            )
            .unwrap();

        assert_eq!(std::fs::read(&output).unwrap(), content);

        // Wrong recipient cannot decrypt
        let wrong = SigningKey::generate(&mut rand::thread_rng());
        let output2 = dir.join("pre_output_wrong.txt");
        let err = client
            .reconstruct_with_pre(
                &result.content_id,
                &output2,
                &re_encrypted,
                &wrong,
                &creator.verifying_key(),
            );
        assert!(err.is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_revoke_and_rotate_full_round_trip() {
        use ed25519_dalek::SigningKey;

        let dir = test_dir();
        let mut client = DataCraftClient::new(&dir).unwrap();

        let creator = SigningKey::generate(&mut rand::thread_rng());
        let user_a = SigningKey::generate(&mut rand::thread_rng());
        let user_b = SigningKey::generate(&mut rand::thread_rng());
        let user_c = SigningKey::generate(&mut rand::thread_rng());

        // Publish encrypted content with PRE
        let file_path = dir.join("revoke_test.txt");
        let content = b"secret content for revocation testing with key rotation";
        std::fs::write(&file_path, content).unwrap();

        let (result, _encrypted_ck) = client
            .publish_with_pre(&file_path, &PublishOptions::default(), &creator)
            .unwrap();
        let content_key = result.encryption_key.as_ref().unwrap().clone();

        // Grant access to all 3 users
        let all_users = vec![
            user_a.verifying_key(),
            user_b.verifying_key(),
            user_c.verifying_key(),
        ];
        let mut re_encrypted_keys = Vec::new();
        for user in &all_users {
            let (_entry, re_enc) = client
                .grant_access(&creator, user, &content_key)
                .unwrap();
            re_encrypted_keys.push(re_enc);
        }

        // Verify all 3 users can decrypt
        for (i, user) in [&user_a, &user_b, &user_c].iter().enumerate() {
            let out = dir.join(format!("pre_revoke_{}.txt", i));
            client
                .reconstruct_with_pre(
                    &result.content_id,
                    &out,
                    &re_encrypted_keys[i],
                    user,
                    &creator.verifying_key(),
                )
                .unwrap();
            assert_eq!(std::fs::read(&out).unwrap(), content);
        }

        // Revoke user_b and rotate
        let revocation = client
            .revoke_and_rotate(
                &result.content_id,
                &content_key,
                &creator,
                &user_b.verifying_key(),
                &all_users,
            )
            .unwrap();

        // Verify: 2 remaining users got re-grants (not user_b)
        assert_eq!(revocation.re_grants.len(), 2);

        // Verify remaining users (A and C) can decrypt the NEW content
        for (entry, re_enc) in &revocation.re_grants {
            let recipient_key = if entry.recipient_did == user_a.verifying_key().to_bytes() {
                &user_a
            } else {
                &user_c
            };
            let out = dir.join(format!("post_revoke_{}.txt", hex::encode(&entry.recipient_did[..4])));
            client
                .reconstruct_with_pre(
                    &revocation.new_content_id,
                    &out,
                    re_enc,
                    recipient_key,
                    &creator.verifying_key(),
                )
                .unwrap();
            assert_eq!(std::fs::read(&out).unwrap(), content);
        }

        // Verify user_b CANNOT decrypt new content with old re-encrypted key
        let out_revoked = dir.join("post_revoke_b.txt");
        let err = client.reconstruct_with_pre(
            &revocation.new_content_id,
            &out_revoked,
            &re_encrypted_keys[1], // user_b's old re-encrypted key
            &user_b,
            &creator.verifying_key(),
        );
        assert!(err.is_err());

        // Verify user_b cannot decrypt even if they try old CID with old key
        // (old content still exists, but that's expected — the point is new content is protected)

        // Verify new CID is different from old
        assert_ne!(result.content_id, revocation.new_content_id);

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
