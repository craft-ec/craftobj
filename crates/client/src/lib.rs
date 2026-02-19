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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use craftec_erasure::{
    check_independence, homomorphic, segmenter, CodedPiece, ContentVerificationRecord,
    ErasureConfig,
};
use craftobj_core::{
    ContentId, ContentManifest, CraftObjError, PublishOptions, RemovalNotice, Result,
    access::{self, AccessEntry},
    pre::{self, EncryptedContentKey, ReEncryptedKey, ReKeyEntry},
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
    /// The manifest (for daemon to announce).
    pub manifest: ContentManifest,
    /// Verification record with homomorphic hashes for piece verification.
    pub verification_record: ContentVerificationRecord,
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

/// Result of a revoke-and-rotate operation.
#[derive(Debug, Clone)]
pub struct RevocationResult {
    /// New content ID (hash of re-encrypted content).
    pub new_content_id: ContentId,
    /// New encryption key.
    pub new_encryption_key: Vec<u8>,
    /// Total size of re-encrypted content.
    pub new_total_size: u64,
    /// Number of segments.
    pub new_segment_count: usize,
    /// Content key encrypted to creator (for DHT AccessList).
    pub encrypted_content_key: pre::EncryptedContentKey,
    /// Re-key entries + re-encrypted keys for remaining authorized users.
    pub re_grants: Vec<(pre::ReKeyEntry, pre::ReEncryptedKey)>,
}

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

        let config = ErasureConfig::default();

        // Segment and RLNC encode
        let encoded_segments = segmenter::segment_and_encode(&content_bytes, &config)
            .map_err(|e| CraftObjError::ErasureError(e.to_string()))?;

        let segment_count = encoded_segments.len();

        info!(
            "Publishing {} ({} bytes, {} segments, piece_size={})",
            content_id, total_size, segment_count, config.piece_size,
        );

        // Store all pieces and compute homomorphic hashes
        let mut segment_hashes = Vec::with_capacity(encoded_segments.len());
        for (seg_idx, pieces) in &encoded_segments {
            // Compute homomorphic hashes for original (source) pieces only
            // Source pieces have identity coefficient vectors (exactly one 1, rest 0)
            let k = config.k_for_segment(
                if (*seg_idx as usize + 1) < segment_count {
                    config.segment_size
                } else {
                    content_bytes.len() - *seg_idx as usize * config.segment_size
                },
            );
            let source_pieces: Vec<&CodedPiece> = pieces.iter().take(k).collect();
            let seed = {
                let mut s = [0u8; 32];
                use sha2::Digest;
                let hash = sha2::Sha256::digest(
                    &[&content_id.0[..], &seg_idx.to_le_bytes()[..]].concat(),
                );
                s.copy_from_slice(&hash);
                s
            };
            let hashes = homomorphic::generate_segment_hashes(
                seed,
                &source_pieces.iter().map(|p| (*p).clone()).collect::<Vec<_>>(),
            );
            segment_hashes.push(hashes);

            for piece in pieces {
                let pid = piece_id_from_coefficients(&piece.coefficients);
                self.store.store_piece(
                    &content_id,
                    *seg_idx,
                    &pid,
                    &piece.data,
                    &piece.coefficients,
                )?;
            }
        }

        // Store verification record (homomorphic hashes)
        let verification_record = ContentVerificationRecord {
            file_size: total_size,
            segment_hashes,
        };
        self.store.store_verification_record(&content_id, &verification_record)?;

        // Build and store manifest
        let content_hash = content_id.0; // CID is SHA-256 of content bytes
        let manifest = ContentManifest {
            content_id,
            content_hash,
            segment_size: config.segment_size,
            piece_size: config.piece_size,
            segment_count,
            total_size,
            creator: String::new(),
            signature: vec![],
        };
        self.store.store_manifest(&manifest)?;

        // Auto-pin published content
        self.pin_manager.pin(&content_id)?;

        info!("Published {} successfully", content_id);

        Ok(PublishResult {
            content_id,
            encryption_key,
            total_size,
            segment_count,
            manifest,
            verification_record,
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
        keypair: &ed25519_dalek::SigningKey,
    ) -> Result<PublishResult> {
        let mut result = self.publish(path, options)?;

        let mut manifest = self.store.get_manifest(&result.content_id)?;
        manifest.sign(keypair);
        self.store.store_manifest(&manifest)?;
        result.manifest = manifest;

        Ok(result)
    }

    /// Create a removal notice for content, signed by the creator.
    pub fn remove_content(
        &mut self,
        keypair: &ed25519_dalek::SigningKey,
        content_id: &ContentId,
        reason: Option<String>,
    ) -> Result<RemovalNotice> {
        let notice = RemovalNotice::sign(keypair, *content_id, reason);
        let _ = self.pin_manager.unpin(content_id);
        info!("Created removal notice for {}", content_id);
        Ok(notice)
    }

    /// Reconstruct content from locally stored pieces using RLNC decoding.
    ///
    /// For each segment, collects pieces from store, checks linear independence,
    /// and decodes once k independent pieces are available.
    pub fn reconstruct(
        &self,
        content_id: &ContentId,
        dest: &Path,
        encryption_key: Option<&[u8]>,
    ) -> Result<()> {
        let manifest = self.store.get_manifest(content_id)?;
        let config = ErasureConfig {
            piece_size: manifest.piece_size,
            segment_size: manifest.segment_size,
            ..Default::default()
        };

        let mut segments: BTreeMap<u32, Vec<CodedPiece>> = BTreeMap::new();

        for seg_idx in 0..manifest.segment_count as u32 {
            let k = manifest.k_for_segment(seg_idx as usize);
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
            manifest.segment_count as u32,
            &config,
            manifest.total_size as usize,
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
            if let Ok(manifest) = self.store.get_manifest(&cid) {
                result.push(ContentInfo {
                    content_id: cid,
                    total_size: manifest.total_size,
                    segment_count: manifest.segment_count,
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
            if let Ok(manifest) = self.store.get_manifest(cid) {
                stored_bytes += manifest.total_size;
                for seg in 0..manifest.segment_count as u32 {
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

    /// Publish with PRE: encrypts content and stores encrypted content key for the creator.
    pub fn publish_with_pre(
        &mut self,
        path: &Path,
        options: &PublishOptions,
        creator_keypair: &ed25519_dalek::SigningKey,
    ) -> Result<(PublishResult, EncryptedContentKey)> {
        let mut opts = options.clone();
        opts.encrypted = true;

        let result = self.publish_signed(path, &opts, creator_keypair)?;
        let content_key = result
            .encryption_key
            .as_ref()
            .ok_or_else(|| CraftObjError::EncryptionError("no encryption key".into()))?;

        let encrypted_ck = pre::encrypt_content_key(creator_keypair, content_key)?;
        Ok((result, encrypted_ck))
    }

    /// Grant access to a recipient: generates a re-encryption key and
    /// re-encrypted content key that the recipient can decrypt.
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
    pub fn reconstruct_with_pre(
        &self,
        content_id: &ContentId,
        dest: &Path,
        re_encrypted_key: &ReEncryptedKey,
        recipient_keypair: &ed25519_dalek::SigningKey,
        creator_pubkey: &ed25519_dalek::VerifyingKey,
    ) -> Result<()> {
        let content_key =
            pre::decrypt_re_encrypted(re_encrypted_key, recipient_keypair, creator_pubkey)?;
        self.reconstruct(content_id, dest, Some(&content_key))
    }

    /// Revoke a user's access and rotate the content key.
    pub fn revoke_and_rotate(
        &mut self,
        old_content_id: &ContentId,
        old_content_key: &[u8],
        creator_keypair: &ed25519_dalek::SigningKey,
        revoked_pubkey: &ed25519_dalek::VerifyingKey,
        all_authorized: &[ed25519_dalek::VerifyingKey],
    ) -> Result<RevocationResult> {
        // 1. Reconstruct original ciphertext from pieces
        let manifest = self.store.get_manifest(old_content_id)?;
        let config = ErasureConfig {
            piece_size: manifest.piece_size,
            segment_size: manifest.segment_size,
            ..Default::default()
        };

        let mut segments: BTreeMap<u32, Vec<CodedPiece>> = BTreeMap::new();
        for seg_idx in 0..manifest.segment_count as u32 {
            let piece_ids = self.store.list_pieces(old_content_id, seg_idx)?;
            let mut collected: Vec<CodedPiece> = Vec::new();
            for pid in &piece_ids {
                let (data, coefficients) =
                    self.store.get_piece(old_content_id, seg_idx, pid)?;
                collected.push(CodedPiece { data, coefficients });
            }
            segments.insert(seg_idx, collected);
        }

        let ciphertext = segmenter::decode_and_reassemble(
            &segments,
            manifest.segment_count as u32,
            &config,
            manifest.total_size as usize,
        )
        .map_err(|e| CraftObjError::ErasureError(e.to_string()))?;

        // Decrypt to get plaintext
        let plaintext = decrypt_content(&ciphertext, old_content_key)?;

        // 2. Re-encrypt with new key and publish
        let new_key = generate_content_key();
        let new_ciphertext = encrypt_content(&plaintext, &new_key)?;
        let new_content_id = ContentId::from_bytes(&new_ciphertext);
        let total_size = new_ciphertext.len() as u64;

        let encoded_segments = segmenter::segment_and_encode(&new_ciphertext, &config)
            .map_err(|e| CraftObjError::ErasureError(e.to_string()))?;
        let segment_count = encoded_segments.len();

        let mut new_segment_hashes = Vec::with_capacity(encoded_segments.len());
        for (seg_idx, pieces) in &encoded_segments {
            let k = config.k_for_segment(
                if (*seg_idx as usize + 1) < segment_count {
                    config.segment_size
                } else {
                    new_ciphertext.len() - *seg_idx as usize * config.segment_size
                },
            );
            let source_pieces: Vec<CodedPiece> = pieces.iter().take(k).cloned().collect();
            let seed = {
                let mut s = [0u8; 32];
                use sha2::Digest;
                let hash = sha2::Sha256::digest(
                    &[&new_content_id.0[..], &seg_idx.to_le_bytes()[..]].concat(),
                );
                s.copy_from_slice(&hash);
                s
            };
            new_segment_hashes.push(homomorphic::generate_segment_hashes(seed, &source_pieces));

            for piece in pieces {
                let pid = piece_id_from_coefficients(&piece.coefficients);
                self.store.store_piece(
                    &new_content_id,
                    *seg_idx,
                    &pid,
                    &piece.data,
                    &piece.coefficients,
                )?;
            }
        }

        let new_verification_record = ContentVerificationRecord {
            file_size: total_size,
            segment_hashes: new_segment_hashes,
        };
        self.store.store_verification_record(&new_content_id, &new_verification_record)?;

        let new_manifest = ContentManifest {
            content_id: new_content_id,
            content_hash: new_content_id.0,
            segment_size: config.segment_size,
            piece_size: config.piece_size,
            segment_count,
            total_size,
            creator: String::new(),
            signature: vec![],
        };
        self.store.store_manifest(&new_manifest)?;
        self.pin_manager.pin(&new_content_id)?;

        // 3. Encrypt content key to creator
        let encrypted_ck = pre::encrypt_content_key(creator_keypair, &new_key)?;

        // 4. Re-grant access to remaining users
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
            new_segment_count: segment_count,
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

    #[test]
    fn test_publish_with_pre_and_grant_access() {
        use ed25519_dalek::SigningKey;

        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();

        let creator = SigningKey::generate(&mut rand::thread_rng());
        let recipient = SigningKey::generate(&mut rand::thread_rng());

        let file_path = dir.join("pre_test.txt");
        let content = b"pre-encrypted content for access control testing";
        std::fs::write(&file_path, content).unwrap();

        let (result, _encrypted_ck) = client
            .publish_with_pre(&file_path, &PublishOptions::default(), &creator)
            .unwrap();

        let content_key = result.encryption_key.as_ref().unwrap();

        let (_re_key_entry, re_encrypted) = client
            .grant_access(&creator, &recipient.verifying_key(), content_key)
            .unwrap();

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

        let wrong = SigningKey::generate(&mut rand::thread_rng());
        let output2 = dir.join("pre_output_wrong.txt");
        let err = client.reconstruct_with_pre(
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
        let mut client = CraftObjClient::new(&dir).unwrap();

        let creator = SigningKey::generate(&mut rand::thread_rng());
        let user_a = SigningKey::generate(&mut rand::thread_rng());
        let user_b = SigningKey::generate(&mut rand::thread_rng());
        let user_c = SigningKey::generate(&mut rand::thread_rng());

        let file_path = dir.join("revoke_test.txt");
        let content = b"secret content for revocation testing with key rotation";
        std::fs::write(&file_path, content).unwrap();

        let (result, _encrypted_ck) = client
            .publish_with_pre(&file_path, &PublishOptions::default(), &creator)
            .unwrap();
        let content_key = result.encryption_key.as_ref().unwrap().clone();

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

        let revocation = client
            .revoke_and_rotate(
                &result.content_id,
                &content_key,
                &creator,
                &user_b.verifying_key(),
                &all_users,
            )
            .unwrap();

        assert_eq!(revocation.re_grants.len(), 2);

        for (entry, re_enc) in &revocation.re_grants {
            let recipient_key = if entry.recipient_did == user_a.verifying_key().to_bytes() {
                &user_a
            } else {
                &user_c
            };
            let out = dir.join(format!(
                "post_revoke_{}.txt",
                hex::encode(&entry.recipient_did[..4])
            ));
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

        let out_revoked = dir.join("post_revoke_b.txt");
        let err = client.reconstruct_with_pre(
            &revocation.new_content_id,
            &out_revoked,
            &re_encrypted_keys[1],
            &user_b,
            &creator.verifying_key(),
        );
        assert!(err.is_err());
        assert_ne!(result.content_id, revocation.new_content_id);

        std::fs::remove_dir_all(&dir).ok();
    }

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

        let manifest = client.store().get_manifest(&result.content_id).unwrap();
        assert!(!manifest.creator.is_empty());
        assert!(manifest.verify_creator());
        assert_eq!(
            manifest.creator_pubkey().unwrap(),
            keypair.verifying_key().to_bytes()
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_remove_content_creates_valid_notice() {
        use ed25519_dalek::SigningKey;

        let dir = test_dir();
        let mut client = CraftObjClient::new(&dir).unwrap();
        let keypair = SigningKey::generate(&mut rand::thread_rng());

        let file_path = dir.join("to_remove.txt");
        std::fs::write(&file_path, b"content to remove").unwrap();

        let result = client
            .publish_signed(&file_path, &PublishOptions::default(), &keypair)
            .unwrap();
        assert!(client.is_pinned(&result.content_id));

        let notice = client
            .remove_content(&keypair, &result.content_id, Some("testing".into()))
            .unwrap();

        assert!(notice.verify());
        assert_eq!(notice.cid, result.content_id);
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
