//! Access control for encrypted content.
//!
//! Stores access metadata (encrypted content keys) in the DHT alongside CIDs.
//! Uses x25519 ECDH + ChaCha20-Poly1305 to encrypt the content key to each recipient.

use chacha20poly1305::{aead::{Aead, KeyInit}, ChaCha20Poly1305, Nonce};
use ed25519_dalek::{SigningKey, VerifyingKey, Signer, Verifier, Signature};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x25519_dalek::{EphemeralSecret, PublicKey as X25519PublicKey, StaticSecret};

use crate::{ContentId, DataCraftError, Result};

/// A content key encrypted to a specific recipient's public key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessEntry {
    /// Recipient's ed25519 public key (DID identity).
    pub recipient_did: [u8; 32],
    /// Content key encrypted via ECDH(ephemeral, recipient_x25519) + ChaCha20-Poly1305.
    /// Format: [ephemeral_pubkey:32][nonce:12][ciphertext]
    pub encrypted_key: Vec<u8>,
}

/// Full access list for a piece of content, signed by the creator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessList {
    /// Content this access list controls.
    pub content_id: ContentId,
    /// Creator's ed25519 public key.
    pub creator_did: [u8; 32],
    /// Content key encrypted to creator (so creator can always decrypt).
    pub encrypted_creator_key: Vec<u8>,
    /// Access entries for each granted recipient.
    pub entries: Vec<AccessEntry>,
    /// Creator's signature over the serializable fields.
    pub signature: Vec<u8>,
}

impl AccessList {
    /// Data that gets signed (everything except signature).
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.content_id.0);
        data.extend_from_slice(&self.creator_did);
        data.extend_from_slice(&(self.encrypted_creator_key.len() as u32).to_le_bytes());
        data.extend_from_slice(&self.encrypted_creator_key);
        data.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for entry in &self.entries {
            data.extend_from_slice(&entry.recipient_did);
            data.extend_from_slice(&(entry.encrypted_key.len() as u32).to_le_bytes());
            data.extend_from_slice(&entry.encrypted_key);
        }
        data
    }

    /// Verify the creator's signature.
    pub fn verify(&self) -> Result<()> {
        let verifying_key = VerifyingKey::from_bytes(&self.creator_did)
            .map_err(|e| DataCraftError::EncryptionError(format!("invalid creator key: {e}")))?;
        let sig = Signature::from_slice(&self.signature)
            .map_err(|e| DataCraftError::EncryptionError(format!("invalid signature: {e}")))?;
        verifying_key
            .verify(&self.signable_data(), &sig)
            .map_err(|e| DataCraftError::EncryptionError(format!("signature verification failed: {e}")))
    }

    /// Serialize for DHT storage.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self)
            .map_err(|e| DataCraftError::EncryptionError(format!("serialization failed: {e}")))
    }

    /// Deserialize from DHT storage.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data)
            .map_err(|e| DataCraftError::EncryptionError(format!("deserialization failed: {e}")))
    }
}

/// Convert ed25519 signing key to x25519 static secret.
fn ed25519_to_x25519_secret(signing_key: &SigningKey) -> StaticSecret {
    let hash = Sha256::digest(signing_key.as_bytes());
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&hash);
    // Clamp for x25519
    key_bytes[0] &= 248;
    key_bytes[31] &= 127;
    key_bytes[31] |= 64;
    StaticSecret::from(key_bytes)
}

/// Convert ed25519 verifying key to x25519 public key.
///
/// Uses a hash-based derivation for compatibility (not the standard
/// edwards-to-montgomery conversion, which requires accessing the
/// internal edwards point). Both sides must use the same derivation.
fn ed25519_to_x25519_public(verifying_key: &VerifyingKey) -> X25519PublicKey {
    let hash = Sha256::digest(verifying_key.as_bytes());
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&hash);
    key_bytes[0] &= 248;
    key_bytes[31] &= 127;
    key_bytes[31] |= 64;
    // Derive the public key from this as a static secret
    let secret = StaticSecret::from(key_bytes);
    X25519PublicKey::from(&secret)
}

/// Encrypt a content key to a recipient's ed25519 public key.
///
/// Uses ephemeral ECDH + ChaCha20-Poly1305.
pub fn grant_access(
    content_id: &ContentId,
    _creator_keypair: &SigningKey, // used for context, not for encryption here
    recipient_pubkey: &VerifyingKey,
    content_key: &[u8],
) -> Result<AccessEntry> {
    let recipient_x25519 = ed25519_to_x25519_public(recipient_pubkey);

    // Ephemeral ECDH
    let ephemeral_secret = EphemeralSecret::random_from_rng(rand::thread_rng());
    let ephemeral_public = X25519PublicKey::from(&ephemeral_secret);
    let shared_secret = ephemeral_secret.diffie_hellman(&recipient_x25519);

    // Derive symmetric key from shared secret
    let sym_key = Sha256::digest(shared_secret.as_bytes());

    let cipher = ChaCha20Poly1305::new_from_slice(&sym_key)
        .map_err(|e| DataCraftError::EncryptionError(e.to_string()))?;

    let mut nonce_bytes = [0u8; 12];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, content_key)
        .map_err(|e| DataCraftError::EncryptionError(e.to_string()))?;

    // Pack: [ephemeral_pubkey:32][nonce:12][ciphertext]
    let mut encrypted_key = Vec::with_capacity(32 + 12 + ciphertext.len());
    encrypted_key.extend_from_slice(ephemeral_public.as_bytes());
    encrypted_key.extend_from_slice(&nonce_bytes);
    encrypted_key.extend_from_slice(&ciphertext);

    let _ = content_id; // for future: could bind to content_id in AAD

    Ok(AccessEntry {
        recipient_did: recipient_pubkey.to_bytes(),
        encrypted_key,
    })
}

/// Decrypt an access entry using the recipient's signing key.
pub fn decrypt_access_entry(
    entry: &AccessEntry,
    recipient_keypair: &SigningKey,
) -> Result<Vec<u8>> {
    if entry.encrypted_key.len() < 32 + 12 + 16 {
        return Err(DataCraftError::EncryptionError("encrypted key too short".into()));
    }

    let ephemeral_pubkey = X25519PublicKey::from({
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&entry.encrypted_key[..32]);
        bytes
    });
    let nonce_bytes = &entry.encrypted_key[32..44];
    let ciphertext = &entry.encrypted_key[44..];

    let recipient_x25519 = ed25519_to_x25519_secret(recipient_keypair);
    let shared_secret = recipient_x25519.diffie_hellman(&ephemeral_pubkey);
    let sym_key = Sha256::digest(shared_secret.as_bytes());

    let cipher = ChaCha20Poly1305::new_from_slice(&sym_key)
        .map_err(|e| DataCraftError::EncryptionError(e.to_string()))?;
    let nonce = Nonce::from_slice(nonce_bytes);

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| DataCraftError::EncryptionError(format!("decryption failed: {e}")))
}

/// Create a signed AccessList for a content item.
pub fn create_access_list(
    content_id: &ContentId,
    creator_keypair: &SigningKey,
    content_key: &[u8],
    recipient_entries: Vec<AccessEntry>,
) -> Result<AccessList> {
    let creator_verifying = creator_keypair.verifying_key();
    let creator_entry = grant_access(content_id, creator_keypair, &creator_verifying, content_key)?;

    let mut list = AccessList {
        content_id: *content_id,
        creator_did: creator_verifying.to_bytes(),
        encrypted_creator_key: creator_entry.encrypted_key,
        entries: recipient_entries,
        signature: Vec::new(),
    };

    let sig = creator_keypair.sign(&list.signable_data());
    list.signature = sig.to_bytes().to_vec();

    Ok(list)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_keypair() -> SigningKey {
        SigningKey::generate(&mut rand::thread_rng())
    }

    #[test]
    fn test_grant_and_decrypt_access() {
        let creator = test_keypair();
        let recipient = test_keypair();
        let content_id = ContentId::from_bytes(b"test content");
        let content_key = b"0123456789abcdef0123456789abcdef"; // 32 bytes

        let entry = grant_access(
            &content_id,
            &creator,
            &recipient.verifying_key(),
            content_key,
        )
        .unwrap();

        assert_eq!(entry.recipient_did, recipient.verifying_key().to_bytes());

        let decrypted = decrypt_access_entry(&entry, &recipient).unwrap();
        assert_eq!(decrypted, content_key);
    }

    #[test]
    fn test_wrong_recipient_cannot_decrypt() {
        let creator = test_keypair();
        let recipient = test_keypair();
        let wrong_recipient = test_keypair();
        let content_id = ContentId::from_bytes(b"test");
        let content_key = b"0123456789abcdef0123456789abcdef";

        let entry = grant_access(
            &content_id,
            &creator,
            &recipient.verifying_key(),
            content_key,
        )
        .unwrap();

        let result = decrypt_access_entry(&entry, &wrong_recipient);
        assert!(result.is_err());
    }

    #[test]
    fn test_access_list_create_verify() {
        let creator = test_keypair();
        let recipient = test_keypair();
        let content_id = ContentId::from_bytes(b"access list test");
        let content_key = b"0123456789abcdef0123456789abcdef";

        let entry = grant_access(
            &content_id,
            &creator,
            &recipient.verifying_key(),
            content_key,
        )
        .unwrap();

        let list = create_access_list(&content_id, &creator, content_key, vec![entry]).unwrap();

        // Verify signature
        list.verify().unwrap();

        // Creator can decrypt their own key
        let creator_entry = AccessEntry {
            recipient_did: creator.verifying_key().to_bytes(),
            encrypted_key: list.encrypted_creator_key.clone(),
        };
        let decrypted = decrypt_access_entry(&creator_entry, &creator).unwrap();
        assert_eq!(decrypted, content_key);

        // Recipient can decrypt their entry
        let decrypted = decrypt_access_entry(&list.entries[0], &recipient).unwrap();
        assert_eq!(decrypted, content_key);
    }

    #[test]
    fn test_access_list_tampered_signature_fails() {
        let creator = test_keypair();
        let content_id = ContentId::from_bytes(b"tamper test");
        let content_key = b"0123456789abcdef0123456789abcdef";

        let mut list = create_access_list(&content_id, &creator, content_key, vec![]).unwrap();
        list.content_id = ContentId::from_bytes(b"different");

        assert!(list.verify().is_err());
    }

    #[test]
    fn test_access_list_serialization() {
        let creator = test_keypair();
        let content_id = ContentId::from_bytes(b"serde test");
        let content_key = b"0123456789abcdef0123456789abcdef";

        let list = create_access_list(&content_id, &creator, content_key, vec![]).unwrap();

        let bytes = list.to_bytes().unwrap();
        let restored = AccessList::from_bytes(&bytes).unwrap();

        assert_eq!(restored.content_id, list.content_id);
        assert_eq!(restored.creator_did, list.creator_did);
        restored.verify().unwrap();
    }
}
