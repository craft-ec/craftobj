//! Storage receipt signing and verification.
//!
//! These functions are used by the PDP challenger system (kernel-level)
//! for signing and verifying storage proofs.

use ed25519_dalek::{Signer, Verifier, SigningKey, VerifyingKey, Signature};

use crate::StorageReceipt;

/// Sign a storage receipt with the challenger's ed25519 key.
pub fn sign_storage_receipt(receipt: &mut StorageReceipt, key: &SigningKey) {
    let data = receipt.signable_data();
    let sig = key.sign(&data);
    receipt.signature = sig.to_bytes().to_vec();
}

/// Verify a storage receipt signature.
pub fn verify_storage_receipt(receipt: &StorageReceipt, pubkey: &VerifyingKey) -> bool {
    if receipt.signature.len() != 64 {
        return false;
    }
    let data = receipt.signable_data();
    let sig = match Signature::from_slice(&receipt.signature) {
        Ok(s) => s,
        Err(_) => return false,
    };
    pubkey.verify(&data, &sig).is_ok()
}

/// Convert a libp2p PeerId to a 32-byte ed25519 public key (best effort).
///
/// This is a helper for the PDP system. Returns zeros if the peer ID
/// doesn't encode an ed25519 key.
pub fn peer_id_to_ed25519_pubkey(peer_id_bytes: &[u8]) -> [u8; 32] {
    // PeerId multihash: 0x00 0x24 0x08 0x01 0x12 0x20 <32 bytes>
    if peer_id_bytes.len() >= 38 {
        let mut out = [0u8; 32];
        out.copy_from_slice(&peer_id_bytes[6..38]);
        out
    } else {
        [0u8; 32]
    }
}
