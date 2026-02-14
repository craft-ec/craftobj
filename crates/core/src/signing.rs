//! Receipt signing and verification using ed25519.
//!
//! Provides helpers to sign and verify `TransferReceipt` and `StorageReceipt`
//! using ed25519-dalek keypairs.

use ed25519_dalek::{Signature, Signer, Verifier, SigningKey, VerifyingKey};

use crate::{StorageReceipt, TransferReceipt};

/// Sign a `TransferReceipt` with the requester's signing key.
/// Fills the `signature` field in place.
pub fn sign_transfer_receipt(receipt: &mut TransferReceipt, keypair: &SigningKey) {
    let data = receipt.signable_data();
    let sig: Signature = keypair.sign(&data);
    receipt.signature = sig.to_bytes().to_vec();
}

/// Verify a `TransferReceipt`'s signature against the requester's public key.
pub fn verify_transfer_receipt(receipt: &TransferReceipt, pubkey: &VerifyingKey) -> bool {
    if receipt.signature.len() != 64 {
        return false;
    }
    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(&receipt.signature);
    let sig = Signature::from_bytes(&sig_bytes);
    let data = receipt.signable_data();
    pubkey.verify(&data, &sig).is_ok()
}

/// Sign a `StorageReceipt` with the challenger's signing key.
/// Fills the `signature` field in place.
pub fn sign_storage_receipt(receipt: &mut StorageReceipt, keypair: &SigningKey) {
    let data = receipt.signable_data();
    let sig: Signature = keypair.sign(&data);
    receipt.signature = sig.to_bytes().to_vec();
}

/// Verify a `StorageReceipt`'s signature against the challenger's public key.
pub fn verify_storage_receipt(receipt: &StorageReceipt, pubkey: &VerifyingKey) -> bool {
    if receipt.signature.len() != 64 {
        return false;
    }
    let mut sig_bytes = [0u8; 64];
    sig_bytes.copy_from_slice(&receipt.signature);
    let sig = Signature::from_bytes(&sig_bytes);
    let data = receipt.signable_data();
    pubkey.verify(&data, &sig).is_ok()
}

/// Extract an ed25519 public key from a libp2p PeerId.
///
/// libp2p PeerIds encode the public key. This extracts it if the PeerId
/// was derived from an ed25519 key.
pub fn peer_id_to_ed25519_pubkey(peer_id: &libp2p::PeerId) -> Option<[u8; 32]> {
    // PeerId can be decoded to a public key if it's an inline key (ed25519 keys are small enough)
    let multihash = peer_id.as_ref();
    // libp2p identity multihash: code 0x00 (identity), then protobuf-encoded PublicKey
    // The protobuf is: field 1 (KeyType) = 1 (Ed25519), field 2 (Data) = 32 bytes
    // We use libp2p's own parsing instead of doing it manually
    // Decode the multihash manually to extract the ed25519 public key.
    let _ = multihash; // suppress warning
    // PeerId bytes = multihash(identity, protobuf(PublicKey))
    // identity multihash: varint(0x00) + varint(len) + data
    let bytes = peer_id.to_bytes();
    // First byte should be 0x00 (identity hash code) for inlined keys
    if bytes.is_empty() || bytes[0] != 0x00 {
        return None;
    }
    // Second byte(s) = varint length of the payload
    let (payload_len, offset) = decode_varint(&bytes[1..])?;
    let payload = &bytes[1 + offset..];
    if payload.len() < payload_len {
        return None;
    }
    let payload = &payload[..payload_len];

    // payload is a protobuf PublicKey message:
    // field 1 (KeyType): varint, Ed25519 = 1
    // field 2 (Data): bytes
    // Protobuf: 0x08 0x01 0x12 0x20 <32 bytes>
    if payload.len() == 36 && payload[0] == 0x08 && payload[1] == 0x01 && payload[2] == 0x12 && payload[3] == 0x20 {
        let mut key = [0u8; 32];
        key.copy_from_slice(&payload[4..36]);
        Some(key)
    } else {
        None
    }
}

/// Decode a varint from bytes. Returns (value, bytes_consumed).
fn decode_varint(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut value: usize = 0;
    let mut shift = 0;
    for (i, &b) in bytes.iter().enumerate() {
        value |= ((b & 0x7f) as usize) << shift;
        if b & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ContentId;
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    fn make_transfer_receipt() -> TransferReceipt {
        TransferReceipt {
            content_id: ContentId([7u8; 32]),
            server_node: [1u8; 32],
            requester: [2u8; 32],
            shard_index: 3,
            bytes_served: 65536,
            timestamp: 1000,
            signature: vec![],
        }
    }

    fn make_storage_receipt() -> StorageReceipt {
        StorageReceipt {
            content_id: ContentId([8u8; 32]),
            storage_node: [1u8; 32],
            challenger: [2u8; 32],
            shard_index: 5,
            timestamp: 2000,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![],
        }
    }

    #[test]
    fn test_sign_verify_transfer_receipt() {
        let keypair = SigningKey::generate(&mut OsRng);
        let pubkey = keypair.verifying_key();
        let mut receipt = make_transfer_receipt();

        sign_transfer_receipt(&mut receipt, &keypair);
        assert_eq!(receipt.signature.len(), 64);
        assert!(verify_transfer_receipt(&receipt, &pubkey));
    }

    #[test]
    fn test_transfer_receipt_tampered() {
        let keypair = SigningKey::generate(&mut OsRng);
        let pubkey = keypair.verifying_key();
        let mut receipt = make_transfer_receipt();

        sign_transfer_receipt(&mut receipt, &keypair);

        // Tamper with bytes_served
        receipt.bytes_served = 999;
        assert!(!verify_transfer_receipt(&receipt, &pubkey));
    }

    #[test]
    fn test_transfer_receipt_wrong_key() {
        let keypair1 = SigningKey::generate(&mut OsRng);
        let keypair2 = SigningKey::generate(&mut OsRng);
        let mut receipt = make_transfer_receipt();

        sign_transfer_receipt(&mut receipt, &keypair1);
        assert!(!verify_transfer_receipt(&receipt, &keypair2.verifying_key()));
    }

    #[test]
    fn test_transfer_receipt_empty_sig_fails() {
        let keypair = SigningKey::generate(&mut OsRng);
        let receipt = make_transfer_receipt(); // signature is vec![]
        assert!(!verify_transfer_receipt(&receipt, &keypair.verifying_key()));
    }

    #[test]
    fn test_sign_verify_storage_receipt() {
        let keypair = SigningKey::generate(&mut OsRng);
        let pubkey = keypair.verifying_key();
        let mut receipt = make_storage_receipt();

        sign_storage_receipt(&mut receipt, &keypair);
        assert_eq!(receipt.signature.len(), 64);
        assert!(verify_storage_receipt(&receipt, &pubkey));
    }

    #[test]
    fn test_storage_receipt_tampered() {
        let keypair = SigningKey::generate(&mut OsRng);
        let pubkey = keypair.verifying_key();
        let mut receipt = make_storage_receipt();

        sign_storage_receipt(&mut receipt, &keypair);

        // Tamper with shard_index
        receipt.shard_index = 99;
        assert!(!verify_storage_receipt(&receipt, &pubkey));
    }

    #[test]
    fn test_storage_receipt_wrong_key() {
        let keypair1 = SigningKey::generate(&mut OsRng);
        let keypair2 = SigningKey::generate(&mut OsRng);
        let mut receipt = make_storage_receipt();

        sign_storage_receipt(&mut receipt, &keypair1);
        assert!(!verify_storage_receipt(&receipt, &keypair2.verifying_key()));
    }

    #[test]
    fn test_peer_id_to_ed25519_pubkey() {
        // Create a libp2p ed25519 keypair and derive a PeerId
        let libp2p_keypair = libp2p::identity::Keypair::generate_ed25519();
        let peer_id = libp2p_keypair.public().to_peer_id();

        let extracted = peer_id_to_ed25519_pubkey(&peer_id);
        assert!(extracted.is_some(), "Should extract ed25519 pubkey from PeerId");

        // The extracted key should match the original
        let expected = match libp2p_keypair.public().try_into_ed25519() {
            Ok(ed_pub) => ed_pub.to_bytes(),
            Err(_) => panic!("Should be ed25519"),
        };
        assert_eq!(extracted.unwrap(), expected);
    }
}
