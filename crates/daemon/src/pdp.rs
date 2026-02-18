//! PDP (Proof of Data Possession) protocol
//!
//! Implements coefficient vector cross-checking for proving storage nodes
//! actually hold piece data. Uses GF(2^8) linear algebra via coefficient vectors.

use std::collections::HashMap;
use std::time::Instant;

use craftobj_core::{ContentId, StorageReceipt};
use craftobj_store::FsStore;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// PDP challenge sent by the challenger to a storage node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PdpChallenge {
    pub content_id: ContentId,
    pub segment_index: u32,
    /// Piece ID to challenge (SHA-256 of coefficient vector).
    pub piece_id: [u8; 32],
    /// Random byte positions to sample from the piece data.
    pub byte_positions: Vec<u32>,
    /// Nonce for replay prevention.
    pub nonce: [u8; 32],
    /// Timestamp for freshness.
    pub timestamp: u64,
}

/// PDP response from a storage node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PdpResponse {
    /// Hash of (sampled_bytes || coefficient_vector || nonce).
    pub proof_hash: [u8; 32],
    /// The coefficient vector for this piece (piece identity).
    pub coefficients: Vec<u8>,
}

/// Derive deterministic byte positions from nonce + piece_id.
///
/// Both challenger and prover can independently compute the same positions,
/// ensuring positions are spread across the full piece rather than just the first N bytes.
pub fn derive_byte_positions(nonce: &[u8; 32], piece_id: &[u8; 32], piece_len: u32, count: usize) -> Vec<u32> {
    if piece_len == 0 {
        return vec![];
    }
    let mut pos_seed = Sha256::new();
    pos_seed.update(nonce);
    pos_seed.update(piece_id);
    let seed = pos_seed.finalize();

    (0..count)
        .map(|i| {
            let b0 = seed[(i * 2) % 32];
            let b1 = seed[(i * 2 + 1) % 32];
            let b2 = seed[(i * 2 + 2) % 32];
            let b3 = seed[(i * 2 + 3) % 32];
            let offset = u32::from_be_bytes([b0, b1, b2, b3]);
            offset % piece_len
        })
        .collect()
}

/// Compute the expected proof hash: SHA-256(sampled_bytes || coefficients || nonce).
pub fn compute_proof_hash(piece_data: &[u8], byte_positions: &[u32], coefficients: &[u8], nonce: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for &pos in byte_positions {
        if (pos as usize) < piece_data.len() {
            hasher.update(&[piece_data[pos as usize]]);
        }
    }
    hasher.update(coefficients);
    hasher.update(nonce);
    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    hash
}

// ---------------------------------------------------------------------------
// Cross-verification via GF(2^8) linear algebra
// ---------------------------------------------------------------------------

/// Result of cross-verification attempt.
#[derive(Debug, Clone)]
pub enum CrossVerifyResult {
    /// Full verification passed — prover's data matches challenger's computation.
    Verified,
    /// Full verification failed — data mismatch (fraud or corruption).
    Failed,
    /// Challenger's pieces don't span the prover's coefficient vector.
    /// Falls back to basic "piece received" verification for v1.
    InsufficientBasis,
}

/// Attempt to cross-verify a prover's piece using the challenger's own local pieces.
///
/// The challenger tries to express the prover's coefficient vector as a linear
/// combination of its own coefficient vectors using GF(2^8) Gaussian elimination.
/// If possible, it computes the expected data bytes at the given positions and
/// compares them with the prover's actual data.
///
/// # Arguments
/// * `own_pieces` - Challenger's local pieces for the same segment (data + coefficients)
/// * `prover_coefficients` - The prover's piece coefficient vector
/// * `prover_data` - The prover's piece data
/// * `byte_positions` - Positions to verify within the piece data
///
/// # Returns
/// `CrossVerifyResult` indicating whether verification passed, failed, or was inconclusive.
pub fn cross_verify_piece(
    own_pieces: &[(Vec<u8>, Vec<u8>)], // (data, coefficients) pairs
    prover_coefficients: &[u8],
    prover_data: &[u8],
    byte_positions: &[u32],
) -> CrossVerifyResult {
    use craftec_erasure::gf256::GF256;

    if own_pieces.is_empty() || prover_coefficients.is_empty() {
        return CrossVerifyResult::InsufficientBasis;
    }

    let k = prover_coefficients.len();

    // Build coefficient matrix from our own pieces
    let mut own_coeffs: Vec<Vec<u8>> = Vec::new();
    let mut own_data: Vec<&[u8]> = Vec::new();
    for (data, coeff) in own_pieces {
        if coeff.len() == k {
            own_coeffs.push(coeff.clone());
            own_data.push(data);
        }
    }

    if own_coeffs.is_empty() {
        return CrossVerifyResult::InsufficientBasis;
    }

    // We need to find scalars α_i such that:
    //   Σ α_i * own_coeffs[i] = prover_coefficients
    // This is equivalent to solving a linear system.
    //
    // Augmented matrix: [own_coeffs^T | prover_coefficients^T]
    // Each row corresponds to one coefficient dimension (k rows),
    // each column is one of our pieces + the prover target.

    let n = own_coeffs.len(); // number of our pieces
    // Build augmented matrix: k rows × (n+1) columns
    let mut matrix: Vec<Vec<u8>> = Vec::with_capacity(k);
    for row in 0..k {
        let mut r = Vec::with_capacity(n + 1);
        for col in 0..n {
            r.push(own_coeffs[col][row]);
        }
        r.push(prover_coefficients[row]);
        matrix.push(r);
    }

    // Gaussian elimination to solve for alphas
    let cols = n + 1;
    let mut pivot_col = 0;
    let mut pivot_rows = Vec::new();
    for row in 0..k {
        if pivot_col >= n {
            break;
        }
        // Find pivot
        let pivot = (row..k).find(|&r| matrix[r][pivot_col] != 0);
        let pivot_idx = match pivot {
            Some(p) => p,
            None => {
                // Try next column? No — we go column by column up to n
                // Skip this column if no pivot
                pivot_col += 1;
                continue;
            }
        };
        matrix.swap(row, pivot_idx);
        let inv = match GF256::inv(matrix[row][pivot_col]) {
            Some(v) => v,
            None => { pivot_col += 1; continue; }
        };
        for c in 0..cols {
            matrix[row][c] = GF256::mul(matrix[row][c], inv);
        }
        for other in 0..k {
            if other == row { continue; }
            let factor = matrix[other][pivot_col];
            if factor == 0 { continue; }
            for c in 0..cols {
                let val = GF256::mul(factor, matrix[row][c]);
                matrix[other][c] = GF256::add(matrix[other][c], val);
            }
        }
        pivot_rows.push((row, pivot_col));
        pivot_col += 1;
    }

    // Check consistency: any row with all zeros in first n columns but non-zero in last column
    // means the system is inconsistent (prover's vector is NOT in our span).
    for row in 0..k {
        let all_zero = (0..n).all(|c| matrix[row][c] == 0);
        if all_zero && matrix[row][n] != 0 {
            return CrossVerifyResult::InsufficientBasis;
        }
    }

    // Extract the solution alphas from the augmented column
    let mut alphas = vec![0u8; n];
    for &(row, col) in &pivot_rows {
        alphas[col] = matrix[row][n];
    }

    // Now compute expected data at the byte positions:
    // expected_byte[pos] = Σ α_i * own_data[i][pos]
    for &pos in byte_positions {
        let pos = pos as usize;
        if pos >= prover_data.len() {
            continue;
        }
        let mut expected = 0u8;
        for (i, &alpha) in alphas.iter().enumerate() {
            if alpha == 0 { continue; }
            let own_byte = if pos < own_data[i].len() { own_data[i][pos] } else { 0 };
            expected = GF256::add(expected, GF256::mul(alpha, own_byte));
        }
        if expected != prover_data[pos] {
            return CrossVerifyResult::Failed;
        }
    }

    CrossVerifyResult::Verified
}

// ---------------------------------------------------------------------------
// Wire encoding (length-prefixed JSON)
// ---------------------------------------------------------------------------

/// Encode a PDP message as length-prefixed JSON.
pub fn encode_pdp_message<T: Serialize>(msg: &T) -> Vec<u8> {
    let json = serde_json::to_vec(msg).expect("PDP message serialization should not fail");
    let len = (json.len() as u32).to_be_bytes();
    let mut buf = Vec::with_capacity(4 + json.len());
    buf.extend_from_slice(&len);
    buf.extend_from_slice(&json);
    buf
}

/// Decode a PDP message from length-prefixed JSON bytes.
pub fn decode_pdp_message<T: for<'de> Deserialize<'de>>(buf: &[u8]) -> Result<(T, usize), String> {
    if buf.len() < 4 {
        return Err("buffer too short for length prefix".into());
    }
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Err(format!("expected {} bytes, got {}", 4 + len, buf.len()));
    }
    let msg: T = serde_json::from_slice(&buf[4..4 + len])
        .map_err(|e| format!("JSON decode error: {}", e))?;
    Ok((msg, 4 + len))
}

// ---------------------------------------------------------------------------
// Peer rotation
// ---------------------------------------------------------------------------

/// Tracks first-seen time for peers providing each CID.
#[derive(Debug, Default)]
pub struct OnlineTimeTracker {
    first_seen: HashMap<(ContentId, PeerId), Instant>,
}

impl OnlineTimeTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, cid: ContentId, peer: PeerId) {
        self.first_seen.entry((cid, peer)).or_insert_with(Instant::now);
    }

    pub fn observe_at(&mut self, cid: ContentId, peer: PeerId, when: Instant) {
        self.first_seen.entry((cid, peer)).or_insert(when);
    }

    pub fn first_seen(&self, cid: &ContentId, peer: &PeerId) -> Option<Instant> {
        self.first_seen.get(&(*cid, *peer)).copied()
    }

    pub fn sort_by_online_time(&self, cid: &ContentId, providers: &[PeerId]) -> Vec<PeerId> {
        let mut sorted: Vec<PeerId> = providers.to_vec();
        sorted.sort_by(|a, b| {
            let ta = self.first_seen.get(&(*cid, *a));
            let tb = self.first_seen.get(&(*cid, *b));
            match (ta, tb) {
                (Some(a_time), Some(b_time)) => a_time.cmp(b_time),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
        sorted
    }
}

/// Manages challenger rotation for each CID.
#[derive(Debug, Default)]
pub struct ChallengerRotation {
    rotation_index: HashMap<ContentId, usize>,
}

impl ChallengerRotation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn current_challenger(&self, cid: &ContentId, sorted_providers: &[PeerId]) -> Option<PeerId> {
        if sorted_providers.is_empty() {
            return None;
        }
        let idx = self.rotation_index.get(cid).copied().unwrap_or(0);
        let idx = idx % sorted_providers.len();
        Some(sorted_providers[idx])
    }

    pub fn advance(&mut self, cid: &ContentId, sorted_providers: &[PeerId]) {
        if sorted_providers.is_empty() {
            return;
        }
        let idx = self.rotation_index.get(cid).copied().unwrap_or(0);
        self.rotation_index.insert(*cid, (idx + 1) % sorted_providers.len());
    }
}

// ---------------------------------------------------------------------------
// PDP handler
// ---------------------------------------------------------------------------

/// Handle an incoming PDP challenge using local store data.
pub fn handle_pdp_challenge(
    store: &FsStore,
    challenge: &PdpChallenge,
) -> Option<PdpResponse> {
    let (data, coefficients) = store
        .get_piece(&challenge.content_id, challenge.segment_index, &challenge.piece_id)
        .ok()?;
    let proof_hash = compute_proof_hash(&data, &challenge.byte_positions, &coefficients, &challenge.nonce);
    Some(PdpResponse { proof_hash, coefficients })
}

/// Verify a PDP response against expected piece data.
pub fn verify_pdp_response(
    piece_data: &[u8],
    byte_positions: &[u32],
    expected_coefficients: &[u8],
    nonce: &[u8; 32],
    response: &PdpResponse,
) -> bool {
    if response.coefficients != expected_coefficients {
        return false;
    }
    let expected = compute_proof_hash(piece_data, byte_positions, &response.coefficients, nonce);
    expected == response.proof_hash
}

/// Create a StorageReceipt for a provider that passed PDP (unsigned).
pub fn create_storage_receipt(
    content_id: ContentId,
    storage_node: [u8; 32],
    challenger: [u8; 32],
    segment_index: u32,
    piece_id: [u8; 32],
    nonce: [u8; 32],
    proof_hash: [u8; 32],
) -> StorageReceipt {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    StorageReceipt {
        content_id,
        storage_node,
        challenger,
        segment_index,
        piece_id,
        timestamp,
        nonce,
        proof_hash,
        signature: vec![],
    }
}

/// Create and sign a StorageReceipt.
pub fn create_signed_storage_receipt(
    content_id: ContentId,
    storage_node: [u8; 32],
    challenger: [u8; 32],
    segment_index: u32,
    piece_id: [u8; 32],
    nonce: [u8; 32],
    proof_hash: [u8; 32],
    signing_key: &ed25519_dalek::SigningKey,
) -> StorageReceipt {
    let mut receipt = create_storage_receipt(
        content_id, storage_node, challenger, segment_index, piece_id, nonce, proof_hash,
    );
    craftobj_core::signing::sign_storage_receipt(&mut receipt, signing_key);
    receipt
}

/// Extract a 32-byte public key from a PeerId (best-effort, zero-padded).
pub fn peer_id_to_local_pubkey(peer_id: &PeerId) -> [u8; 32] {
    use craftobj_core::signing::peer_id_to_ed25519_pubkey;
    peer_id_to_ed25519_pubkey(peer_id).unwrap_or_else(|| {
        let bytes = peer_id.to_bytes();
        let mut key = [0u8; 32];
        let len = bytes.len().min(32);
        key[..len].copy_from_slice(&bytes[..len]);
        key
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proof_hash_deterministic() {
        let data = b"piece data bytes";
        let positions = vec![0, 5, 10];
        let coefficients = vec![1, 0, 0, 0];
        let nonce = [42u8; 32];
        let h1 = compute_proof_hash(data, &positions, &coefficients, &nonce);
        let h2 = compute_proof_hash(data, &positions, &coefficients, &nonce);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_proof_hash_different_nonce() {
        let data = b"piece data";
        let positions = vec![0, 1];
        let coefficients = vec![1];
        let n1 = [1u8; 32];
        let n2 = [2u8; 32];
        assert_ne!(
            compute_proof_hash(data, &positions, &coefficients, &n1),
            compute_proof_hash(data, &positions, &coefficients, &n2),
        );
    }

    #[test]
    fn test_wire_roundtrip_challenge() {
        let challenge = PdpChallenge {
            content_id: ContentId([7u8; 32]),
            segment_index: 3,
            piece_id: [5u8; 32],
            byte_positions: vec![0, 100, 200],
            nonce: [99u8; 32],
            timestamp: 1234567890,
        };
        let encoded = encode_pdp_message(&challenge);
        let (decoded, consumed): (PdpChallenge, _) = decode_pdp_message(&encoded).unwrap();
        assert_eq!(decoded, challenge);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_online_time_sort() {
        let cid = ContentId([1u8; 32]);
        let p1 = PeerId::random();
        let p2 = PeerId::random();
        let p3 = PeerId::random();

        let now = Instant::now();
        let mut tracker = OnlineTimeTracker::new();
        tracker.observe_at(cid, p2, now - std::time::Duration::from_secs(100));
        tracker.observe_at(cid, p1, now - std::time::Duration::from_secs(50));
        tracker.observe_at(cid, p3, now);

        let sorted = tracker.sort_by_online_time(&cid, &[p1, p2, p3]);
        assert_eq!(sorted, vec![p2, p1, p3]);
    }

    #[test]
    fn test_challenger_rotation() {
        let cid = ContentId([3u8; 32]);
        let providers = vec![PeerId::random(), PeerId::random(), PeerId::random()];
        let mut rotation = ChallengerRotation::new();

        assert_eq!(rotation.current_challenger(&cid, &providers), Some(providers[0]));
        rotation.advance(&cid, &providers);
        assert_eq!(rotation.current_challenger(&cid, &providers), Some(providers[1]));
        rotation.advance(&cid, &providers);
        assert_eq!(rotation.current_challenger(&cid, &providers), Some(providers[2]));
        rotation.advance(&cid, &providers);
        assert_eq!(rotation.current_challenger(&cid, &providers), Some(providers[0]));
    }

    #[test]
    fn test_handle_pdp_challenge_with_store() {
        let dir = std::env::temp_dir().join(format!("pdp-test-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"pdp test content");
        let piece_data = b"actual piece bytes here";
        let coefficients = vec![1u8, 0, 0];
        let piece_id = craftobj_store::piece_id_from_coefficients(&coefficients);

        store.store_piece(&cid, 0, &piece_id, piece_data, &coefficients).unwrap();

        let nonce = [77u8; 32];
        let challenge = PdpChallenge {
            content_id: cid,
            segment_index: 0,
            piece_id,
            byte_positions: vec![0, 5, 10],
            nonce,
            timestamp: 1000,
        };

        let response = handle_pdp_challenge(&store, &challenge).unwrap();
        let expected = compute_proof_hash(piece_data, &challenge.byte_positions, &coefficients, &nonce);
        assert_eq!(response.proof_hash, expected);
        assert_eq!(response.coefficients, coefficients);

        // Missing piece returns None
        let bad_challenge = PdpChallenge {
            piece_id: [99u8; 32],
            ..challenge
        };
        assert!(handle_pdp_challenge(&store, &bad_challenge).is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_create_storage_receipt() {
        let cid = ContentId([10u8; 32]);
        let piece_id = [5u8; 32];
        let receipt = create_storage_receipt(
            cid, [1u8; 32], [2u8; 32], 0, piece_id, [3u8; 32], [4u8; 32],
        );
        assert_eq!(receipt.content_id, cid);
        assert_eq!(receipt.piece_id, piece_id);
        assert_eq!(receipt.segment_index, 0);
        assert_eq!(receipt.weight(), 1);
        assert!(receipt.timestamp > 0);
    }

    #[test]
    fn test_create_signed_storage_receipt() {
        use craftobj_core::signing::verify_storage_receipt;
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let key = SigningKey::generate(&mut OsRng);
        let pubkey = key.verifying_key();
        let piece_id = [10u8; 32];

        let receipt = create_signed_storage_receipt(
            ContentId([20u8; 32]),
            [1u8; 32],
            pubkey.to_bytes(),
            0,
            piece_id,
            [3u8; 32],
            [4u8; 32],
            &key,
        );

        assert_eq!(receipt.signature.len(), 64);
        assert!(verify_storage_receipt(&receipt, &pubkey));
        assert_eq!(receipt.piece_id, piece_id);
    }

    #[test]
    fn test_derive_byte_positions_deterministic() {
        let nonce = [1u8; 32];
        let piece_id = [2u8; 32];
        let p1 = derive_byte_positions(&nonce, &piece_id, 1000, 16);
        let p2 = derive_byte_positions(&nonce, &piece_id, 1000, 16);
        assert_eq!(p1, p2);
        assert_eq!(p1.len(), 16);
        for &pos in &p1 {
            assert!(pos < 1000);
        }
    }

    #[test]
    fn test_derive_byte_positions_different_nonces() {
        let piece_id = [2u8; 32];
        let p1 = derive_byte_positions(&[1u8; 32], &piece_id, 10000, 16);
        let p2 = derive_byte_positions(&[2u8; 32], &piece_id, 10000, 16);
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_derive_byte_positions_different_piece_ids() {
        let nonce = [1u8; 32];
        let p1 = derive_byte_positions(&nonce, &[1u8; 32], 10000, 16);
        let p2 = derive_byte_positions(&nonce, &[2u8; 32], 10000, 16);
        assert_ne!(p1, p2);
    }

    #[test]
    fn test_derive_byte_positions_spread() {
        // With a large piece, positions should not all be in the first 16 bytes
        let nonce = [42u8; 32];
        let piece_id = [7u8; 32];
        let positions = derive_byte_positions(&nonce, &piece_id, 1_000_000, 16);
        let max_pos = *positions.iter().max().unwrap();
        assert!(max_pos > 16, "positions should spread across the piece, max was {}", max_pos);
    }

    #[test]
    fn test_derive_byte_positions_zero_len() {
        assert!(derive_byte_positions(&[0u8; 32], &[0u8; 32], 0, 16).is_empty());
    }

    #[test]
    fn test_peer_id_to_local_pubkey_ed25519() {
        let kp = libp2p::identity::Keypair::generate_ed25519();
        let peer_id = kp.public().to_peer_id();
        let extracted = peer_id_to_local_pubkey(&peer_id);
        let expected = kp.public().try_into_ed25519().unwrap().to_bytes();
        assert_eq!(extracted, expected);
    }
}
