//! PDP (Proof of Data Possession) protocol
//!
//! Implements `/datacraft/pdp/1.0.0` — challenge/response protocol for proving
//! storage nodes actually hold shard data. Includes peer rotation challenger model.

use std::collections::HashMap;
use std::time::Instant;

use datacraft_core::{ContentId, StorageReceipt};
use datacraft_store::FsStore;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/// Protocol ID for PDP streams.
pub const PDP_PROTOCOL: &str = "/datacraft/pdp/1.0.0";

/// PDP challenge sent by the challenger to a storage node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PdpChallenge {
    pub content_id: ContentId,
    pub chunk_index: u32,
    pub shard_index: u32,
    pub nonce: [u8; 32],
}

/// PDP response from a storage node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PdpResponse {
    pub proof_hash: [u8; 32],
}

/// Compute the expected proof hash: SHA-256(shard_data || nonce).
pub fn compute_proof_hash(shard_data: &[u8], nonce: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(shard_data);
    hasher.update(nonce);
    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    hash
}

// ---------------------------------------------------------------------------
// Wire encoding (length-prefixed JSON — simple and debuggable)
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
/// Returns (parsed message, bytes consumed).
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

/// Tracks first-seen time for peers providing each CID — used to determine
/// challenger rotation order (longest online = next challenger).
#[derive(Debug, Default)]
pub struct OnlineTimeTracker {
    /// (ContentId, PeerId) → first-seen Instant
    first_seen: HashMap<(ContentId, PeerId), Instant>,
}

impl OnlineTimeTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that we've seen `peer` providing `cid`. Only the first call matters.
    pub fn observe(&mut self, cid: ContentId, peer: PeerId) {
        self.first_seen.entry((cid, peer)).or_insert_with(Instant::now);
    }

    /// Record with a specific instant (for testing).
    pub fn observe_at(&mut self, cid: ContentId, peer: PeerId, when: Instant) {
        self.first_seen.entry((cid, peer)).or_insert(when);
    }

    /// Get first-seen time for a peer providing a CID.
    pub fn first_seen(&self, cid: &ContentId, peer: &PeerId) -> Option<Instant> {
        self.first_seen.get(&(*cid, *peer)).copied()
    }

    /// Sort providers by online time (longest first = earliest first_seen).
    /// Peers not yet tracked are placed at the end.
    pub fn sort_by_online_time(&self, cid: &ContentId, providers: &[PeerId]) -> Vec<PeerId> {
        let mut sorted: Vec<PeerId> = providers.to_vec();
        sorted.sort_by(|a, b| {
            let ta = self.first_seen.get(&(*cid, *a));
            let tb = self.first_seen.get(&(*cid, *b));
            match (ta, tb) {
                (Some(a_time), Some(b_time)) => a_time.cmp(b_time), // earlier = first
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
    /// CID → index into the sorted provider list (who challenged last).
    rotation_index: HashMap<ContentId, usize>,
}

impl ChallengerRotation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Determine the current challenger for a CID given a sorted provider list.
    /// Returns `None` if provider list is empty.
    pub fn current_challenger(&self, cid: &ContentId, sorted_providers: &[PeerId]) -> Option<PeerId> {
        if sorted_providers.is_empty() {
            return None;
        }
        let idx = self.rotation_index.get(cid).copied().unwrap_or(0);
        let idx = idx % sorted_providers.len();
        Some(sorted_providers[idx])
    }

    /// Advance rotation after a challenge round completes.
    pub fn advance(&mut self, cid: &ContentId, sorted_providers: &[PeerId]) {
        if sorted_providers.is_empty() {
            return;
        }
        let idx = self.rotation_index.get(cid).copied().unwrap_or(0);
        self.rotation_index.insert(*cid, (idx + 1) % sorted_providers.len());
    }
}

// ---------------------------------------------------------------------------
// Receipt storage
// ---------------------------------------------------------------------------

/// Stores received and issued StorageReceipts for later settlement.
#[derive(Debug, Default)]
pub struct ReceiptStore {
    /// Receipts we received (from being challenged by others).
    pub received: Vec<StorageReceipt>,
    /// Receipts we issued (as challenger).
    pub issued: Vec<StorageReceipt>,
}

impl ReceiptStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_received(&mut self, receipt: StorageReceipt) {
        self.received.push(receipt);
    }

    pub fn add_issued(&mut self, receipt: StorageReceipt) {
        self.issued.push(receipt);
    }
}

// ---------------------------------------------------------------------------
// PDP handler (responds to challenges)
// ---------------------------------------------------------------------------

/// Handle an incoming PDP challenge using local store data.
/// Returns `Some(PdpResponse)` if we hold the shard, `None` otherwise.
pub fn handle_pdp_challenge(
    store: &FsStore,
    challenge: &PdpChallenge,
) -> Option<PdpResponse> {
    let shard_data = store
        .get_shard(&challenge.content_id, challenge.chunk_index, challenge.shard_index as u8)
        .ok()?;
    let proof_hash = compute_proof_hash(&shard_data, &challenge.nonce);
    Some(PdpResponse { proof_hash })
}

/// Verify a PDP response against expected shard data.
/// Returns true if the proof_hash matches hash(shard_data || nonce).
pub fn verify_pdp_response(
    shard_data: &[u8],
    nonce: &[u8; 32],
    response: &PdpResponse,
) -> bool {
    let expected = compute_proof_hash(shard_data, nonce);
    expected == response.proof_hash
}

/// Create a StorageReceipt for a provider that passed PDP.
pub fn create_storage_receipt(
    content_id: ContentId,
    storage_node: [u8; 32],
    challenger: [u8; 32],
    shard_index: u32,
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
        shard_index,
        timestamp,
        nonce,
        proof_hash,
        signature: vec![], // TODO: sign with challenger's keypair
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proof_hash_deterministic() {
        let data = b"shard data bytes";
        let nonce = [42u8; 32];
        let h1 = compute_proof_hash(data, &nonce);
        let h2 = compute_proof_hash(data, &nonce);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_proof_hash_different_nonce() {
        let data = b"shard data";
        let n1 = [1u8; 32];
        let n2 = [2u8; 32];
        assert_ne!(compute_proof_hash(data, &n1), compute_proof_hash(data, &n2));
    }

    #[test]
    fn test_proof_hash_different_data() {
        let nonce = [0u8; 32];
        assert_ne!(
            compute_proof_hash(b"aaa", &nonce),
            compute_proof_hash(b"bbb", &nonce),
        );
    }

    #[test]
    fn test_wire_roundtrip_challenge() {
        let challenge = PdpChallenge {
            content_id: ContentId([7u8; 32]),
            chunk_index: 3,
            shard_index: 5,
            nonce: [99u8; 32],
        };
        let encoded = encode_pdp_message(&challenge);
        let (decoded, consumed): (PdpChallenge, _) = decode_pdp_message(&encoded).unwrap();
        assert_eq!(decoded, challenge);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn test_wire_roundtrip_response() {
        let response = PdpResponse {
            proof_hash: [0xAB; 32],
        };
        let encoded = encode_pdp_message(&response);
        let (decoded, _): (PdpResponse, _) = decode_pdp_message(&encoded).unwrap();
        assert_eq!(decoded, response);
    }

    #[test]
    fn test_wire_decode_truncated() {
        assert!(decode_pdp_message::<PdpResponse>(&[0, 0]).is_err());
        assert!(decode_pdp_message::<PdpResponse>(&[0, 0, 0, 10]).is_err()); // claims 10 bytes, has 0
    }

    #[test]
    fn test_online_time_sort() {
        let cid = ContentId([1u8; 32]);
        let p1 = PeerId::random();
        let p2 = PeerId::random();
        let p3 = PeerId::random();

        let now = Instant::now();
        let mut tracker = OnlineTimeTracker::new();
        // p2 seen first, then p1, then p3
        tracker.observe_at(cid, p2, now - std::time::Duration::from_secs(100));
        tracker.observe_at(cid, p1, now - std::time::Duration::from_secs(50));
        tracker.observe_at(cid, p3, now);

        let sorted = tracker.sort_by_online_time(&cid, &[p1, p2, p3]);
        assert_eq!(sorted, vec![p2, p1, p3]); // longest online first
    }

    #[test]
    fn test_online_time_untracked_last() {
        let cid = ContentId([2u8; 32]);
        let p1 = PeerId::random();
        let p2 = PeerId::random();

        let mut tracker = OnlineTimeTracker::new();
        tracker.observe(cid, p1);
        // p2 not observed

        let sorted = tracker.sort_by_online_time(&cid, &[p2, p1]);
        assert_eq!(sorted[0], p1); // tracked peer first
    }

    #[test]
    fn test_challenger_rotation() {
        let cid = ContentId([3u8; 32]);
        let providers = vec![PeerId::random(), PeerId::random(), PeerId::random()];
        let mut rotation = ChallengerRotation::new();

        // First round: index 0
        assert_eq!(rotation.current_challenger(&cid, &providers), Some(providers[0]));
        rotation.advance(&cid, &providers);

        // Second round: index 1
        assert_eq!(rotation.current_challenger(&cid, &providers), Some(providers[1]));
        rotation.advance(&cid, &providers);

        // Third round: index 2
        assert_eq!(rotation.current_challenger(&cid, &providers), Some(providers[2]));
        rotation.advance(&cid, &providers);

        // Wraps back to 0
        assert_eq!(rotation.current_challenger(&cid, &providers), Some(providers[0]));
    }

    #[test]
    fn test_challenger_rotation_empty() {
        let cid = ContentId([4u8; 32]);
        let rotation = ChallengerRotation::new();
        assert_eq!(rotation.current_challenger(&cid, &[]), None);
    }

    #[test]
    fn test_verify_pdp_response_success() {
        let data = b"some shard data";
        let nonce = [5u8; 32];
        let proof_hash = compute_proof_hash(data, &nonce);
        let response = PdpResponse { proof_hash };
        assert!(verify_pdp_response(data, &nonce, &response));
    }

    #[test]
    fn test_verify_pdp_response_failure() {
        let data = b"some shard data";
        let nonce = [5u8; 32];
        let response = PdpResponse { proof_hash: [0u8; 32] }; // wrong hash
        assert!(!verify_pdp_response(data, &nonce, &response));
    }

    #[test]
    fn test_handle_pdp_challenge_with_store() {
        let dir = std::env::temp_dir().join(format!("pdp-test-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"pdp test content");
        let shard_data = b"actual shard bytes here";

        store.put_shard(&cid, 0, 2, shard_data).unwrap();

        let nonce = [77u8; 32];
        let challenge = PdpChallenge {
            content_id: cid,
            chunk_index: 0,
            shard_index: 2,
            nonce,
        };

        let response = handle_pdp_challenge(&store, &challenge).unwrap();
        let expected = compute_proof_hash(shard_data, &nonce);
        assert_eq!(response.proof_hash, expected);

        // Missing shard returns None
        let bad_challenge = PdpChallenge {
            content_id: cid,
            chunk_index: 0,
            shard_index: 99,
            nonce,
        };
        assert!(handle_pdp_challenge(&store, &bad_challenge).is_none());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_create_storage_receipt() {
        let cid = ContentId([10u8; 32]);
        let receipt = create_storage_receipt(
            cid,
            [1u8; 32],
            [2u8; 32],
            5,
            [3u8; 32],
            [4u8; 32],
        );
        assert_eq!(receipt.content_id, cid);
        assert_eq!(receipt.storage_node, [1u8; 32]);
        assert_eq!(receipt.challenger, [2u8; 32]);
        assert_eq!(receipt.shard_index, 5);
        assert_eq!(receipt.nonce, [3u8; 32]);
        assert_eq!(receipt.proof_hash, [4u8; 32]);
        assert_eq!(receipt.weight(), 1);
        assert!(receipt.timestamp > 0);
    }

    #[test]
    fn test_receipt_store() {
        let mut store = ReceiptStore::new();
        let receipt = create_storage_receipt(
            ContentId([0u8; 32]),
            [1u8; 32],
            [2u8; 32],
            0,
            [3u8; 32],
            [4u8; 32],
        );
        store.add_received(receipt.clone());
        store.add_issued(receipt);
        assert_eq!(store.received.len(), 1);
        assert_eq!(store.issued.len(), 1);
    }

    #[test]
    fn test_full_pdp_flow() {
        // Simulates: challenger creates challenge → provider responds → challenger verifies → receipt issued
        let dir = std::env::temp_dir().join(format!("pdp-flow-{}", std::process::id()));
        let store = FsStore::new(&dir).unwrap();
        let cid = ContentId::from_bytes(b"full flow test");
        let shard_data = b"the actual shard content for pdp";

        store.put_shard(&cid, 0, 1, shard_data).unwrap();

        // 1. Challenger creates challenge with random nonce
        let nonce = [42u8; 32]; // would be random in production
        let challenge = PdpChallenge {
            content_id: cid,
            chunk_index: 0,
            shard_index: 1,
            nonce,
        };

        // 2. Provider responds
        let response = handle_pdp_challenge(&store, &challenge).unwrap();

        // 3. Challenger verifies (challenger has the shard data to verify against)
        assert!(verify_pdp_response(shard_data, &nonce, &response));

        // 4. Challenger issues receipt
        let receipt = create_storage_receipt(
            cid,
            [1u8; 32], // storage_node pubkey
            [2u8; 32], // challenger pubkey
            1,
            nonce,
            response.proof_hash,
        );

        assert_eq!(receipt.weight(), 1);
        assert_eq!(receipt.content_id, cid);

        std::fs::remove_dir_all(&dir).ok();
    }
}
