//! CraftObj Core
//!
//! Content types and protocol primitives for CraftObj:
//! content-addressed distributed storage with RLNC erasure coding.

// NOTE: access, economics, payment_channel, pre modules
// removed from kernel — these are COM/SQL layer concerns.
pub mod signing;


use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Content identifier — SHA-256 hash of the content (or ciphertext if encrypted).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentId(pub [u8; 32]);

impl ContentId {
    /// Compute the ContentId from raw bytes.
    pub fn from_bytes(data: &[u8]) -> Self {
        let hash = Sha256::digest(data);
        let mut id = [0u8; 32];
        id.copy_from_slice(&hash);
        Self(id)
    }

    /// Hex-encoded content ID.
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse from hex string.
    pub fn from_hex(s: &str) -> std::result::Result<Self, CraftObjError> {
        let bytes = hex::decode(s).map_err(|e| CraftObjError::InvalidContentId(e.to_string()))?;
        if bytes.len() != 32 {
            return Err(CraftObjError::InvalidContentId("expected 32 bytes".into()));
        }
        let mut id = [0u8; 32];
        id.copy_from_slice(&bytes);
        Ok(Self(id))
    }
}

impl std::fmt::Display for ContentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Fixed segment size for RLNC erasure coding (10 MB).
pub const SEGMENT_SIZE: usize = 10_485_760;

/// Fixed piece size for RLNC erasure coding (256 KB).
pub const PIECE_SIZE: usize = 262_144;

// ---------------------------------------------------------------------------
// Redundancy formula: derived from binomial analysis at 30% churn
// ---------------------------------------------------------------------------

/// Base redundancy multiplier for a given k.
/// `redundancy(k) = 2.0 + 16.0 / k` — provides ~8-9 nines of durability at 30% churn.
/// k=1 → 18×, k=40 → 2.4×.
pub fn redundancy(k: u32) -> f64 {
    2.0 + 16.0 / k as f64
}

/// Base target piece count for a segment: `ceil(k × redundancy(k))`.
///
/// This is the kernel-level target. Economic tiers (multipliers) are
/// applied by the COM layer on top — not part of the kernel.
pub fn target_piece_count(k: u32) -> u32 {
    (k as f64 * redundancy(k)).ceil() as u32
}

// ---------------------------------------------------------------------------
// Self-describing piece header
// ---------------------------------------------------------------------------

/// Self-describing piece header — every piece carries its own metadata.
///
/// Two orthogonal axes:
/// - `k` → redundancy target via `redundancy(k) = 2.0 + 16/k`
/// - `vtags_cid` → strategy discriminator:
///   - `Some(cid)` → erasure extension + vtag verification
///   - `None` → replication + hash verification
///
/// With `total_size` included, pieces are fully self-describing —
/// no manifest needed for reconstruction or health scanning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PieceHeader {
    pub content_id: ContentId,
    pub total_size: u64,
    pub segment_idx: u32,
    pub segment_count: u32,
    pub k: u32,
    /// `Some` = erasure-coded content (verify via vtags), `None` = replicated raw object (verify via hash).
    pub vtags_cid: Option<[u8; 32]>,
    pub coefficients: Vec<u8>,
}

impl PieceHeader {
    /// Compute piece_id: SHA-256 of the coefficient vector.
    pub fn piece_id(&self) -> [u8; 32] {
        let hash = Sha256::digest(&self.coefficients);
        let mut id = [0u8; 32];
        id.copy_from_slice(&hash);
        id
    }

    /// Whether this piece uses erasure extension (vtag-verified) or replication (hash-verified).
    pub fn is_erasure(&self) -> bool {
        self.vtags_cid.is_some()
    }

    /// Size of a full segment (all segments except possibly the last).
    pub fn full_segment_size(&self) -> usize {
        SEGMENT_SIZE
    }

    /// Size of the last segment (may be smaller than SEGMENT_SIZE).
    pub fn last_segment_size(&self) -> usize {
        let full_segs = if self.segment_count > 1 { self.segment_count - 1 } else { 0 };
        self.total_size as usize - full_segs as usize * SEGMENT_SIZE
    }
}

// ---------------------------------------------------------------------------
// Content record — optional cached summary (same data lives in piece headers)
// ---------------------------------------------------------------------------

/// Cached content summary. Not required for reconstruction or health scanning —
/// piece headers are authoritative. This is kept for convenience (e.g. listing
/// content without scanning all pieces).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentRecord {
    pub content_id: ContentId,
    pub total_size: u64,
    /// CID of the vtags object for homomorphic piece verification.
    /// `None` for non-content objects (vtag blobs, manifests, SQL pages).
    #[serde(default)]
    pub vtags_cid: Option<[u8; 32]>,
}

/// Backwards-compat alias.
pub type ContentManifest = ContentRecord;

impl ContentRecord {
    pub fn segment_count(&self) -> usize {
        if self.total_size == 0 { return 0; }
        (self.total_size as usize).div_ceil(SEGMENT_SIZE)
    }
    pub fn k(&self) -> usize { SEGMENT_SIZE.div_ceil(PIECE_SIZE) }
    pub fn k_for_segment(&self, segment_index: usize) -> usize {
        let seg_count = self.segment_count();
        if seg_count == 0 { return 0; }
        if segment_index + 1 < seg_count {
            SEGMENT_SIZE.div_ceil(PIECE_SIZE)
        } else {
            let remaining = self.total_size as usize - segment_index * SEGMENT_SIZE;
            remaining.div_ceil(PIECE_SIZE).max(1)
        }
    }
    pub fn last_segment_k(&self) -> usize {
        let remainder = (self.total_size as usize) % SEGMENT_SIZE;
        if remainder == 0 { self.k() } else { remainder.div_ceil(PIECE_SIZE) }
    }
    pub fn segment_size(&self) -> usize { SEGMENT_SIZE }
    pub fn piece_size(&self) -> usize { PIECE_SIZE }
}

/// Extract ed25519 public key bytes from a `did:craftec:<hex>` string.
pub fn extract_pubkey_from_did(did: &str) -> Option<[u8; 32]> {
    let hex_str = did.strip_prefix("did:craftec:")?;
    let bytes = hex::decode(hex_str).ok()?;
    if bytes.len() != 32 {
        return None;
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Some(key)
}

/// Build a DID string from an ed25519 public key.
pub fn did_from_pubkey(pubkey: &ed25519_dalek::VerifyingKey) -> String {
    format!("did:craftec:{}", hex::encode(pubkey.to_bytes()))
}

// NOTE: RemovalNotice removed from kernel — content removal is an economic
// concern (COM layer). Storage naturally degrades via HealthScan when unfunded.


/// Options for publishing content.
#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    /// Encrypt content before publishing. The caller must store the key.
    pub encrypted: bool,
}

/// StorageReceipt — issued by PDP challenger on successful proof-of-possession.
///
/// Weight: 1 per proof (proving you still hold the piece).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageReceipt {
    /// Content whose piece was proven.
    pub content_id: ContentId,
    /// Node that proved possession.
    pub storage_node: [u8; 32],
    /// Node that issued the challenge.
    pub challenger: [u8; 32],
    /// Which segment the piece belongs to.
    pub segment_index: u32,
    /// Piece identity: SHA-256 of the coefficient vector.
    pub piece_id: [u8; 32],
    /// When the proof occurred.
    pub timestamp: u64,
    /// Challenge nonce.
    pub nonce: [u8; 32],
    /// hash(piece_data || nonce) response.
    pub proof_hash: [u8; 32],
    /// Challenger's signature over receipt.
    pub signature: Vec<u8>,
}

impl craftec_core::ContributionReceipt for StorageReceipt {
    fn weight(&self) -> u64 {
        1
    }

    fn signable_data(&self) -> Vec<u8> {
        StorageReceipt::signable_data(self)
    }

    fn operator(&self) -> [u8; 32] {
        self.storage_node
    }

    fn signer(&self) -> [u8; 32] {
        self.challenger
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }

    fn signature(&self) -> &[u8] {
        &self.signature
    }
}

impl StorageReceipt {
    /// Weight for settlement: 1 per successful PDP proof.
    pub fn weight(&self) -> u64 {
        1
    }

    /// Data to be signed by the challenger.
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.content_id.0);
        data.extend_from_slice(&self.storage_node);
        data.extend_from_slice(&self.challenger);
        data.extend_from_slice(&self.segment_index.to_le_bytes());
        data.extend_from_slice(&self.piece_id);
        data.extend_from_slice(&self.timestamp.to_le_bytes());
        data.extend_from_slice(&self.nonce);
        data.extend_from_slice(&self.proof_hash);
        data
    }
}

/// Wire protocol magic bytes: "COBJ" (CraftObj)
pub const WIRE_MAGIC: [u8; 4] = [0x43, 0x4F, 0x42, 0x4A];

/// Transfer stream protocol ID.
pub const TRANSFER_PROTOCOL: &str = "/craftobj/transfer/3.0.0";

/// Manifest exchange protocol ID.
pub const RECORD_PROTOCOL: &str = "/craftobj/manifest/2.0.0";

/// PDP (Proof of Data Possession) protocol ID.
pub const PDP_PROTOCOL: &str = "/craftobj/pdp/2.0.0";

/// DHT key prefix for content providers.
pub const PROVIDERS_DHT_PREFIX: &str = "/craftobj/providers/";

/// DHT key prefix for content manifests.
pub const RECORD_DHT_PREFIX: &str = "/craftobj/manifest/";

/// DHT key prefix for peer pubkey → PeerId records.
pub const PEERS_DHT_PREFIX: &str = "/craftobj/peers/";

// NOTE: ACCESS_DHT_PREFIX, REKEY_DHT_PREFIX, REMOVAL_DHT_PREFIX removed
// — access control, PRE, and content removal are COM layer concerns.


/// Event-sourced piece tracking event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PieceEvent {
    Stored(PieceStored),
    Dropped(PieceDropped),
}

/// A node stored a coded piece.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PieceStored {
    /// PeerId bytes of the node that stored the piece.
    pub node: Vec<u8>,
    /// Content ID.
    pub cid: ContentId,
    /// Segment index.
    pub segment: u32,
    /// Piece identity: SHA-256 of coefficient vector.
    pub piece_id: [u8; 32],
    /// Coefficient vector (k bytes over GF(2^8)).
    pub coefficients: Vec<u8>,
    /// Per-node monotonic sequence number.
    pub seq: u64,
    /// Unix timestamp (seconds).
    pub timestamp: u64,
    /// Ed25519 signature over the event data.
    pub signature: Vec<u8>,
}

/// A node dropped a coded piece.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PieceDropped {
    /// PeerId bytes of the node that dropped the piece.
    pub node: Vec<u8>,
    /// Content ID.
    pub cid: ContentId,
    /// Segment index.
    pub segment: u32,
    /// Piece identity: SHA-256 of coefficient vector.
    pub piece_id: [u8; 32],
    /// Per-node monotonic sequence number.
    pub seq: u64,
    /// Unix timestamp (seconds).
    pub timestamp: u64,
    /// Ed25519 signature over the event data.
    pub signature: Vec<u8>,
}

impl PieceEvent {
    /// Get the node bytes from the event.
    pub fn node(&self) -> &[u8] {
        match self {
            PieceEvent::Stored(s) => &s.node,
            PieceEvent::Dropped(d) => &d.node,
        }
    }

    /// Get the sequence number from the event.
    pub fn seq(&self) -> u64 {
        match self {
            PieceEvent::Stored(s) => s.seq,
            PieceEvent::Dropped(d) => d.seq,
        }
    }

    /// Data to sign/verify (everything except the signature field).
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        match self {
            PieceEvent::Stored(s) => {
                data.push(0u8); // tag
                data.extend_from_slice(&s.node);
                data.extend_from_slice(&s.cid.0);
                data.extend_from_slice(&s.segment.to_le_bytes());
                data.extend_from_slice(&s.piece_id);
                data.extend_from_slice(&s.coefficients);
                data.extend_from_slice(&s.seq.to_le_bytes());
                data.extend_from_slice(&s.timestamp.to_le_bytes());
            }
            PieceEvent::Dropped(d) => {
                data.push(1u8); // tag
                data.extend_from_slice(&d.node);
                data.extend_from_slice(&d.cid.0);
                data.extend_from_slice(&d.segment.to_le_bytes());
                data.extend_from_slice(&d.piece_id);
                data.extend_from_slice(&d.seq.to_le_bytes());
                data.extend_from_slice(&d.timestamp.to_le_bytes());
            }
        }
        data
    }

    /// Get the signature from the event.
    pub fn signature(&self) -> &[u8] {
        match self {
            PieceEvent::Stored(s) => &s.signature,
            PieceEvent::Dropped(d) => &d.signature,
        }
    }

    /// Verify the ed25519 signature on this event.
    /// The signer's public key is derived from the node's PeerId bytes.
    /// Returns true if signature is valid.
    pub fn verify_signature(&self, pubkey: &ed25519_dalek::VerifyingKey) -> bool {
        if self.signature().len() != 64 {
            return false;
        }
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(self.signature());
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        let data = self.signable_data();
        pubkey.verify_strict(&data, &sig).is_ok()
    }
}

impl PieceStored {
    /// Sign this event with the given signing key. Sets the signature field.
    pub fn sign(&mut self, key: &ed25519_dalek::SigningKey) {
        use ed25519_dalek::Signer;
        let event = PieceEvent::Stored(self.clone());
        let data = event.signable_data();
        let sig = key.sign(&data);
        self.signature = sig.to_bytes().to_vec();
    }
}

impl PieceDropped {
    /// Sign this event with the given signing key. Sets the signature field.
    pub fn sign(&mut self, key: &ed25519_dalek::SigningKey) {
        use ed25519_dalek::Signer;
        let event = PieceEvent::Dropped(self.clone());
        let data = event.signable_data();
        let sig = key.sign(&data);
        self.signature = sig.to_bytes().to_vec();
    }
}



/// Capabilities a CraftObj node can declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CraftObjCapability {
    /// Publishes and fetches content.
    Client,
    /// Stores and serves content (participates in PDP).
    Storage,
    /// Settles distributions, aggregates receipts.
    Aggregator,
}

impl std::fmt::Display for CraftObjCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client => write!(f, "Client"),
            Self::Storage => write!(f, "Storage"),
            Self::Aggregator => write!(f, "Aggregator"),
        }
    }
}

/// Announcement of a peer's capabilities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityAnnouncement {
    /// Peer ID bytes of the announcing node.
    pub peer_id: Vec<u8>,
    /// Capabilities this node supports.
    pub capabilities: Vec<CraftObjCapability>,
    /// Unix timestamp (seconds) when announcement was created.
    pub timestamp: u64,
    /// Signature over (peer_id || capabilities || timestamp) by the announcing node.
    pub signature: Vec<u8>,
    /// Storage committed (max_storage_bytes from config). 0 if not a storage node.
    #[serde(default)]
    pub storage_committed_bytes: u64,
    /// Storage currently used. 0 if not a storage node.
    #[serde(default)]
    pub storage_used_bytes: u64,
    /// Geographic region of this node (e.g. "us-east", "eu-west").
    /// Auto-detected or manually configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Root hash of the node's storage Merkle tree (all held pieces).
    #[serde(default)]
    pub storage_root: [u8; 32],
}

impl CapabilityAnnouncement {
    /// Data to be signed/verified.
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.peer_id);
        for cap in &self.capabilities {
            data.push(*cap as u8);
        }
        data.extend_from_slice(&self.timestamp.to_le_bytes());
        data.extend_from_slice(&self.storage_committed_bytes.to_le_bytes());
        data.extend_from_slice(&self.storage_used_bytes.to_le_bytes());
        if let Some(ref region) = self.region {
            data.extend_from_slice(region.as_bytes());
        }
        data.extend_from_slice(&self.storage_root);
        data
    }
}

/// Wire message types for the transfer protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireMessageType {
    /// Request any piece for a segment.
    PieceRequest = 0,
    /// Response with piece data + coefficient vector.
    PieceResponse = 1,
    /// Request the manifest for a CID.
    RecordRequest = 2,
    /// Response with manifest data.
    RecordResponse = 3,
    /// Push a piece to a storage peer (proactive distribution).
    PiecePush = 5,
    /// Push a manifest to a storage peer (sent before piece pushes).
    ManifestPush = 6,
}

impl WireMessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::PieceRequest),
            1 => Some(Self::PieceResponse),
            2 => Some(Self::RecordRequest),
            3 => Some(Self::RecordResponse),
            5 => Some(Self::PiecePush),
            6 => Some(Self::ManifestPush),
            _ => None,
        }
    }
}

/// Wire response status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WireStatus {
    Ok = 0,
    NotFound = 1,
    Error = 2,
}

impl WireStatus {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            1 => Some(Self::NotFound),
            2 => Some(Self::Error),
            _ => None,
        }
    }
}

#[derive(Error, Debug)]
pub enum CraftObjError {
    #[error("Invalid content ID: {0}")]
    InvalidContentId(String),
    #[error("Content not found: {0}")]
    ContentNotFound(String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Erasure coding error: {0}")]
    ErasureError(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Transfer error: {0}")]
    TransferError(String),
    #[error("Encryption error: {0}")]
    EncryptionError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Manifest error: {0}")]
    ManifestError(String),
}

pub type Result<T> = std::result::Result<T, CraftObjError>;

// ── Health History Snapshots ────────────────────────────────

/// A point-in-time snapshot of content health, persisted for timeline display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSnapshot {
    /// Unix milliseconds when this snapshot was taken.
    pub timestamp: u64,
    /// Content ID this snapshot is for.
    pub content_id: ContentId,
    /// Number of segments in the content.
    pub segment_count: usize,
    /// Per-segment health snapshots.
    pub segments: Vec<SegmentSnapshot>,
    /// Number of unique providers holding pieces for this content.
    pub provider_count: usize,
    /// Minimum health ratio across all segments (pieces/k).
    pub health_ratio: f64,
    /// Actions taken by HealthScan during this cycle (per segment).
    pub actions: Vec<HealthAction>,
}

/// Per-segment health snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentSnapshot {
    /// Segment index.
    pub index: u32,
    /// Rank (number of linearly independent pieces from online nodes).
    pub rank: usize,
    /// k value for this segment (pieces needed for reconstruction).
    pub k: usize,
    /// Total pieces across all providers (including redundant/dependent).
    #[serde(default)]
    pub total_pieces: usize,
    /// Number of unique providers for this segment.
    pub provider_count: usize,
}

/// Action taken by HealthScan on a specific segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthAction {
    /// A segment was repaired (new piece generated).
    Repaired { segment: u32, offset: usize },
    /// A segment was degraded (excess piece dropped).
    Degraded { segment: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_id_from_bytes() {
        let data = b"hello craftobj";
        let cid = ContentId::from_bytes(data);
        assert_eq!(cid.0.len(), 32);
        let cid2 = ContentId::from_bytes(data);
        assert_eq!(cid, cid2);
        let cid3 = ContentId::from_bytes(b"other data");
        assert_ne!(cid, cid3);
    }

    #[test]
    fn test_content_id_hex_roundtrip() {
        let cid = ContentId::from_bytes(b"test");
        let hex_str = cid.to_hex();
        let parsed = ContentId::from_hex(&hex_str).unwrap();
        assert_eq!(cid, parsed);
    }

    #[test]
    fn test_content_id_invalid_hex() {
        assert!(ContentId::from_hex("not_hex").is_err());
        assert!(ContentId::from_hex("abcd").is_err());
    }

    #[test]
    fn test_wire_message_type() {
        assert_eq!(WireMessageType::from_u8(0), Some(WireMessageType::PieceRequest));
        assert_eq!(WireMessageType::from_u8(3), Some(WireMessageType::RecordResponse));
        assert_eq!(WireMessageType::from_u8(4), None);
        assert_eq!(WireMessageType::from_u8(99), None);
    }

    #[test]
    fn test_wire_status() {
        assert_eq!(WireStatus::from_u8(0), Some(WireStatus::Ok));
        assert_eq!(WireStatus::from_u8(2), Some(WireStatus::Error));
        assert_eq!(WireStatus::from_u8(99), None);
    }

    #[test]
    fn test_wire_magic() {
        assert_eq!(&WIRE_MAGIC, b"COBJ");
    }

    #[test]
    fn test_storage_receipt_weight() {
        let receipt = StorageReceipt {
            content_id: ContentId([0u8; 32]),
            storage_node: [1u8; 32],
            challenger: [2u8; 32],
            segment_index: 0,
            piece_id: [5u8; 32],
            timestamp: 1000,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![],
        };
        assert_eq!(receipt.weight(), 1);
    }

    #[test]
    fn test_storage_receipt_signable_data() {
        let receipt = StorageReceipt {
            content_id: ContentId([0u8; 32]),
            storage_node: [1u8; 32],
            challenger: [2u8; 32],
            segment_index: 5,
            piece_id: [6u8; 32],
            timestamp: 500,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![],
        };
        let data = receipt.signable_data();
        // 32 (cid) + 32 (storage_node) + 32 (challenger) + 4 (segment_index) + 32 (piece_id) + 8 (timestamp) + 32 (nonce) + 32 (proof_hash) = 204
        assert_eq!(data.len(), 204);
    }

    #[test]
    fn test_storage_receipt_signable_deterministic() {
        let make = || StorageReceipt {
            content_id: ContentId([7u8; 32]),
            storage_node: [1u8; 32],
            challenger: [2u8; 32],
            segment_index: 3,
            piece_id: [10u8; 32],
            timestamp: 999,
            nonce: [8u8; 32],
            proof_hash: [9u8; 32],
            signature: vec![0xAA],
        };
        assert_eq!(make().signable_data(), make().signable_data());
    }

    #[test]
    fn test_storage_receipt_contribution_receipt_trait() {
        use craftec_core::ContributionReceipt;

        let receipt = StorageReceipt {
            content_id: ContentId([0u8; 32]),
            storage_node: [10u8; 32],
            challenger: [20u8; 32],
            segment_index: 0,
            piece_id: [5u8; 32],
            timestamp: 42000,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![0xAA; 64],
        };

        assert_eq!(ContributionReceipt::weight(&receipt), 1);
        assert_eq!(receipt.operator(), [10u8; 32]);
        assert_eq!(receipt.signer(), [20u8; 32]);
        assert_eq!(ContributionReceipt::timestamp(&receipt), 42000);
        assert_eq!(receipt.signature(), &[0xAA; 64]);
        // signable_data should be deterministic and 204 bytes
        let data = ContributionReceipt::signable_data(&receipt);
        assert_eq!(data.len(), 204);
        assert_eq!(data, ContributionReceipt::signable_data(&receipt));
    }

    #[test]
    fn test_storage_receipt_serde() {
        let receipt = StorageReceipt {
            content_id: ContentId([0u8; 32]),
            storage_node: [1u8; 32],
            challenger: [2u8; 32],
            segment_index: 0,
            piece_id: [5u8; 32],
            timestamp: 1000,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![5, 6, 7],
        };
        let json = serde_json::to_string(&receipt).unwrap();
        let parsed: StorageReceipt = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content_id, receipt.content_id);
        assert_eq!(parsed.storage_node, receipt.storage_node);
        assert_eq!(parsed.piece_id, receipt.piece_id);
        assert_eq!(parsed.nonce, receipt.nonce);
    }

    #[test]
    fn test_capability_serde_roundtrip() {
        let caps = vec![
            CraftObjCapability::Client,
            CraftObjCapability::Storage,
            CraftObjCapability::Aggregator,
        ];
        let json = serde_json::to_string(&caps).unwrap();
        let parsed: Vec<CraftObjCapability> = serde_json::from_str(&json).unwrap();
        assert_eq!(caps, parsed);
    }

    #[test]
    fn test_capability_announcement_serde() {
        let ann = CapabilityAnnouncement {
            peer_id: vec![1, 2, 3],
            capabilities: vec![CraftObjCapability::Storage, CraftObjCapability::Client],
            timestamp: 1700000000,
            signature: vec![0xAA, 0xBB],
            storage_committed_bytes: 10_000_000_000,
            storage_used_bytes: 5_000_000_000,
            region: Some("us-east".to_string()),
            storage_root: [0u8; 32],
        };
        let json = serde_json::to_string(&ann).unwrap();
        let parsed: CapabilityAnnouncement = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.peer_id, ann.peer_id);
        assert_eq!(parsed.capabilities, ann.capabilities);
        assert_eq!(parsed.timestamp, ann.timestamp);
    }

    #[test]
    fn test_capability_announcement_signable_data() {
        let ann = CapabilityAnnouncement {
            peer_id: vec![1, 2],
            capabilities: vec![CraftObjCapability::Storage, CraftObjCapability::Client],
            timestamp: 100,
            storage_committed_bytes: 0,
            storage_used_bytes: 0,
            region: None,
            storage_root: [0u8; 32],
            signature: vec![],
        };
        let data = ann.signable_data();
        assert_eq!(data.len(), 60); // 2 (peer_id) + 2 (caps) + 8 (ts) + 8 + 8 + 32 (storage_root)
        assert_eq!(data, ann.signable_data());
    }

    #[test]
    fn test_capability_display() {
        assert_eq!(CraftObjCapability::Storage.to_string(), "Storage");
        assert_eq!(CraftObjCapability::Aggregator.to_string(), "Aggregator");
    }

    #[test]
    fn test_content_manifest_serde() {
        let cid = ContentId::from_bytes(b"test");
        let manifest = ContentRecord {
            content_id: cid,
            total_size: 1024,
            vtags_cid: None,
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: ContentManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content_id, manifest.content_id);
        assert_eq!(parsed.total_size, 1024);
    }

    #[test]
    fn test_content_manifest_k() {
        let manifest = ContentRecord {
            content_id: ContentId([0u8; 32]),
            total_size: 10_485_760,
            vtags_cid: None,
        };
        assert_eq!(manifest.k(), SEGMENT_SIZE / PIECE_SIZE);
    }

    // NOTE: test_content_manifest_sign_verify, test_content_manifest_wrong_key_fails,
    // test_content_manifest_unsigned_verify_false removed — signing moved to COM layer.

    // NOTE: RemovalNotice tests removed — removal is COM layer concern.

    #[test]
    fn test_health_snapshot_serde() {
        let snap = HealthSnapshot {
            timestamp: 1234567890,
            content_id: ContentId([1u8; 32]),
            segment_count: 2,
            segments: vec![
                SegmentSnapshot { index: 0, rank: 3, k: 3, total_pieces: 5, provider_count: 2 },
                SegmentSnapshot { index: 1, rank: 2, k: 3, total_pieces: 3, provider_count: 1 },
            ],
            provider_count: 2,
            health_ratio: 0.667,
            actions: vec![HealthAction::Repaired { segment: 1, offset: 0 }],
        };
        let json = serde_json::to_string(&snap).unwrap();
        let parsed: HealthSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.timestamp, 1234567890);
        assert_eq!(parsed.segments.len(), 2);
        assert_eq!(parsed.actions.len(), 1);
        assert!(matches!(parsed.actions[0], HealthAction::Repaired { segment: 1, .. }));
    }

    #[test]
    fn test_extract_pubkey_from_did() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let keypair = SigningKey::generate(&mut OsRng);
        let did = did_from_pubkey(&keypair.verifying_key());
        let extracted = extract_pubkey_from_did(&did).unwrap();
        assert_eq!(extracted, keypair.verifying_key().to_bytes());

        assert!(extract_pubkey_from_did("invalid").is_none());
        assert!(extract_pubkey_from_did("did:craftec:short").is_none());
    }
}
