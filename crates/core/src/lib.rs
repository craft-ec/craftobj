//! DataCraft Core
//!
//! Content types and protocol primitives for DataCraft:
//! content-addressed distributed storage with RLNC erasure coding.

pub mod access;
pub mod economics;
pub mod payment_channel;
pub mod pre;
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
    pub fn from_hex(s: &str) -> std::result::Result<Self, DataCraftError> {
        let bytes = hex::decode(s).map_err(|e| DataCraftError::InvalidContentId(e.to_string()))?;
        if bytes.len() != 32 {
            return Err(DataCraftError::InvalidContentId("expected 32 bytes".into()));
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

/// Immutable manifest describing how to reconstruct content from RLNC-coded pieces.
///
/// Content is split into segments (default 10MB), each segment into pieces (default 100KB).
/// k = segment_size / piece_size (number of source pieces per segment).
/// Reconstruction requires k linearly independent coded pieces per segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentManifest {
    /// Content ID (hash of original data, or ciphertext if encrypted).
    pub content_id: ContentId,
    /// Final verification hash (SHA-256 of the content bytes).
    pub content_hash: [u8; 32],
    /// Size of each segment in bytes (default 10MB).
    pub segment_size: usize,
    /// Size of each piece in bytes (default 100KB).
    pub piece_size: usize,
    /// Number of segments the content was split into.
    pub segment_count: usize,
    /// Total size in bytes of the original content.
    pub total_size: u64,
    /// Creator's DID string (e.g., `did:craftec:<hex_pubkey>`).
    /// Empty string for unsigned manifests (backwards compat).
    #[serde(default)]
    pub creator: String,
    /// Creator's ed25519 signature over the manifest (excluding this field).
    #[serde(default)]
    pub signature: Vec<u8>,
}

impl ContentManifest {
    /// Number of source pieces per full segment (k = segment_size / piece_size).
    pub fn k(&self) -> usize {
        if self.piece_size == 0 {
            return 0;
        }
        self.segment_size / self.piece_size
    }

    /// Number of source pieces for a specific segment (last segment may be smaller).
    pub fn k_for_segment(&self, segment_index: usize) -> usize {
        if self.piece_size == 0 {
            return 0;
        }
        if segment_index + 1 < self.segment_count {
            // Full segment
            self.segment_size / self.piece_size
        } else {
            // Last segment — may be partial
            let remaining = self.total_size as usize - segment_index * self.segment_size;
            remaining.div_ceil(self.piece_size).max(1)
        }
    }

    /// Data to sign: serialized manifest fields excluding the signature.
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.content_id.0);
        data.extend_from_slice(&self.content_hash);
        data.extend_from_slice(&self.segment_size.to_le_bytes());
        data.extend_from_slice(&self.piece_size.to_le_bytes());
        data.extend_from_slice(&self.segment_count.to_le_bytes());
        data.extend_from_slice(&self.total_size.to_le_bytes());
        data.extend_from_slice(self.creator.as_bytes());
        data
    }

    /// Sign this manifest with the creator's keypair. Sets both `creator` and `signature`.
    pub fn sign(&mut self, keypair: &ed25519_dalek::SigningKey) {
        use ed25519_dalek::Signer;
        let pubkey = keypair.verifying_key();
        self.creator = format!("did:craftec:{}", hex::encode(pubkey.to_bytes()));
        let data = self.signable_data();
        let sig = keypair.sign(&data);
        self.signature = sig.to_bytes().to_vec();
    }

    /// Verify the creator's signature. Returns false if unsigned or invalid.
    pub fn verify_creator(&self) -> bool {
        if self.creator.is_empty() || self.signature.len() != 64 {
            return false;
        }
        let pubkey_bytes = match extract_pubkey_from_did(&self.creator) {
            Some(b) => b,
            None => return false,
        };
        let pubkey = match ed25519_dalek::VerifyingKey::from_bytes(&pubkey_bytes) {
            Ok(k) => k,
            Err(_) => return false,
        };
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(&self.signature);
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        let data = self.signable_data();
        pubkey.verify_strict(&data, &sig).is_ok()
    }

    /// Extract creator's public key bytes from the DID, if present and valid.
    pub fn creator_pubkey(&self) -> Option<[u8; 32]> {
        extract_pubkey_from_did(&self.creator)
    }
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

/// Content removal notice — signed by the creator to request removal of content.
///
/// Stored in the DHT so storage nodes can check before serving.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemovalNotice {
    /// Content ID to remove.
    pub cid: ContentId,
    /// Creator's DID string (must match the manifest's creator).
    pub creator: String,
    /// Unix timestamp (seconds) when the removal was requested.
    pub timestamp: u64,
    /// Optional reason for removal.
    pub reason: Option<String>,
    /// Creator's ed25519 signature over the notice (excluding this field).
    pub signature: Vec<u8>,
}

impl RemovalNotice {
    /// Data to sign: all fields except signature.
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.cid.0);
        data.extend_from_slice(self.creator.as_bytes());
        data.extend_from_slice(&self.timestamp.to_le_bytes());
        if let Some(ref reason) = self.reason {
            data.extend_from_slice(reason.as_bytes());
        }
        data
    }

    /// Create and sign a removal notice.
    pub fn sign(keypair: &ed25519_dalek::SigningKey, cid: ContentId, reason: Option<String>) -> Self {
        use ed25519_dalek::Signer;
        let pubkey = keypair.verifying_key();
        let creator = format!("did:craftec:{}", hex::encode(pubkey.to_bytes()));
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut notice = Self {
            cid,
            creator,
            timestamp,
            reason,
            signature: vec![],
        };
        let data = notice.signable_data();
        let sig = keypair.sign(&data);
        notice.signature = sig.to_bytes().to_vec();
        notice
    }

    /// Verify the signature on this removal notice.
    pub fn verify(&self) -> bool {
        if self.signature.len() != 64 {
            return false;
        }
        let pubkey_bytes = match extract_pubkey_from_did(&self.creator) {
            Some(b) => b,
            None => return false,
        };
        let pubkey = match ed25519_dalek::VerifyingKey::from_bytes(&pubkey_bytes) {
            Ok(k) => k,
            Err(_) => return false,
        };
        let mut sig_bytes = [0u8; 64];
        sig_bytes.copy_from_slice(&self.signature);
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        let data = self.signable_data();
        pubkey.verify_strict(&data, &sig).is_ok()
    }
}

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

/// Wire protocol magic bytes: "DCRF" (DataCraft)
pub const WIRE_MAGIC: [u8; 4] = [0x44, 0x43, 0x52, 0x46];

/// Transfer stream protocol ID.
pub const TRANSFER_PROTOCOL: &str = "/datacraft/transfer/2.0.0";

/// Manifest exchange protocol ID.
pub const MANIFEST_PROTOCOL: &str = "/datacraft/manifest/2.0.0";

/// PDP (Proof of Data Possession) protocol ID.
pub const PDP_PROTOCOL: &str = "/datacraft/pdp/2.0.0";

/// DHT key prefix for content providers.
pub const PROVIDERS_DHT_PREFIX: &str = "/datacraft/providers/";

/// DHT key prefix for content manifests.
pub const MANIFEST_DHT_PREFIX: &str = "/datacraft/manifest/";

/// DHT key prefix for peer pubkey → PeerId records.
pub const PEERS_DHT_PREFIX: &str = "/datacraft/peers/";

/// DHT key prefix for access lists (per CID).
pub const ACCESS_DHT_PREFIX: &str = "/datacraft/access/";

/// DHT key prefix for re-encryption keys (per CID + recipient DID).
pub const REKEY_DHT_PREFIX: &str = "/datacraft/rekey/";

/// DHT key prefix for content removal notices.
pub const REMOVAL_DHT_PREFIX: &str = "/datacraft/removal/";

/// Gossipsub topic for node status heartbeats.
pub const NODE_STATUS_TOPIC: &str = "datacraft/node-status/1.0.0";

/// Gossipsub topic for ZK-proven storage receipt summaries.
pub const PROOFS_TOPIC: &str = "datacraft/proofs/1.0.0";

/// Gossipsub topic for capability announcements.
pub const CAPABILITIES_TOPIC: &str = "datacraft/capabilities/1.0.0";

/// Gossipsub topic for content removal notices (fast propagation).
pub const REMOVAL_TOPIC: &str = "datacraft/removal/1.0.0";

/// Gossipsub topic for StorageReceipt broadcast (aggregator collection).
pub const STORAGE_RECEIPT_TOPIC: &str = "datacraft/storage-receipts/1.0.0";

/// Gossipsub topic for repair signals and announcements.
pub const REPAIR_TOPIC: &str = "datacraft/repair/1.0.0";

/// Repair signal: challenger detected under-replication for a segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairSignal {
    pub content_id: ContentId,
    pub segment_index: u32,
    pub pieces_needed: usize,
    pub current_rank: usize,
    pub k: usize,
    pub challenger: Vec<u8>,
    pub timestamp: u64,
}

/// Repair announcement: a node completed repair and stored a new piece.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairAnnouncement {
    pub content_id: ContentId,
    pub segment_index: u32,
    pub piece_id: [u8; 32],
    pub repairer: Vec<u8>,
    pub timestamp: u64,
}

/// Wrapper for repair gossipsub messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepairMessage {
    Signal(RepairSignal),
    Announcement(RepairAnnouncement),
}

/// Gossipsub topic for demand/scaling signals.
pub const SCALING_TOPIC: &str = "datacraft/scaling/1.0.0";

/// Demand signal: a serving node detected high demand for content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemandSignal {
    /// Content being requested at high rate.
    pub content_id: ContentId,
    /// Demand level (e.g., number of fetches in the window).
    pub demand_level: u32,
    /// Current number of known providers for this content.
    pub current_providers: u32,
    /// Peer ID bytes of the node reporting demand.
    pub reporter: Vec<u8>,
    /// Unix timestamp (seconds) when signal was created.
    pub timestamp: u64,
}

/// Capabilities a DataCraft node can declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataCraftCapability {
    /// Publishes and fetches content.
    Client,
    /// Stores and serves content (participates in PDP).
    Storage,
    /// Settles distributions, aggregates receipts.
    Aggregator,
}

impl std::fmt::Display for DataCraftCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client => write!(f, "Client"),
            Self::Storage => write!(f, "Storage"),
            Self::Aggregator => write!(f, "Aggregator"),
        }
    }
}

/// Announcement of a peer's capabilities, published via gossipsub.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilityAnnouncement {
    /// Peer ID bytes of the announcing node.
    pub peer_id: Vec<u8>,
    /// Capabilities this node supports.
    pub capabilities: Vec<DataCraftCapability>,
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
    ManifestRequest = 2,
    /// Response with manifest data.
    ManifestResponse = 3,
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
            2 => Some(Self::ManifestRequest),
            3 => Some(Self::ManifestResponse),
            5 => Some(Self::PiecePush),
            6 => Some(Self::ManifestPush),
            _ => None,
        }
    }
}

/// Wire response status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub enum DataCraftError {
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

pub type Result<T> = std::result::Result<T, DataCraftError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_content_id_from_bytes() {
        let data = b"hello datacraft";
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
        assert_eq!(WireMessageType::from_u8(3), Some(WireMessageType::ManifestResponse));
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
        assert_eq!(&WIRE_MAGIC, b"DCRF");
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
            DataCraftCapability::Client,
            DataCraftCapability::Storage,
            DataCraftCapability::Aggregator,
        ];
        let json = serde_json::to_string(&caps).unwrap();
        let parsed: Vec<DataCraftCapability> = serde_json::from_str(&json).unwrap();
        assert_eq!(caps, parsed);
    }

    #[test]
    fn test_capability_announcement_serde() {
        let ann = CapabilityAnnouncement {
            peer_id: vec![1, 2, 3],
            capabilities: vec![DataCraftCapability::Storage, DataCraftCapability::Client],
            timestamp: 1700000000,
            signature: vec![0xAA, 0xBB],
            storage_committed_bytes: 10_000_000_000,
            storage_used_bytes: 5_000_000_000,
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
            capabilities: vec![DataCraftCapability::Storage, DataCraftCapability::Client],
            timestamp: 100,
            signature: vec![],
            storage_committed_bytes: 0,
            storage_used_bytes: 0,
        };
        let data = ann.signable_data();
        assert_eq!(data.len(), 28);
        assert_eq!(data, ann.signable_data());
    }

    #[test]
    fn test_capability_display() {
        assert_eq!(DataCraftCapability::Storage.to_string(), "Storage");
        assert_eq!(DataCraftCapability::Aggregator.to_string(), "Aggregator");
    }

    #[test]
    fn test_content_manifest_serde() {
        let cid = ContentId::from_bytes(b"test");
        let manifest = ContentManifest {
            content_id: cid,
            content_hash: cid.0,
            segment_size: 10_485_760,
            piece_size: 102_400,
            segment_count: 1,
            total_size: 1024,
            creator: String::new(),
            signature: vec![],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: ContentManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content_id, manifest.content_id);
        assert_eq!(parsed.content_hash, cid.0);
        assert_eq!(parsed.segment_size, 10_485_760);
        assert_eq!(parsed.piece_size, 102_400);
        assert_eq!(parsed.total_size, 1024);
    }

    #[test]
    fn test_content_manifest_k() {
        let manifest = ContentManifest {
            content_id: ContentId([0u8; 32]),
            content_hash: [0u8; 32],
            segment_size: 10_485_760,
            piece_size: 102_400,
            segment_count: 1,
            total_size: 10_485_760,
            creator: String::new(),
            signature: vec![],
        };
        assert_eq!(manifest.k(), 102);
    }

    #[test]
    fn test_content_manifest_sign_verify() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let keypair = SigningKey::generate(&mut OsRng);
        let cid = ContentId::from_bytes(b"signed manifest test");
        let mut manifest = ContentManifest {
            content_id: cid,
            content_hash: cid.0,
            segment_size: 10_485_760,
            piece_size: 102_400,
            segment_count: 1,
            total_size: 2048,
            creator: String::new(),
            signature: vec![],
        };

        manifest.sign(&keypair);
        assert!(!manifest.creator.is_empty());
        assert_eq!(manifest.signature.len(), 64);
        assert!(manifest.verify_creator());

        // Tamper → invalid
        manifest.total_size = 9999;
        assert!(!manifest.verify_creator());
    }

    #[test]
    fn test_content_manifest_wrong_key_fails() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let keypair1 = SigningKey::generate(&mut OsRng);
        let keypair2 = SigningKey::generate(&mut OsRng);
        let cid = ContentId::from_bytes(b"wrong key test");
        let mut manifest = ContentManifest {
            content_id: cid,
            content_hash: cid.0,
            segment_size: 10_485_760,
            piece_size: 102_400,
            segment_count: 1,
            total_size: 1024,
            creator: String::new(),
            signature: vec![],
        };

        manifest.sign(&keypair1);
        manifest.creator = did_from_pubkey(&keypair2.verifying_key());
        assert!(!manifest.verify_creator());
    }

    #[test]
    fn test_content_manifest_unsigned_verify_false() {
        let cid = ContentId::from_bytes(b"unsigned");
        let manifest = ContentManifest {
            content_id: cid,
            content_hash: cid.0,
            segment_size: 10_485_760,
            piece_size: 102_400,
            segment_count: 1,
            total_size: 1024,
            creator: String::new(),
            signature: vec![],
        };
        assert!(!manifest.verify_creator());
    }

    #[test]
    fn test_removal_notice_sign_verify() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let keypair = SigningKey::generate(&mut OsRng);
        let cid = ContentId::from_bytes(b"remove me");

        let notice = RemovalNotice::sign(&keypair, cid, Some("test removal".into()));
        assert!(notice.verify());
        assert_eq!(notice.cid, cid);
        assert!(notice.reason.as_deref() == Some("test removal"));
    }

    #[test]
    fn test_removal_notice_tampered() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let keypair = SigningKey::generate(&mut OsRng);
        let cid = ContentId::from_bytes(b"tamper test");

        let mut notice = RemovalNotice::sign(&keypair, cid, None);
        assert!(notice.verify());

        notice.timestamp += 1;
        assert!(!notice.verify());
    }

    #[test]
    fn test_removal_notice_wrong_creator() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let keypair1 = SigningKey::generate(&mut OsRng);
        let keypair2 = SigningKey::generate(&mut OsRng);
        let cid = ContentId::from_bytes(b"wrong creator");

        let mut notice = RemovalNotice::sign(&keypair1, cid, None);
        notice.creator = did_from_pubkey(&keypair2.verifying_key());
        assert!(!notice.verify());
    }

    #[test]
    fn test_removal_notice_bincode_roundtrip() {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let keypair = SigningKey::generate(&mut OsRng);
        let cid = ContentId::from_bytes(b"bincode test");
        let notice = RemovalNotice::sign(&keypair, cid, Some("reason".into()));

        let bytes = bincode::serialize(&notice).unwrap();
        let parsed: RemovalNotice = bincode::deserialize(&bytes).unwrap();
        assert_eq!(parsed.cid, notice.cid);
        assert_eq!(parsed.creator, notice.creator);
        assert!(parsed.verify());
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
