//! DataCraft Core
//!
//! Content types and protocol primitives for DataCraft:
//! content-addressed distributed storage with erasure coding.

use craftec_erasure::ErasureConfig;
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

/// Identifies a specific chunk within a content item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId {
    pub content: ContentId,
    pub index: u32,
}

/// Manifest describing how a content item is chunked and erasure-encoded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkManifest {
    /// Content ID (hash of original data, or ciphertext if encrypted).
    pub content_id: ContentId,
    /// Total size in bytes of the original content.
    pub total_size: u64,
    /// Number of chunks the content was split into.
    pub chunk_count: u32,
    /// Chunk size in bytes (last chunk may be smaller).
    pub chunk_size: usize,
    /// Erasure coding parameters used.
    pub erasure_config: ErasureConfig,
    /// Whether the content is encrypted.
    pub encrypted: bool,
    /// Per-chunk information: sizes of each chunk before erasure encoding.
    pub chunk_sizes: Vec<usize>,
}

/// Options for publishing content.
#[derive(Debug, Clone, Default)]
pub struct PublishOptions {
    /// Encrypt content before publishing. The caller must store the key.
    pub encrypted: bool,
    /// Custom erasure coding config. Defaults to 4/4 at 64KB.
    pub erasure_config: Option<ErasureConfig>,
}

/// Default erasure coding config for DataCraft: 4 data + 4 parity, 64KB chunks.
pub fn default_erasure_config() -> ErasureConfig {
    ErasureConfig {
        data_shards: 4,
        parity_shards: 4,
        chunk_size: 65536,
    }
}

/// StorageReceipt — issued by PDP challenger on successful proof-of-possession.
///
/// Weight: 1 per proof (proving you still hold the shard).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageReceipt {
    /// Content whose shard was proven.
    pub content_id: ContentId,
    /// Node that proved possession.
    pub storage_node: [u8; 32],
    /// Node that issued the challenge.
    pub challenger: [u8; 32],
    /// Which shard was proven.
    pub shard_index: u32,
    /// When the proof occurred.
    pub timestamp: u64,
    /// Challenge nonce.
    pub nonce: [u8; 32],
    /// hash(shard_data || nonce) response.
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
        data.extend_from_slice(&self.shard_index.to_le_bytes());
        data.extend_from_slice(&self.timestamp.to_le_bytes());
        data.extend_from_slice(&self.nonce);
        data.extend_from_slice(&self.proof_hash);
        data
    }
}

/// TransferReceipt — issued on every shard transfer, signed by requester.
///
/// Weight: bytes_served (proving you served data).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferReceipt {
    /// Content that was served.
    pub content_id: ContentId,
    /// Node that served the shard.
    pub server_node: [u8; 32],
    /// Node that requested (signs this).
    pub requester: [u8; 32],
    /// Which shard was served.
    pub shard_index: u32,
    /// Bytes served in this transfer.
    pub bytes_served: u64,
    /// When the transfer occurred.
    pub timestamp: u64,
    /// Requester's signature over receipt.
    pub signature: Vec<u8>,
}

impl TransferReceipt {
    /// Weight for settlement: bytes served.
    pub fn weight(&self) -> u64 {
        self.bytes_served
    }

    /// Data to be signed by the requester.
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.content_id.0);
        data.extend_from_slice(&self.server_node);
        data.extend_from_slice(&self.requester);
        data.extend_from_slice(&self.shard_index.to_le_bytes());
        data.extend_from_slice(&self.bytes_served.to_le_bytes());
        data.extend_from_slice(&self.timestamp.to_le_bytes());
        data
    }
}

/// Wire protocol magic bytes: "DCRF" (DataCraft)
pub const WIRE_MAGIC: [u8; 4] = [0x44, 0x43, 0x52, 0x46];

/// Transfer stream protocol ID.
pub const TRANSFER_PROTOCOL: &str = "/datacraft/transfer/1.0.0";

/// Manifest exchange protocol ID.
pub const MANIFEST_PROTOCOL: &str = "/datacraft/manifest/1.0.0";

/// DHT key prefix for content providers.
pub const PROVIDERS_DHT_PREFIX: &str = "/datacraft/providers/";

/// DHT key prefix for content manifests.
pub const MANIFEST_DHT_PREFIX: &str = "/datacraft/manifest/";

/// DHT key prefix for peer pubkey → PeerId records.
pub const PEERS_DHT_PREFIX: &str = "/datacraft/peers/";

/// Gossipsub topic for node status heartbeats.
pub const NODE_STATUS_TOPIC: &str = "datacraft/node-status/1.0.0";

/// Gossipsub topic for ZK-proven storage receipt summaries.
pub const PROOFS_TOPIC: &str = "datacraft/proofs/1.0.0";

/// Gossipsub topic for capability announcements.
pub const CAPABILITIES_TOPIC: &str = "datacraft/capabilities/1.0.0";

/// Capabilities a DataCraft node can declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataCraftCapability {
    /// Stores and serves content (participates in PDP).
    Storage,
    /// Forwards requests, can cache hot content.
    Relay,
    /// Publishes and fetches content.
    Client,
    /// Settles distributions, aggregates receipts.
    Aggregator,
}

impl std::fmt::Display for DataCraftCapability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage => write!(f, "Storage"),
            Self::Relay => write!(f, "Relay"),
            Self::Client => write!(f, "Client"),
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
        data
    }
}

/// Wire message types for the transfer protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireMessageType {
    /// Request a shard.
    ShardRequest = 0,
    /// Response with shard data.
    ShardResponse = 1,
    /// Request the manifest for a CID.
    ManifestRequest = 2,
    /// Response with manifest data.
    ManifestResponse = 3,
}

impl WireMessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::ShardRequest),
            1 => Some(Self::ShardResponse),
            2 => Some(Self::ManifestRequest),
            3 => Some(Self::ManifestResponse),
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

        // Same input → same CID
        let cid2 = ContentId::from_bytes(data);
        assert_eq!(cid, cid2);

        // Different input → different CID
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
        assert!(ContentId::from_hex("abcd").is_err()); // too short
    }

    #[test]
    fn test_default_erasure_config() {
        let config = default_erasure_config();
        assert_eq!(config.data_shards, 4);
        assert_eq!(config.parity_shards, 4);
        assert_eq!(config.chunk_size, 65536);
    }

    #[test]
    fn test_wire_message_type() {
        assert_eq!(WireMessageType::from_u8(0), Some(WireMessageType::ShardRequest));
        assert_eq!(WireMessageType::from_u8(3), Some(WireMessageType::ManifestResponse));
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
            shard_index: 0,
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
            shard_index: 5,
            timestamp: 500,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![],
        };
        let data = receipt.signable_data();
        // 32 (cid) + 32 (storage_node) + 32 (challenger) + 4 (shard_index) + 8 (timestamp) + 32 (nonce) + 32 (proof_hash) = 172
        assert_eq!(data.len(), 172);
    }

    #[test]
    fn test_storage_receipt_signable_deterministic() {
        let make = || StorageReceipt {
            content_id: ContentId([7u8; 32]),
            storage_node: [1u8; 32],
            challenger: [2u8; 32],
            shard_index: 3,
            timestamp: 999,
            nonce: [8u8; 32],
            proof_hash: [9u8; 32],
            signature: vec![0xAA],
        };
        // signature must NOT appear in signable_data
        assert_eq!(make().signable_data(), make().signable_data());
        assert!(!make().signable_data().windows(1).any(|w| w == [0xAA]));
    }

    #[test]
    fn test_storage_receipt_serde() {
        let receipt = StorageReceipt {
            content_id: ContentId([0u8; 32]),
            storage_node: [1u8; 32],
            challenger: [2u8; 32],
            shard_index: 0,
            timestamp: 1000,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![5, 6, 7],
        };
        let json = serde_json::to_string(&receipt).unwrap();
        let parsed: StorageReceipt = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content_id, receipt.content_id);
        assert_eq!(parsed.storage_node, receipt.storage_node);
        assert_eq!(parsed.nonce, receipt.nonce);
        assert_eq!(parsed.proof_hash, receipt.proof_hash);
    }

    #[test]
    fn test_transfer_receipt_weight() {
        let receipt = TransferReceipt {
            content_id: ContentId([0u8; 32]),
            server_node: [1u8; 32],
            requester: [2u8; 32],
            shard_index: 0,
            bytes_served: 65536,
            timestamp: 1000,
            signature: vec![],
        };
        assert_eq!(receipt.weight(), 65536);
    }

    #[test]
    fn test_transfer_receipt_signable_data() {
        let receipt = TransferReceipt {
            content_id: ContentId([0u8; 32]),
            server_node: [1u8; 32],
            requester: [2u8; 32],
            shard_index: 2,
            bytes_served: 100,
            timestamp: 500,
            signature: vec![],
        };
        let data = receipt.signable_data();
        // 32 (cid) + 32 (server_node) + 32 (requester) + 4 (shard_index) + 8 (bytes) + 8 (timestamp) = 116
        assert_eq!(data.len(), 116);
    }

    #[test]
    fn test_transfer_receipt_serde() {
        let receipt = TransferReceipt {
            content_id: ContentId([0u8; 32]),
            server_node: [1u8; 32],
            requester: [2u8; 32],
            shard_index: 1,
            bytes_served: 4096,
            timestamp: 2000,
            signature: vec![10, 20],
        };
        let json = serde_json::to_string(&receipt).unwrap();
        let parsed: TransferReceipt = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.bytes_served, 4096);
        assert_eq!(parsed.shard_index, 1);
        assert_eq!(parsed.server_node, [1u8; 32]);
    }

    #[test]
    fn test_capability_serde_roundtrip() {
        let caps = vec![
            DataCraftCapability::Storage,
            DataCraftCapability::Relay,
            DataCraftCapability::Client,
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
        };
        let json = serde_json::to_string(&ann).unwrap();
        let parsed: CapabilityAnnouncement = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.peer_id, ann.peer_id);
        assert_eq!(parsed.capabilities, ann.capabilities);
        assert_eq!(parsed.timestamp, ann.timestamp);
        assert_eq!(parsed.signature, ann.signature);
    }

    #[test]
    fn test_capability_announcement_signable_data() {
        let ann = CapabilityAnnouncement {
            peer_id: vec![1, 2],
            capabilities: vec![DataCraftCapability::Storage, DataCraftCapability::Relay],
            timestamp: 100,
            signature: vec![],
        };
        let data = ann.signable_data();
        // 2 (peer_id) + 2 (capability bytes) + 8 (timestamp) = 12
        assert_eq!(data.len(), 12);
        // Deterministic
        assert_eq!(data, ann.signable_data());
    }

    #[test]
    fn test_capability_display() {
        assert_eq!(DataCraftCapability::Storage.to_string(), "Storage");
        assert_eq!(DataCraftCapability::Aggregator.to_string(), "Aggregator");
    }

    #[test]
    fn test_chunk_manifest_serde() {
        let manifest = ChunkManifest {
            content_id: ContentId::from_bytes(b"test"),
            total_size: 1024,
            chunk_count: 1,
            chunk_size: 65536,
            erasure_config: default_erasure_config(),
            encrypted: false,
            chunk_sizes: vec![1024],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: ChunkManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content_id, manifest.content_id);
        assert_eq!(parsed.total_size, 1024);
    }
}
