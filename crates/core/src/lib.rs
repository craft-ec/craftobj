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

/// StorageReceipt — proof that a provider served content.
///
/// Implements the ContributionReceipt pattern from craftec-core:
/// weight = bytes_served.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageReceipt {
    /// Content that was served.
    pub content_id: ContentId,
    /// Provider that served the content.
    pub provider: [u8; 32],
    /// Client that requested the content.
    pub client: [u8; 32],
    /// Bytes served in this receipt.
    pub bytes_served: u64,
    /// Unix timestamp when the transfer occurred.
    pub timestamp: u64,
    /// Signature from the client over the receipt data.
    pub signature: Vec<u8>,
}

impl StorageReceipt {
    /// Weight for settlement distribution (bytes served).
    pub fn weight(&self) -> u64 {
        self.bytes_served
    }

    /// Data to be signed by the client.
    pub fn signable_data(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.content_id.0);
        data.extend_from_slice(&self.provider);
        data.extend_from_slice(&self.client);
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
            provider: [1u8; 32],
            client: [2u8; 32],
            bytes_served: 65536,
            timestamp: 1000,
            signature: vec![],
        };
        assert_eq!(receipt.weight(), 65536);
    }

    #[test]
    fn test_storage_receipt_signable_data() {
        let receipt = StorageReceipt {
            content_id: ContentId([0u8; 32]),
            provider: [1u8; 32],
            client: [2u8; 32],
            bytes_served: 100,
            timestamp: 500,
            signature: vec![],
        };
        let data = receipt.signable_data();
        // 32 (cid) + 32 (provider) + 32 (client) + 8 (bytes) + 8 (timestamp) = 112
        assert_eq!(data.len(), 112);
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
