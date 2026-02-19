//! CraftObj Transfer
//!
//! Piece exchange protocol for CraftObj using RLNC coding.
//!
//! Protocol: `/craftobj/transfer/3.0.0`
//!
//! Uses `libp2p_stream` with persistent two-unidirectional streams per peer.
//!
//! Wire format: `[seq_id:8 BE][type:1][payload_len:4 BE][payload (bincode)]`
//!
//! Type discriminants:
//!   - 0x01 = PieceSync request
//!   - 0x02 = PiecePush request
//!   - 0x03 = ManifestPush request
//!   - 0x81 = PieceBatch response
//!   - 0x82 = Ack response

pub mod wire;

use craftobj_core::{ContentId, WireStatus};
use serde::{Deserialize, Serialize};

// ========================================================================
// Request / Response types for request_response
// ========================================================================

/// A request in the CraftObj transfer protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CraftObjRequest {
    /// Fetch pieces for a segment. Peer sends pieces NOT in `have_pieces`.
    PieceSync {
        content_id: ContentId,
        segment_index: u32,
        merkle_root: [u8; 32],
        have_pieces: Vec<[u8; 32]>,
        max_pieces: u16,
    },
    /// Push a single piece to a storage peer (used during distribution).
    PiecePush {
        content_id: ContentId,
        segment_index: u32,
        piece_id: [u8; 32],
        coefficients: Vec<u8>,
        data: Vec<u8>,
    },
    /// Push a manifest to a storage peer. Must arrive before pieces.
    ManifestPush {
        content_id: ContentId,
        manifest_json: Vec<u8>,
    },
    /// Query PieceMap entries for a specific segment (lightweight — no piece data).
    PieceMapQuery {
        content_id: ContentId,
        segment_index: u32,
    },
    /// Request Merkle root and leaf count for a content ID.
    MerkleRoot {
        content_id: ContentId,
        segment_index: u32,
    },
    /// Request Merkle diff since a given root for a content ID.
    MerkleDiff {
        content_id: ContentId,
        segment_index: u32,
        since_root: [u8; 32],
    },
    /// PEX: peer exchange — share known peers
    PexExchange {
        payload: Vec<u8>, // serialized PexMessage
    },
    /// Request capabilities from a peer (sent on connection established).
    CapabilityRequest,
    /// PDP challenge request: challenger sends nonce + byte positions for verification.
    PdpChallenge {
        content_id: ContentId,
        segment_index: u32,
        piece_id: [u8; 32],
        nonce: [u8; 32],
        byte_positions: Vec<u32>,
    },
}

/// A piece within a PieceBatch response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiecePayload {
    pub segment_index: u32,
    pub piece_id: [u8; 32],
    pub coefficients: Vec<u8>,
    pub data: Vec<u8>,
}

/// A PieceMap entry returned by PieceMapQuery — metadata only, no piece data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PieceMapEntry {
    /// Node that holds this piece (PeerId bytes).
    pub node: Vec<u8>,
    /// Piece identifier (SHA-256 of coefficients).
    pub piece_id: [u8; 32],
    /// Coefficient vector over GF(2^8).
    pub coefficients: Vec<u8>,
}

/// A response in the CraftObj transfer protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CraftObjResponse {
    /// Response to PieceSync: batch of pieces the peer has that we don't.
    PieceBatch { pieces: Vec<PiecePayload> },
    /// Ack for PiecePush or ManifestPush.
    Ack { status: WireStatus },
    /// Response to PieceMapQuery: known PieceMap entries for the queried segment.
    PieceMapEntries { entries: Vec<PieceMapEntry> },
    /// Response to MerkleRoot: current root and leaf count.
    MerkleRootResponse {
        root: [u8; 32],
        leaf_count: u32,
    },
    /// Response to MerkleDiff: current root and piece changes.
    MerkleDiffResponse {
        current_root: [u8; 32],
        added: Vec<PieceMapEntry>, // piece_id + coefficients for new pieces
        removed: Vec<u32>,          // piece_ids that were dropped (first 4 bytes as u32)
    },
    /// Response to PexExchange: return our own peer list
    PexExchangeResponse {
        payload: Vec<u8>, // serialized PexMessage
    },
    /// Response to CapabilityRequest: node capabilities and storage info.
    CapabilityResponse {
        capabilities: Vec<String>,
        storage_committed_bytes: u64,
        storage_used_bytes: u64,
        region: Option<String>,
    },
    /// Response to PdpChallenge: proof of data possession.
    PdpProof {
        piece_id: [u8; 32],
        coefficients: Vec<u8>,
        challenged_bytes: Vec<u8>, // bytes at the challenged positions
        proof_hash: [u8; 32],     // hash of (data_bytes + positions + coefficients + nonce)
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_variants() {
        let _sync = CraftObjRequest::PieceSync {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 0,
            merkle_root: [0; 32],
            have_pieces: vec![],
            max_pieces: 10,
        };
        let _push = CraftObjRequest::PiecePush {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 0,
            piece_id: [0; 32],
            coefficients: vec![],
            data: vec![],
        };
        let _manifest = CraftObjRequest::ManifestPush {
            content_id: ContentId::from_bytes(b"test"),
            manifest_json: vec![],
        };
        let _merkle_root = CraftObjRequest::MerkleRoot {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 0,
        };
        let _merkle_diff = CraftObjRequest::MerkleDiff {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 0,
            since_root: [0xAA; 32],
        };
    }

    #[test]
    fn test_response_variants() {
        let _batch = CraftObjResponse::PieceBatch { pieces: vec![] };
        let _ack = CraftObjResponse::Ack { status: WireStatus::Ok };
        let _merkle_root_resp = CraftObjResponse::MerkleRootResponse {
            root: [0xBB; 32],
            leaf_count: 42,
        };
        let _merkle_diff_resp = CraftObjResponse::MerkleDiffResponse {
            current_root: [0xCC; 32],
            added: vec![],
            removed: vec![],
        };
    }

    // Note: JSON serialization tests removed to avoid adding serde_json dependency.
    // The wire protocol tests in wire.rs provide sufficient coverage for serialization.
}
