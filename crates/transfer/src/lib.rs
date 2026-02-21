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
//!   - 0x04 = HealthQuery request
//!   - 0x81 = PieceBatch response
//!   - 0x82 = Ack response
//!   - 0x84 = HealthResponse

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
    // NOTE: ManifestPush removed — piece headers are self-describing and carry
    // all manifest metadata (content_id, total_size, segment_count, k, vtags_cid).
    // Manifest is auto-created from the first received piece header.

    /// Query how many pieces a peer holds for a segment.
    /// Commit-then-challenge: nonce prevents replay attacks.
    HealthQuery {
        content_id: ContentId,
        segment_index: u32,
        /// Nonce for commit-then-challenge: provider commits piece list first,
        /// verifier picks spot-check AFTER seeing committed list.
        nonce: [u8; 32],
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
    /// Push multiple pieces in a batch for distribution.
    PieceBatchPush {
        content_id: ContentId,
        pieces: Vec<PiecePayload>,
    },
}

/// A piece payload with self-describing header fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiecePayload {
    pub segment_index: u32,
    /// Total segments in the content.
    pub segment_count: u32,
    /// Original content size in bytes — needed for manifest auto-creation.
    pub total_size: u64,
    /// Source pieces per segment (determines redundancy via `2.0 + 16/k`).
    pub k: u32,
    /// Strategy discriminator: `Some` = erasure+vtag, `None` = replicate+hash.
    pub vtags_cid: Option<[u8; 32]>,
    pub piece_id: [u8; 32],
    pub coefficients: Vec<u8>,
    pub data: Vec<u8>,
    /// Homomorphic verification tag blob. Sent with the first piece in a batch
    /// (`Some(blob)`) so receivers can verify all future pieces. Subsequent
    /// pieces in the same batch set this to `None`.
    pub vtag_blob: Option<Vec<u8>>,
}

/// A response in the CraftObj transfer protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CraftObjResponse {
    /// Response to PieceSync: batch of pieces the peer has that we don't.
    PieceBatch { pieces: Vec<PiecePayload> },
    /// Ack for PiecePush or ManifestPush.
    Ack { status: WireStatus },
    /// Response to HealthQuery: committed list of pieces held for the segment.
    /// Verifier uses this to select spot-check targets AFTER seeing the commitment.
    HealthResponse {
        pieces: Vec<HealthPiece>,
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
    /// Batch acknowledgment for multiple piece pushes.
    BatchAck {
        confirmed_pieces: Vec<[u8; 32]>, // piece_ids that were successfully stored
        failed_pieces: Vec<[u8; 32]>,    // piece_ids that failed to store
    },
}

/// A piece commitment in a HealthResponse (piece_id + coefficients, no data).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthPiece {
    pub piece_id: [u8; 32],
    pub coefficients: Vec<u8>,
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
        let _health_query = CraftObjRequest::HealthQuery {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 0,
            nonce: [0; 32],
        };
    }

    #[test]
    fn test_response_variants() {
        let _batch = CraftObjResponse::PieceBatch { pieces: vec![] };
        let _ack = CraftObjResponse::Ack { status: WireStatus::Ok };
        let _health = CraftObjResponse::HealthResponse { pieces: vec![] };
    }
}
