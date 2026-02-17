//! DataCraft Transfer
//!
//! Piece exchange protocol for DataCraft using RLNC coding.
//!
//! Protocol: `/datacraft/transfer/3.0.0`
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

use datacraft_core::{ContentId, WireStatus};
use serde::{Deserialize, Serialize};

// ========================================================================
// Request / Response types for request_response
// ========================================================================

/// A request in the DataCraft transfer protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataCraftRequest {
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
}

/// A piece within a PieceBatch response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiecePayload {
    pub segment_index: u32,
    pub piece_id: [u8; 32],
    pub coefficients: Vec<u8>,
    pub data: Vec<u8>,
}

/// A response in the DataCraft transfer protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataCraftResponse {
    /// Response to PieceSync: batch of pieces the peer has that we don't.
    PieceBatch { pieces: Vec<PiecePayload> },
    /// Ack for PiecePush or ManifestPush.
    Ack { status: WireStatus },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_variants() {
        let _sync = DataCraftRequest::PieceSync {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 0,
            merkle_root: [0; 32],
            have_pieces: vec![],
            max_pieces: 10,
        };
        let _push = DataCraftRequest::PiecePush {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 0,
            piece_id: [0; 32],
            coefficients: vec![],
            data: vec![],
        };
        let _manifest = DataCraftRequest::ManifestPush {
            content_id: ContentId::from_bytes(b"test"),
            manifest_json: vec![],
        };
    }

    #[test]
    fn test_response_variants() {
        let _batch = DataCraftResponse::PieceBatch { pieces: vec![] };
        let _ack = DataCraftResponse::Ack { status: WireStatus::Ok };
    }
}
