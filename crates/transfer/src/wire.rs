//! Wire framing for persistent CraftObj streams.
//!
//! Wire format: `[seq_id: u64 BE][type: u8][len: u32 BE][payload: bytes]`
//!
//! Generic frame read/write is provided by `craftec_network::wire`.
//! This module adds CraftObj-specific message serialization on top.

use std::io;

use futures::prelude::*;

use crate::{CraftObjRequest, CraftObjResponse, PiecePayload};
use craftobj_core::{ContentId, WireStatus};
use craftec_network::wire::{read_raw_frame, write_raw_frame};

// Type discriminants
const TYPE_PIECE_SYNC: u8 = 0x01;
const TYPE_PIECE_PUSH: u8 = 0x02;
const TYPE_MANIFEST_PUSH: u8 = 0x03;
const TYPE_HEALTH_QUERY: u8 = 0x04;
const TYPE_PDP_CHALLENGE: u8 = 0x07;
const TYPE_PEX_EXCHANGE: u8 = 0x08;
const TYPE_PIECE_BATCH_PUSH: u8 = 0x09;
const TYPE_PIECE_BATCH: u8 = 0x81;
const TYPE_ACK: u8 = 0x82;
const TYPE_HEALTH_RESPONSE: u8 = 0x84;
const TYPE_PDP_PROOF: u8 = 0x86;
const TYPE_PEX_EXCHANGE_RESPONSE: u8 = 0x87;
const TYPE_BATCH_ACK: u8 = 0x89;
const TYPE_CAPABILITY_REQUEST: u8 = 0x0A;
const TYPE_CAPABILITY_RESPONSE: u8 = 0x88;

/// A parsed frame from a persistent stream.
#[derive(Debug)]
pub enum StreamFrame {
    /// A request message from the peer.
    Request { seq_id: u64, request: CraftObjRequest },
    /// A response message from the peer.
    Response { seq_id: u64, response: CraftObjResponse },
}

/// Write a request frame: `[seq_id:8][type:1][len:4][payload]` + flush.
pub async fn write_request_frame<T: AsyncWrite + Unpin>(
    io: &mut T,
    seq_id: u64,
    request: &CraftObjRequest,
) -> io::Result<()> {
    let (msg_type, payload) = serialize_request(request)?;
    write_raw_frame(io, seq_id, msg_type, &payload).await
}

/// Write a response frame: `[seq_id:8][type:1][len:4][payload]` + flush.
pub async fn write_response_frame<T: AsyncWrite + Unpin>(
    io: &mut T,
    seq_id: u64,
    response: &CraftObjResponse,
) -> io::Result<()> {
    let (msg_type, payload) = serialize_response(response)?;
    write_raw_frame(io, seq_id, msg_type, &payload).await
}

/// Read a single frame from a persistent stream.
pub async fn read_frame<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<StreamFrame> {
    let raw = read_raw_frame(io).await?;

    match raw.msg_type {
        TYPE_PIECE_SYNC | TYPE_PIECE_PUSH | TYPE_MANIFEST_PUSH | TYPE_HEALTH_QUERY
        | TYPE_PDP_CHALLENGE | TYPE_PEX_EXCHANGE | TYPE_PIECE_BATCH_PUSH
        | TYPE_CAPABILITY_REQUEST => {
            let request = deserialize_request(raw.msg_type, &raw.payload)?;
            Ok(StreamFrame::Request { seq_id: raw.seq_id, request })
        }
        TYPE_PIECE_BATCH | TYPE_ACK | TYPE_HEALTH_RESPONSE | TYPE_PDP_PROOF
        | TYPE_PEX_EXCHANGE_RESPONSE | TYPE_BATCH_ACK | TYPE_CAPABILITY_RESPONSE => {
            let response = deserialize_response(raw.msg_type, &raw.payload)?;
            Ok(StreamFrame::Response { seq_id: raw.seq_id, response })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown frame type: 0x{:02x}", raw.msg_type),
        )),
    }
}

// ========================================================================
// Internal helpers
// ========================================================================

fn serialize_request(request: &CraftObjRequest) -> io::Result<(u8, Vec<u8>)> {
    match request {
        CraftObjRequest::PieceSync { content_id, segment_index, merkle_root, have_pieces, max_pieces } => {
            let inner = PieceSyncWire {
                content_id: *content_id,
                segment_index: *segment_index,
                merkle_root: *merkle_root,
                have_pieces: have_pieces.clone(),
                max_pieces: *max_pieces,
            };
            let payload = bincode::serialize(&inner).map_err(io::Error::other)?;
            Ok((TYPE_PIECE_SYNC, payload))
        }
        CraftObjRequest::PiecePush { content_id, segment_index, piece_id, coefficients, data } => {
            let inner = PiecePushWire {
                content_id: *content_id,
                segment_index: *segment_index,
                piece_id: *piece_id,
                coefficients: coefficients.clone(),
                data: data.clone(),
            };
            let payload = bincode::serialize(&inner).map_err(io::Error::other)?;
            Ok((TYPE_PIECE_PUSH, payload))
        }
        CraftObjRequest::ManifestPush { content_id, record_json } => {
            let inner = RecordPushWire {
                content_id: *content_id,
                record_json: record_json.clone(),
            };
            let payload = bincode::serialize(&inner).map_err(io::Error::other)?;
            Ok((TYPE_MANIFEST_PUSH, payload))
        }
        CraftObjRequest::HealthQuery { content_id, segment_index } => {
            let inner = HealthQueryWire {
                content_id: *content_id,
                segment_index: *segment_index,
            };
            let payload = bincode::serialize(&inner).map_err(io::Error::other)?;
            Ok((TYPE_HEALTH_QUERY, payload))
        }
        CraftObjRequest::PdpChallenge { content_id, segment_index, piece_id, nonce, byte_positions } => {
            let inner = PdpChallengeWire {
                content_id: *content_id,
                segment_index: *segment_index,
                piece_id: *piece_id,
                nonce: *nonce,
                byte_positions: byte_positions.clone(),
            };
            let payload = bincode::serialize(&inner).map_err(io::Error::other)?;
            Ok((TYPE_PDP_CHALLENGE, payload))
        }
        CraftObjRequest::PexExchange { payload } => {
            Ok((TYPE_PEX_EXCHANGE, payload.clone()))
        }
        CraftObjRequest::PieceBatchPush { content_id, pieces } => {
            let inner = PieceBatchPushWire {
                content_id: *content_id,
                pieces: pieces.clone(),
            };
            let payload = bincode::serialize(&inner).map_err(io::Error::other)?;
            Ok((TYPE_PIECE_BATCH_PUSH, payload))
        }
        CraftObjRequest::CapabilityRequest => {
            Ok((TYPE_CAPABILITY_REQUEST, vec![]))
        }
    }
}

fn deserialize_request(msg_type: u8, payload: &[u8]) -> io::Result<CraftObjRequest> {
    match msg_type {
        TYPE_PIECE_SYNC => {
            let inner: PieceSyncWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjRequest::PieceSync {
                content_id: inner.content_id,
                segment_index: inner.segment_index,
                merkle_root: inner.merkle_root,
                have_pieces: inner.have_pieces,
                max_pieces: inner.max_pieces,
            })
        }
        TYPE_PIECE_PUSH => {
            let inner: PiecePushWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjRequest::PiecePush {
                content_id: inner.content_id,
                segment_index: inner.segment_index,
                piece_id: inner.piece_id,
                coefficients: inner.coefficients,
                data: inner.data,
            })
        }
        TYPE_MANIFEST_PUSH => {
            let inner: RecordPushWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjRequest::ManifestPush {
                content_id: inner.content_id,
                record_json: inner.record_json,
            })
        }
        TYPE_HEALTH_QUERY => {
            let inner: HealthQueryWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjRequest::HealthQuery {
                content_id: inner.content_id,
                segment_index: inner.segment_index,
            })
        }
        TYPE_PDP_CHALLENGE => {
            let inner: PdpChallengeWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjRequest::PdpChallenge {
                content_id: inner.content_id,
                segment_index: inner.segment_index,
                piece_id: inner.piece_id,
                nonce: inner.nonce,
                byte_positions: inner.byte_positions,
            })
        }
        TYPE_PEX_EXCHANGE => {
            Ok(CraftObjRequest::PexExchange { payload: payload.to_vec() })
        }
        TYPE_PIECE_BATCH_PUSH => {
            let inner: PieceBatchPushWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjRequest::PieceBatchPush {
                content_id: inner.content_id,
                pieces: inner.pieces,
            })
        }
        TYPE_CAPABILITY_REQUEST => {
            Ok(CraftObjRequest::CapabilityRequest)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown request type: 0x{:02x}", msg_type),
        )),
    }
}

fn serialize_response(response: &CraftObjResponse) -> io::Result<(u8, Vec<u8>)> {
    match response {
        CraftObjResponse::PieceBatch { pieces } => {
            let payload = bincode::serialize(pieces).map_err(io::Error::other)?;
            Ok((TYPE_PIECE_BATCH, payload))
        }
        CraftObjResponse::Ack { status } => {
            let payload = bincode::serialize(status).map_err(io::Error::other)?;
            Ok((TYPE_ACK, payload))
        }
        CraftObjResponse::HealthResponse { piece_count } => {
            let payload = bincode::serialize(piece_count).map_err(io::Error::other)?;
            Ok((TYPE_HEALTH_RESPONSE, payload))
        }
        CraftObjResponse::PdpProof { piece_id, coefficients, challenged_bytes, proof_hash } => {
            let inner = PdpProofWire {
                piece_id: *piece_id,
                coefficients: coefficients.clone(),
                challenged_bytes: challenged_bytes.clone(),
                proof_hash: *proof_hash,
            };
            let payload = bincode::serialize(&inner).map_err(io::Error::other)?;
            Ok((TYPE_PDP_PROOF, payload))
        }
        CraftObjResponse::PexExchangeResponse { payload } => {
            Ok((TYPE_PEX_EXCHANGE_RESPONSE, payload.clone()))
        }
        CraftObjResponse::BatchAck { confirmed_pieces, failed_pieces } => {
            let inner = BatchAckWire {
                confirmed_pieces: confirmed_pieces.clone(),
                failed_pieces: failed_pieces.clone(),
            };
            let payload = bincode::serialize(&inner).map_err(io::Error::other)?;
            Ok((TYPE_BATCH_ACK, payload))
        }
        CraftObjResponse::CapabilityResponse { capabilities, storage_committed_bytes, storage_used_bytes, region } => {
            let payload = serde_json::to_vec(&serde_json::json!({
                "capabilities": capabilities,
                "storage_committed_bytes": storage_committed_bytes,
                "storage_used_bytes": storage_used_bytes,
                "region": region,
            })).map_err(io::Error::other)?;
            Ok((TYPE_CAPABILITY_RESPONSE, payload))
        }
    }
}

fn deserialize_response(msg_type: u8, payload: &[u8]) -> io::Result<CraftObjResponse> {
    match msg_type {
        TYPE_PIECE_BATCH => {
            let pieces: Vec<PiecePayload> = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjResponse::PieceBatch { pieces })
        }
        TYPE_ACK => {
            let status: WireStatus = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjResponse::Ack { status })
        }
        TYPE_HEALTH_RESPONSE => {
            let piece_count: u32 = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjResponse::HealthResponse { piece_count })
        }
        TYPE_PDP_PROOF => {
            let inner: PdpProofWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjResponse::PdpProof {
                piece_id: inner.piece_id,
                coefficients: inner.coefficients,
                challenged_bytes: inner.challenged_bytes,
                proof_hash: inner.proof_hash,
            })
        }
        TYPE_PEX_EXCHANGE_RESPONSE => {
            Ok(CraftObjResponse::PexExchangeResponse { payload: payload.to_vec() })
        }
        TYPE_BATCH_ACK => {
            let inner: BatchAckWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjResponse::BatchAck {
                confirmed_pieces: inner.confirmed_pieces,
                failed_pieces: inner.failed_pieces,
            })
        }
        TYPE_CAPABILITY_RESPONSE => {
            let v: serde_json::Value = serde_json::from_slice(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftObjResponse::CapabilityResponse {
                capabilities: v["capabilities"].as_array()
                    .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
                    .unwrap_or_default(),
                storage_committed_bytes: v["storage_committed_bytes"].as_u64().unwrap_or(0),
                storage_used_bytes: v["storage_used_bytes"].as_u64().unwrap_or(0),
                region: v["region"].as_str().map(String::from),
            })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown response type: 0x{:02x}", msg_type),
        )),
    }
}

// Wire serialization structs
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct PieceSyncWire {
    content_id: ContentId,
    segment_index: u32,
    merkle_root: [u8; 32],
    have_pieces: Vec<[u8; 32]>,
    max_pieces: u16,
}

#[derive(Serialize, Deserialize)]
struct PiecePushWire {
    content_id: ContentId,
    segment_index: u32,
    piece_id: [u8; 32],
    coefficients: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct RecordPushWire {
    content_id: ContentId,
    record_json: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct HealthQueryWire {
    content_id: ContentId,
    segment_index: u32,
}

#[derive(Serialize, Deserialize)]
struct PdpChallengeWire {
    content_id: ContentId,
    segment_index: u32,
    piece_id: [u8; 32],
    nonce: [u8; 32],
    byte_positions: Vec<u32>,
}

#[derive(Serialize, Deserialize)]
struct PdpProofWire {
    piece_id: [u8; 32],
    coefficients: Vec<u8>,
    challenged_bytes: Vec<u8>,
    proof_hash: [u8; 32],
}

#[derive(Serialize, Deserialize)]
struct PieceBatchPushWire {
    content_id: ContentId,
    pieces: Vec<PiecePayload>,
}

#[derive(Serialize, Deserialize)]
struct BatchAckWire {
    confirmed_pieces: Vec<[u8; 32]>,
    failed_pieces: Vec<[u8; 32]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request_frame_roundtrip() {
        let req = CraftObjRequest::PieceSync {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 3,
            merkle_root: [0xAB; 32],
            have_pieces: vec![[0x01; 32]],
            max_pieces: 10,
        };

        let mut buf = Vec::new();
        write_request_frame(&mut futures::io::Cursor::new(&mut buf), 42, &req)
            .await
            .unwrap();

        let frame = read_frame(&mut futures::io::Cursor::new(&buf)).await.unwrap();
        match frame {
            StreamFrame::Request { seq_id, request } => {
                assert_eq!(seq_id, 42);
                match request {
                    CraftObjRequest::PieceSync { segment_index, max_pieces, .. } => {
                        assert_eq!(segment_index, 3);
                        assert_eq!(max_pieces, 10);
                    }
                    _ => panic!("wrong variant"),
                }
            }
            _ => panic!("expected request frame"),
        }
    }

    #[tokio::test]
    async fn test_response_frame_roundtrip() {
        let resp = CraftObjResponse::Ack { status: WireStatus::Ok };

        let mut buf = Vec::new();
        write_response_frame(&mut futures::io::Cursor::new(&mut buf), 99, &resp)
            .await
            .unwrap();

        let frame = read_frame(&mut futures::io::Cursor::new(&buf)).await.unwrap();
        match frame {
            StreamFrame::Response { seq_id, response } => {
                assert_eq!(seq_id, 99);
                match response {
                    CraftObjResponse::Ack { status } => {
                        assert_eq!(status as u8, WireStatus::Ok as u8);
                    }
                    _ => panic!("wrong variant"),
                }
            }
            _ => panic!("expected response frame"),
        }
    }

    #[tokio::test]
    async fn test_health_query_roundtrip() {
        let req = CraftObjRequest::HealthQuery {
            content_id: ContentId::from_bytes(b"health-test"),
            segment_index: 2,
        };

        let mut buf = Vec::new();
        write_request_frame(&mut futures::io::Cursor::new(&mut buf), 77, &req)
            .await
            .unwrap();

        let frame = read_frame(&mut futures::io::Cursor::new(&buf)).await.unwrap();
        match frame {
            StreamFrame::Request { seq_id, request } => {
                assert_eq!(seq_id, 77);
                match request {
                    CraftObjRequest::HealthQuery { content_id: _, segment_index } => {
                        assert_eq!(segment_index, 2);
                    }
                    _ => panic!("wrong variant"),
                }
            }
            _ => panic!("expected request frame"),
        }
    }

    #[tokio::test]
    async fn test_health_response_roundtrip() {
        let resp = CraftObjResponse::HealthResponse { piece_count: 17 };

        let mut buf = Vec::new();
        write_response_frame(&mut futures::io::Cursor::new(&mut buf), 88, &resp)
            .await
            .unwrap();

        let frame = read_frame(&mut futures::io::Cursor::new(&buf)).await.unwrap();
        match frame {
            StreamFrame::Response { seq_id, response } => {
                assert_eq!(seq_id, 88);
                match response {
                    CraftObjResponse::HealthResponse { piece_count } => {
                        assert_eq!(piece_count, 17);
                    }
                    _ => panic!("wrong variant"),
                }
            }
            _ => panic!("expected response frame"),
        }
    }
}
