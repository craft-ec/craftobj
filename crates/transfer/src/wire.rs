//! Wire framing for persistent CraftOBJ streams.
//!
//! Wire format: `[seq_id: u64 BE][type: u8][len: u32 BE][payload: bytes]`
//!
//! Flush after every write — yamux buffers data, flush pushes it to the wire.

use std::io;

use futures::prelude::*;

use crate::{CraftOBJRequest, CraftOBJResponse, PieceMapEntry, PiecePayload};
use craftobj_core::{ContentId, WireStatus};

// Type discriminants (matching existing codec)
const TYPE_PIECE_SYNC: u8 = 0x01;
const TYPE_PIECE_PUSH: u8 = 0x02;
const TYPE_MANIFEST_PUSH: u8 = 0x03;
const TYPE_PIECE_MAP_QUERY: u8 = 0x04;
const TYPE_PIECE_BATCH: u8 = 0x81;
const TYPE_ACK: u8 = 0x82;
const TYPE_PIECE_MAP_ENTRIES: u8 = 0x83;

/// Maximum frame payload (50 MB).
const MAX_FRAME_PAYLOAD: usize = 50 * 1024 * 1024;

/// A parsed frame from a persistent stream.
#[derive(Debug)]
pub enum StreamFrame {
    /// A request message from the peer.
    Request { seq_id: u64, request: CraftOBJRequest },
    /// A response message from the peer.
    Response { seq_id: u64, response: CraftOBJResponse },
}

/// Write a request frame: `[seq_id:8][type:1][len:4][payload]` + flush.
pub async fn write_request_frame<T: AsyncWrite + Unpin>(
    io: &mut T,
    seq_id: u64,
    request: &CraftOBJRequest,
) -> io::Result<()> {
    let (msg_type, payload) = serialize_request(request)?;
    write_raw_frame(io, seq_id, msg_type, &payload).await
}

/// Write a response frame: `[seq_id:8][type:1][len:4][payload]` + flush.
pub async fn write_response_frame<T: AsyncWrite + Unpin>(
    io: &mut T,
    seq_id: u64,
    response: &CraftOBJResponse,
) -> io::Result<()> {
    let (msg_type, payload) = serialize_response(response)?;
    write_raw_frame(io, seq_id, msg_type, &payload).await
}

/// Read a single frame from a persistent stream.
pub async fn read_frame<T: AsyncRead + Unpin>(io: &mut T) -> io::Result<StreamFrame> {
    // Read seq_id (8 bytes)
    let mut seq_bytes = [0u8; 8];
    io.read_exact(&mut seq_bytes).await?;
    let seq_id = u64::from_be_bytes(seq_bytes);

    // Read type (1 byte)
    let mut ty = [0u8; 1];
    io.read_exact(&mut ty).await?;

    // Read length (4 bytes)
    let mut len_bytes = [0u8; 4];
    io.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    if len > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Frame payload too large: {} > {}", len, MAX_FRAME_PAYLOAD),
        ));
    }

    // Read payload
    let mut payload = vec![0u8; len];
    if len > 0 {
        io.read_exact(&mut payload).await?;
    }

    match ty[0] {
        TYPE_PIECE_SYNC | TYPE_PIECE_PUSH | TYPE_MANIFEST_PUSH | TYPE_PIECE_MAP_QUERY => {
            let request = deserialize_request(ty[0], &payload)?;
            Ok(StreamFrame::Request { seq_id, request })
        }
        TYPE_PIECE_BATCH | TYPE_ACK | TYPE_PIECE_MAP_ENTRIES => {
            let response = deserialize_response(ty[0], &payload)?;
            Ok(StreamFrame::Response { seq_id, response })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown frame type: 0x{:02x}", ty[0]),
        )),
    }
}

// ========================================================================
// Internal helpers
// ========================================================================

/// Write a raw frame atomically: build in memory, single write_all + flush.
async fn write_raw_frame<T: AsyncWrite + Unpin>(
    io: &mut T,
    seq_id: u64,
    msg_type: u8,
    payload: &[u8],
) -> io::Result<()> {
    if payload.len() > MAX_FRAME_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Frame payload too large: {} > {}", payload.len(), MAX_FRAME_PAYLOAD),
        ));
    }

    // [seq_id:8][type:1][len:4][payload:N]
    let frame_len = 8 + 1 + 4 + payload.len();
    let mut buf = Vec::with_capacity(frame_len);
    buf.extend_from_slice(&seq_id.to_be_bytes());
    buf.push(msg_type);
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);

    io.write_all(&buf).await?;
    io.flush().await?;

    Ok(())
}

fn serialize_request(request: &CraftOBJRequest) -> io::Result<(u8, Vec<u8>)> {
    match request {
        CraftOBJRequest::PieceSync { content_id, segment_index, merkle_root, have_pieces, max_pieces } => {
            let inner = PieceSyncWire {
                content_id: *content_id,
                segment_index: *segment_index,
                merkle_root: *merkle_root,
                have_pieces: have_pieces.clone(),
                max_pieces: *max_pieces,
            };
            let payload = bincode::serialize(&inner)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok((TYPE_PIECE_SYNC, payload))
        }
        CraftOBJRequest::PiecePush { content_id, segment_index, piece_id, coefficients, data } => {
            let inner = PiecePushWire {
                content_id: *content_id,
                segment_index: *segment_index,
                piece_id: *piece_id,
                coefficients: coefficients.clone(),
                data: data.clone(),
            };
            let payload = bincode::serialize(&inner)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok((TYPE_PIECE_PUSH, payload))
        }
        CraftOBJRequest::ManifestPush { content_id, manifest_json } => {
            let inner = ManifestPushWire {
                content_id: *content_id,
                manifest_json: manifest_json.clone(),
            };
            let payload = bincode::serialize(&inner)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok((TYPE_MANIFEST_PUSH, payload))
        }
        CraftOBJRequest::PieceMapQuery { content_id, segment_index } => {
            let inner = PieceMapQueryWire {
                content_id: *content_id,
                segment_index: *segment_index,
            };
            let payload = bincode::serialize(&inner)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok((TYPE_PIECE_MAP_QUERY, payload))
        }
    }
}

fn deserialize_request(msg_type: u8, payload: &[u8]) -> io::Result<CraftOBJRequest> {
    match msg_type {
        TYPE_PIECE_SYNC => {
            let inner: PieceSyncWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftOBJRequest::PieceSync {
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
            Ok(CraftOBJRequest::PiecePush {
                content_id: inner.content_id,
                segment_index: inner.segment_index,
                piece_id: inner.piece_id,
                coefficients: inner.coefficients,
                data: inner.data,
            })
        }
        TYPE_MANIFEST_PUSH => {
            let inner: ManifestPushWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftOBJRequest::ManifestPush {
                content_id: inner.content_id,
                manifest_json: inner.manifest_json,
            })
        }
        TYPE_PIECE_MAP_QUERY => {
            let inner: PieceMapQueryWire = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftOBJRequest::PieceMapQuery {
                content_id: inner.content_id,
                segment_index: inner.segment_index,
            })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown request type: 0x{:02x}", msg_type),
        )),
    }
}

fn serialize_response(response: &CraftOBJResponse) -> io::Result<(u8, Vec<u8>)> {
    match response {
        CraftOBJResponse::PieceBatch { pieces } => {
            let payload = bincode::serialize(pieces)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok((TYPE_PIECE_BATCH, payload))
        }
        CraftOBJResponse::Ack { status } => {
            let payload = bincode::serialize(status)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok((TYPE_ACK, payload))
        }
        CraftOBJResponse::PieceMapEntries { entries } => {
            let payload = bincode::serialize(entries)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok((TYPE_PIECE_MAP_ENTRIES, payload))
        }
    }
}

fn deserialize_response(msg_type: u8, payload: &[u8]) -> io::Result<CraftOBJResponse> {
    match msg_type {
        TYPE_PIECE_BATCH => {
            let pieces: Vec<PiecePayload> = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftOBJResponse::PieceBatch { pieces })
        }
        TYPE_ACK => {
            let status: WireStatus = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftOBJResponse::Ack { status })
        }
        TYPE_PIECE_MAP_ENTRIES => {
            let entries: Vec<PieceMapEntry> = bincode::deserialize(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            Ok(CraftOBJResponse::PieceMapEntries { entries })
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
struct ManifestPushWire {
    content_id: ContentId,
    manifest_json: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct PieceMapQueryWire {
    content_id: ContentId,
    segment_index: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_request_frame_roundtrip() {
        let req = CraftOBJRequest::PieceSync {
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
                    CraftOBJRequest::PieceSync { segment_index, max_pieces, .. } => {
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
        let resp = CraftOBJResponse::Ack { status: WireStatus::Ok };

        let mut buf = Vec::new();
        write_response_frame(&mut futures::io::Cursor::new(&mut buf), 99, &resp)
            .await
            .unwrap();

        let frame = read_frame(&mut futures::io::Cursor::new(&buf)).await.unwrap();
        match frame {
            StreamFrame::Response { seq_id, response } => {
                assert_eq!(seq_id, 99);
                match response {
                    CraftOBJResponse::Ack { status } => {
                        assert_eq!(status as u8, WireStatus::Ok as u8);
                    }
                    _ => panic!("wrong variant"),
                }
            }
            _ => panic!("expected response frame"),
        }
    }
}
