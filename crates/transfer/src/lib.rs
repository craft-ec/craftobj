//! DataCraft Transfer
//!
//! Piece exchange protocol for DataCraft using RLNC coding.
//!
//! Protocol: `/datacraft/transfer/3.0.0`
//!
//! Uses `libp2p::request_response` for transport — no manual stream management.
//!
//! Wire format: `[type:1][payload_len:4 BE][payload (bincode)]`
//!
//! Type discriminants:
//!   - 0x01 = PieceSync request
//!   - 0x02 = PiecePush request
//!   - 0x03 = ManifestPush request
//!   - 0x81 = PieceBatch response
//!   - 0x82 = Ack response

use async_trait::async_trait;
use datacraft_core::{ContentId, WireStatus};
use futures::prelude::*;
use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};
use std::io;

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

// ========================================================================
// Codec
// ========================================================================

// Type discriminants
const TYPE_PIECE_SYNC: u8 = 0x01;
const TYPE_PIECE_PUSH: u8 = 0x02;
const TYPE_MANIFEST_PUSH: u8 = 0x03;
const TYPE_PIECE_BATCH: u8 = 0x81;
const TYPE_ACK: u8 = 0x82;

/// Maximum total message size (50 MB — generous for PieceBatch with multiple 256 KiB pieces).
const MAX_MESSAGE_SIZE: usize = 50 * 1024 * 1024;

/// Codec for the DataCraft transfer request-response protocol.
#[derive(Debug, Clone, Default)]
pub struct DataCraftCodec;

#[async_trait]
impl libp2p::request_response::Codec for DataCraftCodec {
    type Protocol = StreamProtocol;
    type Request = DataCraftRequest;
    type Response = DataCraftResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut header = [0u8; 5];
        io.read_exact(&mut header).await?;
        let msg_type = header[0];
        let payload_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

        if payload_len > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "payload too large"));
        }

        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            io.read_exact(&mut payload).await?;
        }

        match msg_type {
            TYPE_PIECE_SYNC => {
                let inner: PieceSyncPayload = bincode::deserialize(&payload)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(DataCraftRequest::PieceSync {
                    content_id: inner.content_id,
                    segment_index: inner.segment_index,
                    merkle_root: inner.merkle_root,
                    have_pieces: inner.have_pieces,
                    max_pieces: inner.max_pieces,
                })
            }
            TYPE_PIECE_PUSH => {
                let inner: PiecePushPayload = bincode::deserialize(&payload)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(DataCraftRequest::PiecePush {
                    content_id: inner.content_id,
                    segment_index: inner.segment_index,
                    piece_id: inner.piece_id,
                    coefficients: inner.coefficients,
                    data: inner.data,
                })
            }
            TYPE_MANIFEST_PUSH => {
                let inner: ManifestPushPayload = bincode::deserialize(&payload)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(DataCraftRequest::ManifestPush {
                    content_id: inner.content_id,
                    manifest_json: inner.manifest_json,
                })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown request type: 0x{:02x}", msg_type),
            )),
        }
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut header = [0u8; 5];
        io.read_exact(&mut header).await?;
        let msg_type = header[0];
        let payload_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

        if payload_len > MAX_MESSAGE_SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "payload too large"));
        }

        let mut payload = vec![0u8; payload_len];
        if payload_len > 0 {
            io.read_exact(&mut payload).await?;
        }

        match msg_type {
            TYPE_PIECE_BATCH => {
                let pieces: Vec<PiecePayload> = bincode::deserialize(&payload)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(DataCraftResponse::PieceBatch { pieces })
            }
            TYPE_ACK => {
                let status: WireStatus = bincode::deserialize(&payload)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                Ok(DataCraftResponse::Ack { status })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown response type: 0x{:02x}", msg_type),
            )),
        }
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let (msg_type, payload) = match &req {
            DataCraftRequest::PieceSync { content_id, segment_index, merkle_root, have_pieces, max_pieces } => {
                let inner = PieceSyncPayload {
                    content_id: *content_id,
                    segment_index: *segment_index,
                    merkle_root: *merkle_root,
                    have_pieces: have_pieces.clone(),
                    max_pieces: *max_pieces,
                };
                (TYPE_PIECE_SYNC, bincode::serialize(&inner).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?)
            }
            DataCraftRequest::PiecePush { content_id, segment_index, piece_id, coefficients, data } => {
                let inner = PiecePushPayload {
                    content_id: *content_id,
                    segment_index: *segment_index,
                    piece_id: *piece_id,
                    coefficients: coefficients.clone(),
                    data: data.clone(),
                };
                (TYPE_PIECE_PUSH, bincode::serialize(&inner).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?)
            }
            DataCraftRequest::ManifestPush { content_id, manifest_json } => {
                let inner = ManifestPushPayload {
                    content_id: *content_id,
                    manifest_json: manifest_json.clone(),
                };
                (TYPE_MANIFEST_PUSH, bincode::serialize(&inner).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?)
            }
        };

        let mut buf = Vec::with_capacity(5 + payload.len());
        buf.push(msg_type);
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let (msg_type, payload) = match &res {
            DataCraftResponse::PieceBatch { pieces } => {
                (TYPE_PIECE_BATCH, bincode::serialize(pieces).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?)
            }
            DataCraftResponse::Ack { status } => {
                (TYPE_ACK, bincode::serialize(status).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?)
            }
        };

        let mut buf = Vec::with_capacity(5 + payload.len());
        buf.push(msg_type);
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        io.write_all(&buf).await?;
        io.close().await?;
        Ok(())
    }
}

// Internal serialization structs (to avoid serde on the enum directly for the codec)
#[derive(Serialize, Deserialize)]
struct PieceSyncPayload {
    content_id: ContentId,
    segment_index: u32,
    merkle_root: [u8; 32],
    have_pieces: Vec<[u8; 32]>,
    max_pieces: u16,
}

#[derive(Serialize, Deserialize)]
struct PiecePushPayload {
    content_id: ContentId,
    segment_index: u32,
    piece_id: [u8; 32],
    coefficients: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct ManifestPushPayload {
    content_id: ContentId,
    manifest_json: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::request_response::Codec;

    fn test_protocol() -> StreamProtocol {
        StreamProtocol::new("/datacraft/transfer/3.0.0")
    }

    #[tokio::test]
    async fn test_codec_piece_sync_roundtrip() {
        let mut codec = DataCraftCodec;
        let proto = test_protocol();

        let req = DataCraftRequest::PieceSync {
            content_id: ContentId::from_bytes(b"test"),
            segment_index: 3,
            merkle_root: [0xAB; 32],
            have_pieces: vec![[0x01; 32], [0x02; 32]],
            max_pieces: 10,
        };

        let mut buf = Vec::new();
        codec.write_request(&proto, &mut futures::io::Cursor::new(&mut buf), req.clone()).await.unwrap();

        let decoded = codec.read_request(&proto, &mut futures::io::Cursor::new(&buf)).await.unwrap();
        match decoded {
            DataCraftRequest::PieceSync { content_id, segment_index, merkle_root, have_pieces, max_pieces } => {
                assert_eq!(content_id, ContentId::from_bytes(b"test"));
                assert_eq!(segment_index, 3);
                assert_eq!(merkle_root, [0xAB; 32]);
                assert_eq!(have_pieces.len(), 2);
                assert_eq!(max_pieces, 10);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[tokio::test]
    async fn test_codec_piece_push_roundtrip() {
        let mut codec = DataCraftCodec;
        let proto = test_protocol();

        let req = DataCraftRequest::PiecePush {
            content_id: ContentId::from_bytes(b"push"),
            segment_index: 1,
            piece_id: [0x42; 32],
            coefficients: vec![1, 2, 3, 4],
            data: vec![0xAA; 100],
        };

        let mut buf = Vec::new();
        codec.write_request(&proto, &mut futures::io::Cursor::new(&mut buf), req).await.unwrap();

        let decoded = codec.read_request(&proto, &mut futures::io::Cursor::new(&buf)).await.unwrap();
        match decoded {
            DataCraftRequest::PiecePush { content_id, segment_index, piece_id, coefficients, data } => {
                assert_eq!(content_id, ContentId::from_bytes(b"push"));
                assert_eq!(segment_index, 1);
                assert_eq!(piece_id, [0x42; 32]);
                assert_eq!(coefficients, vec![1, 2, 3, 4]);
                assert_eq!(data.len(), 100);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[tokio::test]
    async fn test_codec_manifest_push_roundtrip() {
        let mut codec = DataCraftCodec;
        let proto = test_protocol();

        let req = DataCraftRequest::ManifestPush {
            content_id: ContentId::from_bytes(b"manifest"),
            manifest_json: b"{\"test\": true}".to_vec(),
        };

        let mut buf = Vec::new();
        codec.write_request(&proto, &mut futures::io::Cursor::new(&mut buf), req).await.unwrap();

        let decoded = codec.read_request(&proto, &mut futures::io::Cursor::new(&buf)).await.unwrap();
        match decoded {
            DataCraftRequest::ManifestPush { content_id, manifest_json } => {
                assert_eq!(content_id, ContentId::from_bytes(b"manifest"));
                assert_eq!(manifest_json, b"{\"test\": true}");
            }
            _ => panic!("wrong variant"),
        }
    }

    #[tokio::test]
    async fn test_codec_piece_batch_roundtrip() {
        let mut codec = DataCraftCodec;
        let proto = test_protocol();

        let res = DataCraftResponse::PieceBatch {
            pieces: vec![
                PiecePayload {
                    segment_index: 0,
                    piece_id: [0x11; 32],
                    coefficients: vec![1, 0, 0],
                    data: vec![0xBB; 50],
                },
            ],
        };

        let mut buf = Vec::new();
        codec.write_response(&proto, &mut futures::io::Cursor::new(&mut buf), res).await.unwrap();

        let decoded = codec.read_response(&proto, &mut futures::io::Cursor::new(&buf)).await.unwrap();
        match decoded {
            DataCraftResponse::PieceBatch { pieces } => {
                assert_eq!(pieces.len(), 1);
                assert_eq!(pieces[0].segment_index, 0);
                assert_eq!(pieces[0].piece_id, [0x11; 32]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[tokio::test]
    async fn test_codec_ack_roundtrip() {
        let mut codec = DataCraftCodec;
        let proto = test_protocol();

        let res = DataCraftResponse::Ack { status: WireStatus::Ok };

        let mut buf = Vec::new();
        codec.write_response(&proto, &mut futures::io::Cursor::new(&mut buf), res).await.unwrap();

        let decoded = codec.read_response(&proto, &mut futures::io::Cursor::new(&buf)).await.unwrap();
        match decoded {
            DataCraftResponse::Ack { status } => {
                assert_eq!(status as u8, WireStatus::Ok as u8);
            }
            _ => panic!("wrong variant"),
        }
    }
}
