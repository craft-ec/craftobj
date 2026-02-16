//! DataCraft Transfer
//!
//! Piece exchange protocol for DataCraft using RLNC coding.
//!
//! Protocol: `/datacraft/transfer/2.0.0`
//!
//! Wire format:
//! ```text
//! PieceRequest:  [magic:4][type:1][content_id:32][segment_index:4][piece_id:32] = 73 bytes
//! PieceResponse: [status:1][coeff_len:4][coefficients][data_len:4][data]
//! PiecePush:     [magic:4][type:1][content_id:32][segment_index:4][piece_id:32][coeff_len:4][coeff][data_len:4][data]
//! ```

use datacraft_core::{
    ContentId, DataCraftError, Result, WireMessageType, WireStatus, WIRE_MAGIC,
};
use datacraft_store::FsStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};

/// Size of a piece request header.
pub const PIECE_REQUEST_SIZE: usize = 73; // 4 + 1 + 32 + 4 + 32

/// Size of a piece response header (status + coeff_len).
pub const PIECE_RESPONSE_HEADER_SIZE: usize = 5; // 1 + 4

/// Encode a piece request into wire format.
///
/// `piece_id` of all zeros means "any random piece for this segment".
pub fn encode_piece_request(
    content_id: &ContentId,
    segment_index: u32,
    piece_id: &[u8; 32],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PIECE_REQUEST_SIZE);
    buf.extend_from_slice(&WIRE_MAGIC);
    buf.push(WireMessageType::PieceRequest as u8);
    buf.extend_from_slice(&content_id.0);
    buf.extend_from_slice(&segment_index.to_be_bytes());
    buf.extend_from_slice(piece_id);
    buf
}

/// Decode a piece request from wire format.
///
/// Returns (content_id, segment_index, piece_id).
/// piece_id of all zeros means "any piece".
pub fn decode_piece_request(buf: &[u8]) -> Result<(ContentId, u32, [u8; 32])> {
    if buf.len() < PIECE_REQUEST_SIZE {
        return Err(DataCraftError::TransferError("request too short".into()));
    }
    if buf[0..4] != WIRE_MAGIC {
        return Err(DataCraftError::TransferError("invalid magic".into()));
    }
    if buf[4] != WireMessageType::PieceRequest as u8 {
        return Err(DataCraftError::TransferError(format!(
            "expected PieceRequest, got {}",
            buf[4]
        )));
    }
    let mut cid_bytes = [0u8; 32];
    cid_bytes.copy_from_slice(&buf[5..37]);
    let segment_index = u32::from_be_bytes([buf[37], buf[38], buf[39], buf[40]]);
    let mut piece_id = [0u8; 32];
    piece_id.copy_from_slice(&buf[41..73]);
    Ok((ContentId(cid_bytes), segment_index, piece_id))
}

/// Encode a piece response with status, coefficient vector, and piece data.
pub fn encode_piece_response(
    status: WireStatus,
    coefficients: &[u8],
    data: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 4 + coefficients.len() + 4 + data.len());
    buf.push(status as u8);
    buf.extend_from_slice(&(coefficients.len() as u32).to_be_bytes());
    buf.extend_from_slice(coefficients);
    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
    buf.extend_from_slice(data);
    buf
}

/// Encode an error/not-found piece response (no data).
pub fn encode_piece_response_error(status: WireStatus) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(status as u8);
    buf.extend_from_slice(&0u32.to_be_bytes()); // coeff_len = 0
    buf.extend_from_slice(&0u32.to_be_bytes()); // data_len = 0
    buf
}

/// Encode a piece push message.
///
/// Wire format: `[magic:4][type:1(PiecePush)][content_id:32][segment_index:4][piece_id:32][coeff_len:4][coeff][data_len:4][data]`
pub fn encode_piece_push(
    content_id: &ContentId,
    segment_index: u32,
    piece_id: &[u8; 32],
    coefficients: &[u8],
    data: &[u8],
) -> Vec<u8> {
    let header_size = 4 + 1 + 32 + 4 + 32; // 73
    let mut buf = Vec::with_capacity(header_size + 4 + coefficients.len() + 4 + data.len());
    buf.extend_from_slice(&WIRE_MAGIC);
    buf.push(WireMessageType::PiecePush as u8);
    buf.extend_from_slice(&content_id.0);
    buf.extend_from_slice(&segment_index.to_be_bytes());
    buf.extend_from_slice(piece_id);
    buf.extend_from_slice(&(coefficients.len() as u32).to_be_bytes());
    buf.extend_from_slice(coefficients);
    buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
    buf.extend_from_slice(data);
    buf
}

/// Decode a piece push header (first 73 bytes) plus the variable-length payload.
///
/// Returns (content_id, segment_index, piece_id, coefficients, data).
#[allow(clippy::type_complexity)]
pub fn decode_piece_push(buf: &[u8]) -> Result<(ContentId, u32, [u8; 32], Vec<u8>, Vec<u8>)> {
    if buf.len() < 73 + 8 {
        return Err(DataCraftError::TransferError("push too short".into()));
    }
    if buf[0..4] != WIRE_MAGIC {
        return Err(DataCraftError::TransferError("invalid magic".into()));
    }
    if buf[4] != WireMessageType::PiecePush as u8 {
        return Err(DataCraftError::TransferError(format!(
            "expected PiecePush, got {}",
            buf[4]
        )));
    }
    let mut cid_bytes = [0u8; 32];
    cid_bytes.copy_from_slice(&buf[5..37]);
    let segment_index = u32::from_be_bytes([buf[37], buf[38], buf[39], buf[40]]);
    let mut piece_id = [0u8; 32];
    piece_id.copy_from_slice(&buf[41..73]);

    let coeff_len = u32::from_be_bytes([buf[73], buf[74], buf[75], buf[76]]) as usize;
    let coeff_end = 77 + coeff_len;
    if buf.len() < coeff_end + 4 {
        return Err(DataCraftError::TransferError("push coefficients truncated".into()));
    }
    let coefficients = buf[77..coeff_end].to_vec();

    let data_len = u32::from_be_bytes([
        buf[coeff_end],
        buf[coeff_end + 1],
        buf[coeff_end + 2],
        buf[coeff_end + 3],
    ]) as usize;
    let data_start = coeff_end + 4;
    if buf.len() < data_start + data_len {
        return Err(DataCraftError::TransferError("push data truncated".into()));
    }
    let data = buf[data_start..data_start + data_len].to_vec();

    Ok((ContentId(cid_bytes), segment_index, piece_id, coefficients, data))
}

/// Manifest push header size: magic(4) + type(1) + content_id(32) + payload_len(4) = 41
pub const MANIFEST_PUSH_HEADER_SIZE: usize = 41;

/// Encode a manifest push message.
/// Wire format: `[magic:4][type:1(ManifestPush=6)][content_id:32][payload_len:4][manifest_json]`
pub fn encode_manifest_push(content_id: &ContentId, manifest_json: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(MANIFEST_PUSH_HEADER_SIZE + manifest_json.len());
    buf.extend_from_slice(&WIRE_MAGIC);
    buf.push(WireMessageType::ManifestPush as u8);
    buf.extend_from_slice(&content_id.0);
    buf.extend_from_slice(&(manifest_json.len() as u32).to_be_bytes());
    buf.extend_from_slice(manifest_json);
    buf
}

/// Decode a manifest push header.
/// Returns (content_id, payload_len).
pub fn decode_manifest_push_header(buf: &[u8]) -> Result<(ContentId, u32)> {
    if buf.len() < MANIFEST_PUSH_HEADER_SIZE {
        return Err(DataCraftError::TransferError(
            "manifest push header too short".into(),
        ));
    }
    if buf[0..4] != WIRE_MAGIC {
        return Err(DataCraftError::TransferError("invalid magic".into()));
    }
    if buf[4] != WireMessageType::ManifestPush as u8 {
        return Err(DataCraftError::TransferError(format!(
            "expected ManifestPush type (6), got {}",
            buf[4]
        )));
    }
    let mut cid_bytes = [0u8; 32];
    cid_bytes.copy_from_slice(&buf[5..37]);
    let payload_len = u32::from_be_bytes([buf[37], buf[38], buf[39], buf[40]]);
    Ok((ContentId(cid_bytes), payload_len))
}

/// Handle an incoming piece request: read from local store and respond.
pub async fn handle_piece_request<S>(
    stream: &mut S,
    store: &FsStore,
    content_id: &ContentId,
    segment_index: u32,
    piece_id: &[u8; 32],
) -> Result<()>
where
    S: AsyncWriteExt + Unpin,
{
    let is_any = *piece_id == [0u8; 32];

    let result = if is_any {
        store.get_random_piece(content_id, segment_index)
    } else {
        store
            .get_piece(content_id, segment_index, piece_id)
            .map(|(data, coeff)| Some((*piece_id, data, coeff)))
    };

    match result {
        Ok(Some((_pid, data, coefficients))) => {
            debug!(
                "Serving piece {}/{} ({} bytes data, {} bytes coeff)",
                content_id,
                segment_index,
                data.len(),
                coefficients.len()
            );
            let response = encode_piece_response(WireStatus::Ok, &coefficients, &data);
            stream.write_all(&response).await.map_err(|e| {
                DataCraftError::TransferError(format!("write response: {}", e))
            })?;
        }
        Ok(None) | Err(_) => {
            warn!("Piece not found: {}/{}", content_id, segment_index);
            let response = encode_piece_response_error(WireStatus::NotFound);
            stream.write_all(&response).await.map_err(|e| {
                DataCraftError::TransferError(format!("write not-found: {}", e))
            })?;
        }
    }
    Ok(())
}

/// Request a piece from a remote peer over an async stream.
///
/// Returns (coefficients, data) on success.
pub async fn request_piece<S>(
    stream: &mut S,
    content_id: &ContentId,
    segment_index: u32,
    piece_id: &[u8; 32],
) -> Result<(Vec<u8>, Vec<u8>)>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    let request = encode_piece_request(content_id, segment_index, piece_id);
    stream.write_all(&request).await.map_err(|e| {
        DataCraftError::TransferError(format!("write request: {}", e))
    })?;

    // Read status
    let mut status_byte = [0u8; 1];
    stream.read_exact(&mut status_byte).await.map_err(|e| {
        DataCraftError::TransferError(format!("read status: {}", e))
    })?;
    let status = WireStatus::from_u8(status_byte[0]).ok_or_else(|| {
        DataCraftError::TransferError(format!("invalid status: {}", status_byte[0]))
    })?;

    // Read coeff_len
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(|e| {
        DataCraftError::TransferError(format!("read coeff_len: {}", e))
    })?;
    let coeff_len = u32::from_be_bytes(len_buf) as usize;

    match status {
        WireStatus::Ok => {
            let mut coefficients = vec![0u8; coeff_len];
            stream.read_exact(&mut coefficients).await.map_err(|e| {
                DataCraftError::TransferError(format!("read coefficients: {}", e))
            })?;

            stream.read_exact(&mut len_buf).await.map_err(|e| {
                DataCraftError::TransferError(format!("read data_len: {}", e))
            })?;
            let data_len = u32::from_be_bytes(len_buf) as usize;

            let mut data = vec![0u8; data_len];
            stream.read_exact(&mut data).await.map_err(|e| {
                DataCraftError::TransferError(format!("read data: {}", e))
            })?;

            Ok((coefficients, data))
        }
        WireStatus::NotFound => Err(DataCraftError::ContentNotFound(format!(
            "piece {}/{}",
            content_id, segment_index
        ))),
        WireStatus::Error => Err(DataCraftError::TransferError("remote error".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_piece_request() {
        let cid = ContentId::from_bytes(b"test");
        let piece_id = [0xABu8; 32];
        let encoded = encode_piece_request(&cid, 5, &piece_id);
        assert_eq!(encoded.len(), PIECE_REQUEST_SIZE);
        assert_eq!(&encoded[0..4], &WIRE_MAGIC);

        let (decoded_cid, seg, decoded_pid) = decode_piece_request(&encoded).unwrap();
        assert_eq!(decoded_cid, cid);
        assert_eq!(seg, 5);
        assert_eq!(decoded_pid, piece_id);
    }

    #[test]
    fn test_encode_decode_piece_request_any() {
        let cid = ContentId::from_bytes(b"any piece");
        let zeroed = [0u8; 32];
        let encoded = encode_piece_request(&cid, 0, &zeroed);
        let (_, _, pid) = decode_piece_request(&encoded).unwrap();
        assert_eq!(pid, zeroed);
    }

    #[test]
    fn test_decode_invalid_magic() {
        let mut buf = vec![0u8; PIECE_REQUEST_SIZE];
        buf[0..4].copy_from_slice(b"XXXX");
        assert!(decode_piece_request(&buf).is_err());
    }

    #[test]
    fn test_decode_too_short() {
        assert!(decode_piece_request(&[0u8; 10]).is_err());
    }

    #[test]
    fn test_encode_piece_response() {
        let coefficients = vec![1, 0, 0, 0, 1];
        let data = b"piece data here";
        let response = encode_piece_response(WireStatus::Ok, &coefficients, data);
        assert_eq!(response[0], WireStatus::Ok as u8);
        let coeff_len =
            u32::from_be_bytes([response[1], response[2], response[3], response[4]]) as usize;
        assert_eq!(coeff_len, 5);
        assert_eq!(&response[5..10], &coefficients);
        let data_len =
            u32::from_be_bytes([response[10], response[11], response[12], response[13]]) as usize;
        assert_eq!(data_len, 15);
        assert_eq!(&response[14..], data);
    }

    #[test]
    fn test_encode_piece_response_error() {
        let response = encode_piece_response_error(WireStatus::NotFound);
        assert_eq!(response[0], WireStatus::NotFound as u8);
        let coeff_len =
            u32::from_be_bytes([response[1], response[2], response[3], response[4]]);
        assert_eq!(coeff_len, 0);
        let data_len =
            u32::from_be_bytes([response[5], response[6], response[7], response[8]]);
        assert_eq!(data_len, 0);
    }

    #[test]
    fn test_encode_decode_piece_push() {
        let cid = ContentId::from_bytes(b"push test");
        let piece_id = [0x42u8; 32];
        let coefficients = vec![1, 2, 3, 4, 5];
        let data = b"pushed piece data";

        let encoded = encode_piece_push(&cid, 3, &piece_id, &coefficients, data);
        let (dec_cid, seg, dec_pid, dec_coeff, dec_data) =
            decode_piece_push(&encoded).unwrap();
        assert_eq!(dec_cid, cid);
        assert_eq!(seg, 3);
        assert_eq!(dec_pid, piece_id);
        assert_eq!(dec_coeff, coefficients);
        assert_eq!(dec_data, data);
    }

    #[test]
    fn test_decode_piece_push_too_short() {
        assert!(decode_piece_push(&[0u8; 10]).is_err());
    }

    #[test]
    fn test_encode_decode_manifest_push() {
        let cid = ContentId::from_bytes(b"manifest");
        let json = b"{\"test\": true}";
        let encoded = encode_manifest_push(&cid, json);
        let (dec_cid, len) = decode_manifest_push_header(&encoded).unwrap();
        assert_eq!(dec_cid, cid);
        assert_eq!(len as usize, json.len());
        assert_eq!(
            &encoded[MANIFEST_PUSH_HEADER_SIZE..],
            json
        );
    }

    #[tokio::test]
    async fn test_request_response_roundtrip() {
        use datacraft_store::piece_id_from_coefficients;
        use tokio::io::duplex;

        let dir = std::env::temp_dir().join("datacraft-transfer-test-rlnc");
        std::fs::create_dir_all(&dir).ok();
        let store = FsStore::new(&dir).unwrap();

        let cid = ContentId::from_bytes(b"transfer test");
        let coefficients = vec![1u8, 0, 0, 0];
        let piece_id = piece_id_from_coefficients(&coefficients);
        store
            .store_piece(&cid, 0, &piece_id, b"piece data here", &coefficients)
            .unwrap();

        let (mut client, mut server) = duplex(4096);

        let server_store = FsStore::new(&dir).unwrap();
        let server_handle = tokio::spawn(async move {
            let mut header = [0u8; PIECE_REQUEST_SIZE];
            server.read_exact(&mut header).await.unwrap();
            let (req_cid, seg, req_pid) = decode_piece_request(&header).unwrap();
            handle_piece_request(&mut server, &server_store, &req_cid, seg, &req_pid)
                .await
                .unwrap();
        });

        let (coeff, data) = request_piece(&mut client, &cid, 0, &piece_id)
            .await
            .unwrap();
        assert_eq!(coeff, coefficients);
        assert_eq!(data, b"piece data here");

        server_handle.await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_request_any_piece() {
        use datacraft_store::piece_id_from_coefficients;
        use tokio::io::duplex;

        let dir = std::env::temp_dir().join("datacraft-transfer-test-any");
        std::fs::create_dir_all(&dir).ok();
        let store = FsStore::new(&dir).unwrap();

        let cid = ContentId::from_bytes(b"any piece test");
        let coefficients = vec![0u8, 1, 0];
        let piece_id = piece_id_from_coefficients(&coefficients);
        store
            .store_piece(&cid, 0, &piece_id, b"any piece data", &coefficients)
            .unwrap();

        let (mut client, mut server) = duplex(4096);
        let server_store = FsStore::new(&dir).unwrap();

        let server_handle = tokio::spawn(async move {
            let mut header = [0u8; PIECE_REQUEST_SIZE];
            server.read_exact(&mut header).await.unwrap();
            let (req_cid, seg, req_pid) = decode_piece_request(&header).unwrap();
            handle_piece_request(&mut server, &server_store, &req_cid, seg, &req_pid)
                .await
                .unwrap();
        });

        // Request with zeroed piece_id = "any piece"
        let zeroed = [0u8; 32];
        let (coeff, data) = request_piece(&mut client, &cid, 0, &zeroed)
            .await
            .unwrap();
        assert_eq!(coeff, coefficients);
        assert_eq!(data, b"any piece data");

        server_handle.await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }
}
