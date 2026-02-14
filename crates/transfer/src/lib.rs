//! DataCraft Transfer
//!
//! Piece exchange protocol for DataCraft.
//!
//! Protocol: `/datacraft/transfer/1.0.0`
//!
//! Wire format:
//! ```text
//! Request:  [magic:4][type:1][content_id:32][chunk_index:4][shard_index:1] = 42 bytes
//! Response: [status:1][len:4][payload]
//! ```

use datacraft_core::{
    ContentId, DataCraftError, Result, TransferReceipt, WireMessageType, WireStatus, WIRE_MAGIC,
};
use datacraft_store::FsStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};

/// Size of a wire request header.
pub const REQUEST_HEADER_SIZE: usize = 42; // 4 + 1 + 32 + 4 + 1

/// Size of a wire response header.
pub const RESPONSE_HEADER_SIZE: usize = 5; // 1 + 4

/// Encode a shard request into wire format.
pub fn encode_shard_request(
    content_id: &ContentId,
    chunk_index: u32,
    shard_index: u8,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(REQUEST_HEADER_SIZE);
    buf.extend_from_slice(&WIRE_MAGIC);
    buf.push(WireMessageType::ShardRequest as u8);
    buf.extend_from_slice(&content_id.0);
    buf.extend_from_slice(&chunk_index.to_be_bytes());
    buf.push(shard_index);
    buf
}

/// Decode a shard request from wire format.
pub fn decode_shard_request(buf: &[u8]) -> Result<(ContentId, u32, u8)> {
    if buf.len() < REQUEST_HEADER_SIZE {
        return Err(DataCraftError::TransferError("request too short".into()));
    }
    if buf[0..4] != WIRE_MAGIC {
        return Err(DataCraftError::TransferError("invalid magic".into()));
    }
    let msg_type = buf[4];
    if msg_type != WireMessageType::ShardRequest as u8 {
        return Err(DataCraftError::TransferError(format!(
            "expected ShardRequest, got {}",
            msg_type
        )));
    }
    let mut cid_bytes = [0u8; 32];
    cid_bytes.copy_from_slice(&buf[5..37]);
    let chunk_index = u32::from_be_bytes([buf[37], buf[38], buf[39], buf[40]]);
    let shard_index = buf[41];
    Ok((ContentId(cid_bytes), chunk_index, shard_index))
}

/// Encode a response with status and payload.
pub fn encode_response(status: WireStatus, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(RESPONSE_HEADER_SIZE + payload.len());
    buf.push(status as u8);
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Encode a TransferReceipt into wire format.
///
/// Wire format: [magic:4][type:1(Receipt=4)][len:4 BE][bincode receipt]
pub fn encode_receipt(receipt: &TransferReceipt) -> Result<Vec<u8>> {
    let payload = bincode::serialize(receipt).map_err(|e| {
        DataCraftError::TransferError(format!("serialize receipt: {}", e))
    })?;
    let mut buf = Vec::with_capacity(4 + 1 + 4 + payload.len());
    buf.extend_from_slice(&WIRE_MAGIC);
    buf.push(WireMessageType::Receipt as u8);
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// Decode a TransferReceipt from wire format.
///
/// Expects: [magic:4][type:1(Receipt=4)][len:4 BE][bincode receipt]
pub fn decode_receipt(buf: &[u8]) -> Result<TransferReceipt> {
    if buf.len() < 9 {
        return Err(DataCraftError::TransferError("receipt frame too short".into()));
    }
    if buf[0..4] != WIRE_MAGIC {
        return Err(DataCraftError::TransferError("invalid magic in receipt".into()));
    }
    if buf[4] != WireMessageType::Receipt as u8 {
        return Err(DataCraftError::TransferError(format!(
            "expected Receipt type, got {}",
            buf[4]
        )));
    }
    let payload_len = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
    if buf.len() < 9 + payload_len {
        return Err(DataCraftError::TransferError("receipt payload truncated".into()));
    }
    let receipt: TransferReceipt = bincode::deserialize(&buf[9..9 + payload_len]).map_err(|e| {
        DataCraftError::TransferError(format!("deserialize receipt: {}", e))
    })?;
    Ok(receipt)
}

/// Read a TransferReceipt from an async stream.
pub async fn read_receipt<S>(stream: &mut S) -> Result<TransferReceipt>
where
    S: AsyncReadExt + Unpin,
{
    // Read header: magic(4) + type(1) + len(4) = 9 bytes
    let mut header = [0u8; 9];
    stream.read_exact(&mut header).await.map_err(|e| {
        DataCraftError::TransferError(format!("read receipt header: {}", e))
    })?;
    if header[0..4] != WIRE_MAGIC {
        return Err(DataCraftError::TransferError("invalid magic in receipt".into()));
    }
    if header[4] != WireMessageType::Receipt as u8 {
        return Err(DataCraftError::TransferError(format!(
            "expected Receipt type, got {}",
            header[4]
        )));
    }
    let payload_len = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).await.map_err(|e| {
        DataCraftError::TransferError(format!("read receipt payload: {}", e))
    })?;
    let receipt: TransferReceipt = bincode::deserialize(&payload).map_err(|e| {
        DataCraftError::TransferError(format!("deserialize receipt: {}", e))
    })?;
    Ok(receipt)
}

/// Write a TransferReceipt to an async stream.
pub async fn write_receipt<S>(stream: &mut S, receipt: &TransferReceipt) -> Result<()>
where
    S: AsyncWriteExt + Unpin,
{
    let encoded = encode_receipt(receipt)?;
    stream.write_all(&encoded).await.map_err(|e| {
        DataCraftError::TransferError(format!("write receipt: {}", e))
    })?;
    Ok(())
}

/// Handle an incoming shard request: read from local store and respond.
pub async fn handle_shard_request<S>(
    stream: &mut S,
    store: &FsStore,
    content_id: &ContentId,
    chunk_index: u32,
    shard_index: u8,
) -> Result<()>
where
    S: AsyncWriteExt + Unpin,
{
    match store.get_shard(content_id, chunk_index, shard_index) {
        Ok(data) => {
            debug!(
                "Serving shard {}/{}/{} ({} bytes)",
                content_id,
                chunk_index,
                shard_index,
                data.len()
            );
            let response = encode_response(WireStatus::Ok, &data);
            stream.write_all(&response).await.map_err(|e| {
                DataCraftError::TransferError(format!("write response: {}", e))
            })?;
        }
        Err(_) => {
            warn!(
                "Shard not found: {}/{}/{}",
                content_id, chunk_index, shard_index
            );
            let response = encode_response(WireStatus::NotFound, b"");
            stream.write_all(&response).await.map_err(|e| {
                DataCraftError::TransferError(format!("write not-found: {}", e))
            })?;
        }
    }
    Ok(())
}

/// Request a shard from a remote peer over an async stream.
pub async fn request_shard<S>(
    stream: &mut S,
    content_id: &ContentId,
    chunk_index: u32,
    shard_index: u8,
) -> Result<Vec<u8>>
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // Send request
    let request = encode_shard_request(content_id, chunk_index, shard_index);
    stream.write_all(&request).await.map_err(|e| {
        DataCraftError::TransferError(format!("write request: {}", e))
    })?;

    // Read response header
    let mut header = [0u8; RESPONSE_HEADER_SIZE];
    stream.read_exact(&mut header).await.map_err(|e| {
        DataCraftError::TransferError(format!("read response header: {}", e))
    })?;

    let status = WireStatus::from_u8(header[0]).ok_or_else(|| {
        DataCraftError::TransferError(format!("invalid status: {}", header[0]))
    })?;
    let payload_len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;

    match status {
        WireStatus::Ok => {
            let mut payload = vec![0u8; payload_len];
            stream.read_exact(&mut payload).await.map_err(|e| {
                DataCraftError::TransferError(format!("read payload: {}", e))
            })?;
            Ok(payload)
        }
        WireStatus::NotFound => Err(DataCraftError::ContentNotFound(format!(
            "shard {}/{}/{}",
            content_id, chunk_index, shard_index
        ))),
        WireStatus::Error => {
            let mut msg = vec![0u8; payload_len];
            stream.read_exact(&mut msg).await.ok();
            Err(DataCraftError::TransferError(
                String::from_utf8_lossy(&msg).to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_shard_request() {
        let cid = ContentId::from_bytes(b"test");
        let encoded = encode_shard_request(&cid, 5, 2);
        assert_eq!(encoded.len(), REQUEST_HEADER_SIZE);
        assert_eq!(&encoded[0..4], &WIRE_MAGIC);

        let (decoded_cid, chunk, shard) = decode_shard_request(&encoded).unwrap();
        assert_eq!(decoded_cid, cid);
        assert_eq!(chunk, 5);
        assert_eq!(shard, 2);
    }

    #[test]
    fn test_decode_invalid_magic() {
        let mut buf = vec![0u8; REQUEST_HEADER_SIZE];
        buf[0..4].copy_from_slice(b"XXXX");
        assert!(decode_shard_request(&buf).is_err());
    }

    #[test]
    fn test_decode_too_short() {
        assert!(decode_shard_request(&[0u8; 10]).is_err());
    }

    #[test]
    fn test_encode_response() {
        let response = encode_response(WireStatus::Ok, b"hello");
        assert_eq!(response[0], WireStatus::Ok as u8);
        let len = u32::from_be_bytes([response[1], response[2], response[3], response[4]]);
        assert_eq!(len, 5);
        assert_eq!(&response[5..], b"hello");
    }

    #[test]
    fn test_encode_response_empty() {
        let response = encode_response(WireStatus::NotFound, b"");
        assert_eq!(response[0], WireStatus::NotFound as u8);
        let len = u32::from_be_bytes([response[1], response[2], response[3], response[4]]);
        assert_eq!(len, 0);
    }

    #[tokio::test]
    async fn test_request_response_roundtrip() {
        use tokio::io::duplex;

        let dir = std::env::temp_dir().join("datacraft-transfer-test");
        std::fs::create_dir_all(&dir).ok();
        let store = FsStore::new(&dir).unwrap();

        let cid = ContentId::from_bytes(b"transfer test");
        store.put_shard(&cid, 0, 0, b"shard data here").unwrap();

        let (mut client, mut server) = duplex(4096);

        // Spawn server handler
        let server_store = FsStore::new(&dir).unwrap();
        let cid_clone = cid;
        let server_handle = tokio::spawn(async move {
            let mut header = [0u8; REQUEST_HEADER_SIZE];
            server.read_exact(&mut header).await.unwrap();
            let (req_cid, chunk, shard) = decode_shard_request(&header).unwrap();
            handle_shard_request(&mut server, &server_store, &req_cid, chunk, shard)
                .await
                .unwrap();
        });

        // Client request
        let data = request_shard(&mut client, &cid_clone, 0, 0).await.unwrap();
        assert_eq!(data, b"shard data here");

        server_handle.await.unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }
}
