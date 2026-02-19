//! Unified piece transfer — one send/receive path for all piece movement.
//!
//! Both distribution (publisher → storage) and fetch (storage → fetcher) use
//! the same batched protocol:
//!
//!   Sender: PieceBatchPush { content_id, pieces[..BATCH_SIZE] }
//!   Receiver: BatchAck { confirmed, failed }
//!   (repeat until all pieces sent, then sender closes stream)
//!
//! `send_pieces()` — batches and sends pieces on a stream, waits for ack per batch.
//! `receive_pieces()` — reads batches from a stream, stores pieces, sends acks.
//!
//! Both use PieceBatchPush as REQUEST frames so the existing wire codec works.

use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn, error, debug};

use craftobj_core::ContentId;
use craftobj_store::FsStore;
use craftobj_transfer::{CraftObjRequest, CraftObjResponse, PiecePayload};
use craftobj_transfer::wire::{write_request_frame, write_response_frame, read_frame, StreamFrame};

/// Max pieces per batch frame (~1.5MB at 100KB pieces).
pub const BATCH_SIZE: usize = 15;

/// Result of a send_pieces call.
#[derive(Debug)]
pub struct TransferResult {
    pub total_sent: usize,
    pub total_confirmed: usize,
    pub total_failed: usize,
}

/// Send pieces in batches over a stream. Each batch: write PieceBatchPush, read BatchAck.
/// Returns after all pieces sent or on error.
pub async fn send_pieces<S>(
    stream: &mut S,
    content_id: &ContentId,
    pieces: Vec<PiecePayload>,
    label: &str,
) -> Result<TransferResult, String>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let total = pieces.len();
    let mut confirmed = 0usize;
    let mut failed = 0usize;
    let mut seq_id = 0u64;

    for (batch_idx, chunk) in pieces.chunks(BATCH_SIZE).enumerate() {
        let batch_start = std::time::Instant::now();
        let batch_len = chunk.len();

        let request = CraftObjRequest::PieceBatchPush {
            content_id: content_id.clone(),
            pieces: chunk.to_vec(),
        };

        // Write batch
        write_request_frame(stream, seq_id, &request)
            .await
            .map_err(|e| format!("[{}] batch#{} write error: {}", label, batch_idx, e))?;

        // Read ack
        match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            read_frame(stream),
        ).await {
            Ok(Ok(StreamFrame::Response { response: CraftObjResponse::BatchAck { confirmed_pieces, failed_pieces }, .. })) => {
                let ms = batch_start.elapsed().as_secs_f64() * 1000.0;
                confirmed += confirmed_pieces.len();
                failed += failed_pieces.len();
                if batch_idx % 5 == 0 || batch_idx == (total / BATCH_SIZE) {
                    info!("[{}] batch#{}: {}/{} confirmed in {:.1}ms (total: {}/{})", 
                        label, batch_idx, confirmed_pieces.len(), batch_len, ms, confirmed, total);
                }
            }
            Ok(Ok(_)) => return Err(format!("[{}] batch#{}: unexpected response type", label, batch_idx)),
            Ok(Err(e)) => return Err(format!("[{}] batch#{}: read error: {}", label, batch_idx, e)),
            Err(_) => return Err(format!("[{}] batch#{}: ack timeout (30s)", label, batch_idx)),
        }

        seq_id += 1;
    }

    info!("[{}] complete: {}/{} confirmed, {} failed", label, confirmed, total, failed);
    Ok(TransferResult { total_sent: total, total_confirmed: confirmed, total_failed: failed })
}

/// Receive pieces from a stream. Reads PieceBatchPush frames, stores pieces,
/// sends BatchAck. Returns when stream closes (sender done) or on error.
pub async fn receive_pieces<S>(
    stream: &mut S,
    store: &Arc<Mutex<FsStore>>,
    label: &str,
) -> Result<TransferResult, String>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let mut total_confirmed = 0usize;
    let mut total_failed = 0usize;

    loop {
        match tokio::time::timeout(
            std::time::Duration::from_secs(60),
            read_frame(stream),
        ).await {
            Ok(Ok(StreamFrame::Request { seq_id, request: CraftObjRequest::PieceBatchPush { content_id, pieces } })) => {
                let batch_start = std::time::Instant::now();
                let batch_len = pieces.len();
                let mut confirmed = Vec::new();
                let mut failed = Vec::new();

                let store_guard = store.lock().await;
                for piece in &pieces {
                    match store_guard.store_piece(
                        &content_id,
                        piece.segment_index,
                        &piece.piece_id,
                        &piece.data,
                        &piece.coefficients,
                    ) {
                        Ok(()) => confirmed.push(piece.piece_id),
                        Err(e) => {
                            warn!("[{}] store error: {}", label, e);
                            failed.push(piece.piece_id);
                        }
                    }
                }
                drop(store_guard);

                total_confirmed += confirmed.len();
                total_failed += failed.len();

                let response = CraftObjResponse::BatchAck {
                    confirmed_pieces: confirmed,
                    failed_pieces: failed,
                };

                write_response_frame(stream, seq_id, &response)
                    .await
                    .map_err(|e| format!("[{}] ack write error: {}", label, e))?;

                let ms = batch_start.elapsed().as_secs_f64() * 1000.0;
                debug!("[{}] batch: {}/{} stored in {:.1}ms (total: {})", label, batch_len, batch_len, ms, total_confirmed);
            }
            Ok(Ok(_)) => {
                // Non-PieceBatchPush frame — end of transfer
                break;
            }
            Ok(Err(e)) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    // Stream closed — normal end of transfer
                    info!("[{}] stream closed, received {} pieces", label, total_confirmed);
                } else {
                    warn!("[{}] read error: {}", label, e);
                }
                break;
            }
            Err(_) => {
                // Timeout — no more batches coming
                info!("[{}] timeout, received {} pieces", label, total_confirmed);
                break;
            }
        }
    }

    Ok(TransferResult { total_sent: 0, total_confirmed, total_failed })
}

/// Read pieces from store for a given content/segment, excluding `have_pieces`.
/// Returns up to `max_pieces` as PiecePayload vec.
pub async fn read_pieces_from_store(
    store: &Arc<Mutex<FsStore>>,
    content_id: &ContentId,
    segment_index: u32,
    have_pieces: &HashSet<[u8; 32]>,
    max_pieces: usize,
) -> Vec<PiecePayload> {
    let store_guard = store.lock().await;
    let piece_ids = store_guard.list_pieces(content_id, segment_index).unwrap_or_default();
    let mut pieces = Vec::new();

    for pid in piece_ids {
        if have_pieces.contains(&pid) { continue; }
        if pieces.len() >= max_pieces { break; }
        match store_guard.get_piece(content_id, segment_index, &pid) {
            Ok((data, coefficients)) => {
                pieces.push(PiecePayload {
                    segment_index,
                    piece_id: pid,
                    coefficients,
                    data,
                });
            }
            Err(e) => debug!("Failed to read piece {}: {}", hex::encode(&pid[..4]), e),
        }
    }

    pieces
}
