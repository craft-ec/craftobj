//! Simple stream-per-request transport for CraftObj.
//!
//! Each request opens a fresh yamux substream:
//!   1. Open stream to peer
//!   2. Write request frame
//!   3. Read response frame
//!   4. Close stream
//!
//! Inbound: accept stream from peer, read request, write response, close.
//! No persistent streams. No stream management. No contention.

use craftobj_transfer::wire::{read_frame, write_request_frame, write_response_frame, StreamFrame};
use craftobj_transfer::{CraftObjRequest, CraftObjResponse};
use libp2p::PeerId;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn, debug, error};

/// An inbound message received from a peer.
pub struct InboundMessage {
    pub peer: PeerId,
    pub seq_id: u64,
    pub request: CraftObjRequest,
    /// The stream to write the response on (same stream the request came from).
    pub stream: libp2p::Stream,
    /// If set, return the stream after writing the response (for persistent stream reuse).
    pub stream_return_tx: Option<tokio::sync::oneshot::Sender<libp2p::Stream>>,
}

/// An outbound request to send to a peer.
pub struct OutboundMessage {
    pub peer: PeerId,
    pub request: CraftObjRequest,
    /// Receives the response from the peer.
    pub reply_tx: Option<tokio::sync::oneshot::Sender<CraftObjResponse>>,
}

/// A batch distribution message for persistent streams.
pub struct DistributionMessage {
    pub peer: PeerId,
    pub content_id: craftobj_core::ContentId,
    pub pieces: Vec<craftobj_transfer::PiecePayload>,
    /// Receives the batch acknowledgment.
    pub reply_tx: oneshot::Sender<CraftObjResponse>,
}

/// Protocol for CraftObj transfer streams.
pub fn transfer_stream_protocol() -> libp2p::StreamProtocol {
    libp2p::StreamProtocol::new(craftobj_core::TRANSFER_PROTOCOL)
}

/// Manages peer addresses and provides stream-per-request transport.
pub struct StreamManager {
    control: libp2p_stream::Control,
    inbound_tx: mpsc::Sender<InboundMessage>,
    /// Track known peers for cleanup
    known_peers: std::collections::HashSet<PeerId>,
    /// Persistent distribution streams (peer -> stream handle)
    distribution_streams: std::collections::HashMap<PeerId, DistributionStreamHandle>,
}

/// Handle to a persistent distribution stream for a peer.
struct DistributionStreamHandle {
    /// Sender for pieces to distribute to this peer
    piece_tx: mpsc::UnboundedSender<PendingPiece>,
    /// Join handle for the distribution task
    task_handle: tokio::task::JoinHandle<()>,
}

/// A piece pending distribution on a persistent stream.
struct PendingPiece {
    content_id: craftobj_core::ContentId,
    pieces: Vec<craftobj_transfer::PiecePayload>,
    reply_tx: oneshot::Sender<CraftObjResponse>,
}

impl StreamManager {
    pub fn new(
        control: libp2p_stream::Control,
    ) -> (
        Self,
        mpsc::Receiver<InboundMessage>,
        mpsc::Sender<OutboundMessage>,
        mpsc::Sender<DistributionMessage>,
        libp2p_stream::Control,  // for direct stream access (fetch)
    ) {
        let (inbound_tx, inbound_rx) = mpsc::channel(8192);
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundMessage>(8192);
        let (distribution_tx, distribution_rx) = mpsc::channel::<DistributionMessage>(1024);

        let mut control_clone = control.clone();

        // Spawn outbound writer — opens fresh stream per request
        tokio::spawn(Self::outbound_loop(control.clone(), outbound_rx));

        // Spawn inbound acceptor — accepts streams from peers
        tokio::spawn(Self::inbound_acceptor(control_clone, inbound_tx.clone()));

        let control_for_fetch = control.clone();
        let mgr = Self {
            control,
            inbound_tx,
            known_peers: std::collections::HashSet::new(),
            distribution_streams: std::collections::HashMap::new(),
        };

        // Start distribution manager
        let mut mgr_clone = mgr.clone_for_distribution();
        tokio::spawn(Self::distribution_manager(mgr_clone, distribution_rx));

        (mgr, inbound_rx, outbound_tx, distribution_tx, control_for_fetch)
    }

    fn clone_for_distribution(&self) -> Self {
        Self {
            control: self.control.clone(),
            inbound_tx: self.inbound_tx.clone(),
            known_peers: std::collections::HashSet::new(),
            distribution_streams: std::collections::HashMap::new(),
        }
    }

    /// Accept incoming streams and read requests from them.
    async fn inbound_acceptor(
        mut control: libp2p_stream::Control,
        inbound_tx: mpsc::Sender<InboundMessage>,
    ) {
        let mut incoming = control.accept(transfer_stream_protocol()).unwrap();
        use futures::StreamExt;
        while let Some((peer, stream)) = incoming.next().await {
            let tx = inbound_tx.clone();
            // Spawn a task per inbound stream — loop reading multiple requests (persistent stream)
            tokio::spawn(async move {
                let (peer_id, mut stream) = (peer, stream);
                let mut req_count = 0u64;
                let stream_start = std::time::Instant::now();
                info!("[inbound] NEW stream from {}", peer_id);
                loop {
                    let read_start = std::time::Instant::now();
                    match read_frame(&mut stream).await {
                        Ok(StreamFrame::Request { seq_id, request }) => {
                            req_count += 1;
                            let read_ms = read_start.elapsed().as_secs_f64() * 1000.0;
                            let req_type = format!("{:?}", std::mem::discriminant(&request));
                            info!("[inbound] req#{} from {} seq={} type={} read_ms={:.1}", req_count, &peer_id.to_string()[..8], seq_id, req_type, read_ms);
                            
                            let (stream_return_tx, stream_return_rx) = tokio::sync::oneshot::channel();
                            let send_start = std::time::Instant::now();
                            if let Err(e) = tx.send(InboundMessage { peer: peer_id, seq_id, request, stream, stream_return_tx: Some(stream_return_tx) }).await {
                                warn!("[inbound] Failed to queue inbound from {}: {}", peer_id, e);
                                break;
                            }
                            let send_ms = send_start.elapsed().as_secs_f64() * 1000.0;
                            
                            // Wait for the stream to be returned after the handler writes the response
                            let wait_start = std::time::Instant::now();
                            match stream_return_rx.await {
                                Ok(returned_stream) => {
                                    let wait_ms = wait_start.elapsed().as_secs_f64() * 1000.0;
                                    info!("[inbound] req#{} stream returned from handler send_ms={:.1} handler_ms={:.1}", req_count, send_ms, wait_ms);
                                    stream = returned_stream;
                                }
                                Err(_) => {
                                    let wait_ms = wait_start.elapsed().as_secs_f64() * 1000.0;
                                    warn!("[inbound] req#{} stream return DROPPED after {:.1}ms — ending loop", req_count, wait_ms);
                                    break;
                                }
                            }
                        }
                        Ok(StreamFrame::Response { .. }) => {
                            warn!("[inbound] Unexpected response frame on inbound from {}", peer_id);
                            break;
                        }
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::UnexpectedEof {
                                warn!("[inbound] read error from {} after {} reqs: {}", peer_id, req_count, e);
                            } else {
                                info!("[inbound] EOF from {} after {} reqs ({:.1}s)", &peer_id.to_string()[..8], req_count, stream_start.elapsed().as_secs_f64());
                            }
                            break;
                        }
                    }
                }
            });
        }
    }

    /// Process outbound requests — open fresh stream per request.
    async fn outbound_loop(
        control: libp2p_stream::Control,
        mut rx: mpsc::Receiver<OutboundMessage>,
    ) {
        let mut msg_count = 0u64;
        while let Some(msg) = rx.recv().await {
            msg_count += 1;
            let mut ctrl = control.clone();
            let msg_id = msg_count;
            tokio::spawn(async move {
                let total_start = std::time::Instant::now();
                let peer = msg.peer;
                let req_desc = format!("{:?}", std::mem::discriminant(&msg.request));

                // Open fresh stream
                let open_start = std::time::Instant::now();
                let mut stream = match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    ctrl.open_stream(peer, transfer_stream_protocol()),
                ).await {
                    Ok(Ok(s)) => {
                        info!("[outbound] #{} open stream to {} in {:.1}ms", msg_id, &peer.to_string()[..8], open_start.elapsed().as_secs_f64() * 1000.0);
                        s
                    }
                    Ok(Err(e)) => {
                        warn!("[outbound] #{} FAILED open stream to {}: {} ({:.1}ms)", msg_id, &peer.to_string()[..8], e, open_start.elapsed().as_secs_f64() * 1000.0);
                        if let Some(tx) = msg.reply_tx {
                            let _ = tx.send(CraftObjResponse::Ack {
                                status: craftobj_core::WireStatus::Error,
                            });
                        }
                        return;
                    }
                    Err(_) => {
                        warn!("[outbound] #{} TIMEOUT open stream to {} (5s)", msg_id, &peer.to_string()[..8]);
                        if let Some(tx) = msg.reply_tx {
                            let _ = tx.send(CraftObjResponse::Ack {
                                status: craftobj_core::WireStatus::Error,
                            });
                        }
                        return;
                    }
                };

                // Write request
                let write_start = std::time::Instant::now();
                if let Err(e) = write_request_frame(&mut stream, 0, &msg.request).await {
                    warn!("[outbound] #{} WRITE FAILED to {}: {} ({:.1}ms)", msg_id, &peer.to_string()[..8], e, write_start.elapsed().as_secs_f64() * 1000.0);
                    if let Some(tx) = msg.reply_tx {
                        let _ = tx.send(CraftObjResponse::Ack {
                            status: craftobj_core::WireStatus::Error,
                        });
                    }
                    return;
                }
                let write_ms = write_start.elapsed().as_secs_f64() * 1000.0;
                info!("[outbound] #{} WRITE OK {} to {} in {:.1}ms", msg_id, req_desc, &peer.to_string()[..8], write_ms);

                // Read response on same stream
                let read_start = std::time::Instant::now();
                match tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    read_frame(&mut stream),
                ).await {
                    Ok(Ok(StreamFrame::Response { response, .. })) => {
                        let read_ms = read_start.elapsed().as_secs_f64() * 1000.0;
                        let total_ms = total_start.elapsed().as_secs_f64() * 1000.0;
                        let resp_desc = match &response {
                            CraftObjResponse::PieceBatch { pieces } => format!("PieceBatch({})", pieces.len()),
                            CraftObjResponse::BatchAck { confirmed_pieces, .. } => format!("BatchAck({})", confirmed_pieces.len()),
                            CraftObjResponse::Ack { status } => format!("Ack({:?})", status),
                            other => format!("{:?}", std::mem::discriminant(other)),
                        };
                        info!("[outbound] #{} READ OK from {} in {:.1}ms | total={:.1}ms | {}", msg_id, &peer.to_string()[..8], read_ms, total_ms, resp_desc);
                        if let Some(tx) = msg.reply_tx {
                            let _ = tx.send(response);
                        }
                    }
                    Ok(Ok(StreamFrame::Request { .. })) => {
                        warn!("[outbound] #{} unexpected request frame from {} ({:.1}ms)", msg_id, &peer.to_string()[..8], read_start.elapsed().as_secs_f64() * 1000.0);
                    }
                    Ok(Err(e)) => {
                        warn!("[outbound] #{} READ FAILED from {}: {} ({:.1}ms)", msg_id, &peer.to_string()[..8], e, read_start.elapsed().as_secs_f64() * 1000.0);
                    }
                    Err(_) => {
                        warn!("[outbound] #{} READ TIMEOUT from {} (30s)", msg_id, &peer.to_string()[..8]);
                    }
                }
                // Stream drops here — closed automatically
            });
        }
    }

    /// Manages persistent distribution streams.
    async fn distribution_manager(
        mut mgr: Self,
        mut distribution_rx: mpsc::Receiver<DistributionMessage>,
    ) {
        while let Some(msg) = distribution_rx.recv().await {
            debug!("[stream_mgr] Distribution request for {} to {}", msg.content_id, msg.peer);
            
            // Get or create persistent stream for this peer
            if !mgr.distribution_streams.contains_key(&msg.peer) {
                mgr.create_distribution_stream(msg.peer).await;
            }
            
            // Send pieces to the persistent stream
            if let Some(handle) = mgr.distribution_streams.get(&msg.peer) {
                let pending = PendingPiece {
                    content_id: msg.content_id,
                    pieces: msg.pieces,
                    reply_tx: msg.reply_tx,
                };
                
                if let Err(e) = handle.piece_tx.send(pending) {
                    error!("[stream_mgr] Failed to send to distribution stream for {}: {}", msg.peer, e);
                    // Stream is dead, remove it
                    mgr.distribution_streams.remove(&msg.peer);
                }
            } else {
                error!("[stream_mgr] Failed to create distribution stream for {}", msg.peer);
                let _ = msg.reply_tx.send(CraftObjResponse::Ack { 
                    status: craftobj_core::WireStatus::Error 
                });
            }
        }
    }

    /// Creates a persistent distribution stream for a peer.
    async fn create_distribution_stream(&mut self, peer: PeerId) {
        debug!("[stream_mgr] Creating distribution stream for {}", peer);
        
        let (piece_tx, piece_rx) = mpsc::unbounded_channel();
        let control = self.control.clone();
        
        let task_handle = tokio::spawn(async move {
            Self::distribution_stream_task(peer, control, piece_rx).await;
        });
        
        let handle = DistributionStreamHandle {
            piece_tx,
            task_handle,
        };
        
        self.distribution_streams.insert(peer, handle);
    }

    /// Task that manages a single persistent distribution stream to a peer.
    async fn distribution_stream_task(
        peer: PeerId,
        mut control: libp2p_stream::Control,
        mut piece_rx: mpsc::UnboundedReceiver<PendingPiece>,
    ) {
        info!("[dist_stream] STARTED for peer {}", peer);
        
        let mut current_stream: Option<libp2p::Stream> = None;
        let mut seq_id = 1u64;
        let mut batch_count = 0u64;
        let task_start = std::time::Instant::now();
        
        while let Some(pending) = piece_rx.recv().await {
            batch_count += 1;
            let batch_start = std::time::Instant::now();
            let piece_count = pending.pieces.len();
            let batch_bytes: usize = pending.pieces.iter().map(|p| p.data.len() + p.coefficients.len()).sum();
            
            info!("[dist_stream] batch#{} peer={} pieces={} bytes={} stream_reuse={}", 
                batch_count, &peer.to_string()[..8], piece_count, batch_bytes, current_stream.is_some());
            
            // Try to reuse existing stream, or open a new one if needed
            let open_start = std::time::Instant::now();
            let mut stream = match current_stream.take() {
                Some(s) => {
                    info!("[dist_stream] batch#{} reusing stream (0ms)", batch_count);
                    s
                }
                None => {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        control.open_stream(peer, transfer_stream_protocol())
                    ).await {
                        Ok(Ok(s)) => {
                            info!("[dist_stream] batch#{} opened NEW stream in {:.1}ms", batch_count, open_start.elapsed().as_secs_f64() * 1000.0);
                            s
                        }
                        Ok(Err(e)) => {
                            error!("[dist_stream] batch#{} FAILED to open stream: {} (after {:.1}ms)", batch_count, e, open_start.elapsed().as_secs_f64() * 1000.0);
                            let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                                status: craftobj_core::WireStatus::Error 
                            });
                            continue;
                        }
                        Err(_) => {
                            error!("[dist_stream] batch#{} TIMEOUT opening stream (10s)", batch_count);
                            let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                                status: craftobj_core::WireStatus::Error 
                            });
                            continue;
                        }
                    }
                }
            };
            
            let request = CraftObjRequest::PieceBatchPush {
                content_id: pending.content_id,
                pieces: pending.pieces,
            };
            
            // Send batch request on the persistent stream
            let write_start = std::time::Instant::now();
            match tokio::time::timeout(
                std::time::Duration::from_secs(15),
                write_request_frame(&mut stream, seq_id, &request)
            ).await {
                Ok(Ok(())) => {
                    let write_ms = write_start.elapsed().as_secs_f64() * 1000.0;
                    info!("[dist_stream] batch#{} WRITE OK in {:.1}ms ({} pieces, {} bytes)", batch_count, write_ms, piece_count, batch_bytes);
                    
                    // Read response on the same stream
                    let read_start = std::time::Instant::now();
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        read_frame(&mut stream)
                    ).await {
                        Ok(Ok(StreamFrame::Response { response, .. })) => {
                            let read_ms = read_start.elapsed().as_secs_f64() * 1000.0;
                            let total_ms = batch_start.elapsed().as_secs_f64() * 1000.0;
                            info!("[dist_stream] batch#{} READ OK in {:.1}ms | total={:.1}ms | response={:?}", 
                                batch_count, read_ms, total_ms, std::mem::discriminant(&response));
                            let _ = pending.reply_tx.send(response);
                            
                            // Keep the stream for the next batch
                            current_stream = Some(stream);
                            seq_id += 1;
                        }
                        Ok(Ok(frame)) => {
                            warn!("[dist_stream] batch#{} unexpected frame type (not Response) after {:.1}ms", batch_count, read_start.elapsed().as_secs_f64() * 1000.0);
                            let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                                status: craftobj_core::WireStatus::Error 
                            });
                        }
                        Ok(Err(e)) => {
                            error!("[dist_stream] batch#{} READ FAILED: {} (after {:.1}ms)", batch_count, e, read_start.elapsed().as_secs_f64() * 1000.0);
                            let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                                status: craftobj_core::WireStatus::Error 
                            });
                        }
                        Err(_) => {
                            error!("[dist_stream] batch#{} READ TIMEOUT (30s)", batch_count);
                            let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                                status: craftobj_core::WireStatus::Error 
                            });
                        }
                    }
                }
                Ok(Err(e)) => {
                    error!("[dist_stream] batch#{} WRITE FAILED: {} (after {:.1}ms)", batch_count, e, write_start.elapsed().as_secs_f64() * 1000.0);
                    let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                        status: craftobj_core::WireStatus::Error 
                    });
                }
                Err(_) => {
                    error!("[dist_stream] batch#{} WRITE TIMEOUT (15s)", batch_count);
                    let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                        status: craftobj_core::WireStatus::Error 
                    });
                }
            }
        }
        
        let total_secs = task_start.elapsed().as_secs_f64();
        info!("[dist_stream] ENDED for peer {} — {} batches in {:.1}s", peer, batch_count, total_secs);
    }

    /// Called from swarm event loop — no-op now (no persistent streams to manage).
    pub fn poll_open_streams(&mut self) -> usize {
        0
    }

    pub fn accept_stream(&mut self, _peer: PeerId, _stream: libp2p::Stream) {
        // No-op: inbound_acceptor handles all incoming streams
    }

    pub fn on_peer_disconnected(&mut self, peer: &PeerId) {
        self.known_peers.remove(peer);
        
        // Clean up distribution stream
        if let Some(handle) = self.distribution_streams.remove(peer) {
            handle.task_handle.abort();
            debug!("[stream_mgr] Cleaned up distribution stream for {}", peer);
        }
    }

    pub fn on_peer_connected(&mut self, peer: &PeerId) {
        self.known_peers.insert(*peer);
    }

    pub fn cleanup_dead_streams(&mut self) {
        // No-op: no persistent streams
    }

    pub fn force_reopen_outbound(&mut self, _peer: &PeerId) {
        // No-op: no persistent streams
    }

    pub fn stream_count(&self) -> usize {
        self.distribution_streams.len()
    }

    pub fn has_stream(&self, _peer: &PeerId) -> bool {
        true // Can always open a fresh stream if peer is connected
    }

    pub fn ensure_opening(&mut self, _peer: PeerId) {
        // No-op: streams opened on demand
    }

    pub fn clear_open_cooldown(&mut self, _peer: &PeerId) {
        // No-op
    }
}
