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
    ) {
        let (inbound_tx, inbound_rx) = mpsc::channel(8192);
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundMessage>(8192);
        let (distribution_tx, distribution_rx) = mpsc::channel::<DistributionMessage>(1024);

        let mut control_clone = control.clone();

        // Spawn outbound writer — opens fresh stream per request
        tokio::spawn(Self::outbound_loop(control.clone(), outbound_rx));

        // Spawn inbound acceptor — accepts streams from peers
        tokio::spawn(Self::inbound_acceptor(control_clone, inbound_tx.clone()));

        let mgr = Self {
            control,
            inbound_tx,
            known_peers: std::collections::HashSet::new(),
            distribution_streams: std::collections::HashMap::new(),
        };

        // Start distribution manager
        let mut mgr_clone = mgr.clone_for_distribution();
        tokio::spawn(Self::distribution_manager(mgr_clone, distribution_rx));

        (mgr, inbound_rx, outbound_tx, distribution_tx)
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
            // Spawn a task per inbound stream — read one request, pass to handler
            tokio::spawn(async move {
                let (peer_id, mut stream) = (peer, stream);
                match read_frame(&mut stream).await {
                    Ok(StreamFrame::Request { seq_id, request }) => {
                        info!("[stream_mgr.rs] Inbound request from {} seq={}", peer_id, seq_id);
                        if let Err(e) = tx.send(InboundMessage { peer: peer_id, seq_id, request, stream }).await {
                            warn!("[stream_mgr.rs] Failed to queue inbound from {}: {}", peer_id, e);
                        }
                    }
                    Ok(StreamFrame::Response { .. }) => {
                        warn!("[stream_mgr.rs] Unexpected response frame on inbound from {}", peer_id);
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::UnexpectedEof {
                            warn!("[stream_mgr.rs] Inbound read error from {}: {}", peer_id, e);
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
        while let Some(msg) = rx.recv().await {
            let mut ctrl = control.clone();
            tokio::spawn(async move {
                let peer = msg.peer;
                let req_desc = format!("{:?}", std::mem::discriminant(&msg.request));

                // Open fresh stream
                let mut stream = match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    ctrl.open_stream(peer, transfer_stream_protocol()),
                ).await {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        warn!("[stream_mgr.rs] Failed to open stream to {}: {}", peer, e);
                        if let Some(tx) = msg.reply_tx {
                            let _ = tx.send(CraftObjResponse::Ack {
                                status: craftobj_core::WireStatus::Error,
                            });
                        }
                        return;
                    }
                    Err(_) => {
                        warn!("[stream_mgr.rs] Timeout opening stream to {}", peer);
                        if let Some(tx) = msg.reply_tx {
                            let _ = tx.send(CraftObjResponse::Ack {
                                status: craftobj_core::WireStatus::Error,
                            });
                        }
                        return;
                    }
                };

                // Write request
                if let Err(e) = write_request_frame(&mut stream, 0, &msg.request).await {
                    warn!("[stream_mgr.rs] Write to {} failed: {}", peer, e);
                    if let Some(tx) = msg.reply_tx {
                        let _ = tx.send(CraftObjResponse::Ack {
                            status: craftobj_core::WireStatus::Error,
                        });
                    }
                    return;
                }
                info!("[stream_mgr.rs] Sent {} to {}", req_desc, peer);

                // Read response on same stream
                match tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    read_frame(&mut stream),
                ).await {
                    Ok(Ok(StreamFrame::Response { response, .. })) => {
                        info!("[stream_mgr.rs] Got response from {} for {}", peer, req_desc);
                        if let Some(tx) = msg.reply_tx {
                            let _ = tx.send(response);
                        }
                    }
                    Ok(Ok(StreamFrame::Request { .. })) => {
                        warn!("[stream_mgr.rs] Unexpected request frame from {} (expected response)", peer);
                    }
                    Ok(Err(e)) => {
                        warn!("[stream_mgr.rs] Read response from {} failed: {}", peer, e);
                    }
                    Err(_) => {
                        warn!("[stream_mgr.rs] Response timeout from {} for {}", peer, req_desc);
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
        info!("[stream_mgr] Starting distribution stream task for {}", peer);
        
        // Simple approach: handle requests sequentially but on a persistent stream
        // This still provides better performance than opening a new stream per piece
        while let Some(pending) = piece_rx.recv().await {
            // Open stream for this batch (reuse if possible)
            let mut stream = match control.open_stream(peer, transfer_stream_protocol()).await {
                Ok(s) => s,
                Err(e) => {
                    error!("[stream_mgr] Failed to open distribution stream to {}: {}", peer, e);
                    let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                        status: craftobj_core::WireStatus::Error 
                    });
                    continue;
                }
            };
            
            let request = CraftObjRequest::PieceBatchPush {
                content_id: pending.content_id,
                pieces: pending.pieces,
            };
            
            // Send batch request
            match write_request_frame(&mut stream, 1, &request).await {
                Ok(()) => {
                    debug!("[stream_mgr] Sent batch {} to {}", pending.content_id, peer);
                    
                    // Read response
                    match read_frame(&mut stream).await {
                        Ok(StreamFrame::Response { response, .. }) => {
                            debug!("[stream_mgr] Got batch response from {}", peer);
                            let _ = pending.reply_tx.send(response);
                        }
                        Ok(_) => {
                            warn!("[stream_mgr] Unexpected frame type from {}", peer);
                            let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                                status: craftobj_core::WireStatus::Error 
                            });
                        }
                        Err(e) => {
                            error!("[stream_mgr] Failed to read batch response from {}: {}", peer, e);
                            let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                                status: craftobj_core::WireStatus::Error 
                            });
                        }
                    }
                }
                Err(e) => {
                    error!("[stream_mgr] Failed to write batch to {}: {}", peer, e);
                    let _ = pending.reply_tx.send(CraftObjResponse::Ack { 
                        status: craftobj_core::WireStatus::Error 
                    });
                }
            }
        }
        
        info!("[stream_mgr] Distribution stream task ended for {}", peer);
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
