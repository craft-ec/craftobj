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
use tokio::sync::mpsc;
use tracing::{info, warn};

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
}

impl StreamManager {
    pub fn new(
        control: libp2p_stream::Control,
    ) -> (
        Self,
        mpsc::Receiver<InboundMessage>,
        mpsc::Sender<OutboundMessage>,
    ) {
        let (inbound_tx, inbound_rx) = mpsc::channel(8192);
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundMessage>(8192);

        let mut control_clone = control.clone();

        // Spawn outbound writer — opens fresh stream per request
        tokio::spawn(Self::outbound_loop(control.clone(), outbound_rx));

        // Spawn inbound acceptor — accepts streams from peers
        tokio::spawn(Self::inbound_acceptor(control_clone, inbound_tx.clone()));

        let mgr = Self {
            control,
            inbound_tx,
            known_peers: std::collections::HashSet::new(),
        };

        (mgr, inbound_rx, outbound_tx)
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

    /// Called from swarm event loop — no-op now (no persistent streams to manage).
    pub fn poll_open_streams(&mut self) -> usize {
        0
    }

    pub fn accept_stream(&mut self, _peer: PeerId, _stream: libp2p::Stream) {
        // No-op: inbound_acceptor handles all incoming streams
    }

    pub fn on_peer_disconnected(&mut self, peer: &PeerId) {
        self.known_peers.remove(peer);
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
        0 // No persistent streams
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
