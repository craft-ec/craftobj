//! Two-unidirectional-stream transport per peer for DataCraft.
//!
//! Adapted from TunnelCraft's StreamManager. Each peer pair uses two streams:
//! - **Outbound**: we open → we write requests, peer reads.
//! - **Inbound**: peer opens → peer writes requests, we read.
//!
//! Acks/responses for requests received on inbound are sent on outbound.
//! The peer reads responses from their inbound (our outbound) and matches by seq_id.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use datacraft_transfer::wire::{read_frame, write_request_frame, StreamFrame};
use datacraft_transfer::{DataCraftRequest, DataCraftResponse};
use libp2p::PeerId;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Cooldown after a failed outbound open before retrying (seconds).
const OPEN_RETRY_COOLDOWN_SECS: u64 = 1;

/// Protocol for DataCraft transfer streams.
pub fn transfer_stream_protocol() -> libp2p::StreamProtocol {
    libp2p::StreamProtocol::new(datacraft_core::TRANSFER_PROTOCOL)
}

/// Shared pending-ack map type: seq_id → oneshot for response.
type PendingAcks = Arc<std::sync::Mutex<HashMap<u64, oneshot::Sender<DataCraftResponse>>>>;

/// Global registry: peer → pending acks (shared between writer loop + reader loops).
type PendingAcksRegistry = Arc<std::sync::RwLock<HashMap<PeerId, PendingAcks>>>;

/// An inbound message received from a peer stream.
#[derive(Debug)]
pub struct InboundMessage {
    pub peer: PeerId,
    pub seq_id: u64,
    pub request: DataCraftRequest,
}

/// An outbound message queued for the background writer task.
pub struct OutboundMessage {
    pub peer: PeerId,
    pub request: DataCraftRequest,
    /// If set, the sender receives the response matched by seq_id.
    pub ack_tx: Option<oneshot::Sender<DataCraftResponse>>,
}

/// Per-peer writer handle for the background writer task.
struct PeerWriterHandle {
    writer: Arc<Mutex<libp2p::Stream>>,
    next_seq: Arc<AtomicU64>,
    poisoned: Arc<AtomicBool>,
}

type WriterRegistry = Arc<std::sync::RwLock<HashMap<PeerId, PeerWriterHandle>>>;

struct PeerConnection {
    outbound: Option<OutboundHandle>,
    inbound: Option<InboundHandle>,
    pending_acks: PendingAcks,
}

struct OutboundHandle {
    writer: Arc<Mutex<libp2p::Stream>>,
    #[allow(dead_code)]
    next_seq: Arc<AtomicU64>,
}

struct InboundHandle {
    reader_handle: JoinHandle<()>,
}

/// Manages two unidirectional streams per peer for DataCraft transfer.
pub struct StreamManager {
    control: libp2p_stream::Control,
    peers: HashMap<PeerId, PeerConnection>,
    inbound_tx: mpsc::Sender<InboundMessage>,
    open_result_rx: mpsc::UnboundedReceiver<(PeerId, Result<libp2p::Stream, std::io::Error>)>,
    open_result_tx: mpsc::UnboundedSender<(PeerId, Result<libp2p::Stream, std::io::Error>)>,
    opening: HashSet<PeerId>,
    open_cooldown: HashMap<PeerId, Instant>,
    writer_registry: WriterRegistry,
    pending_acks_registry: PendingAcksRegistry,
    write_fail_rx: mpsc::UnboundedReceiver<PeerId>,
    need_stream_rx: mpsc::UnboundedReceiver<PeerId>,
    /// Channel for spawned inbound handlers to send responses back
    response_rx: mpsc::UnboundedReceiver<(PeerId, u64, DataCraftResponse)>,
    response_tx: mpsc::UnboundedSender<(PeerId, u64, DataCraftResponse)>,
}

impl StreamManager {
    /// Create a new StreamManager.
    ///
    /// Returns (StreamManager, inbound_rx, outbound_tx).
    pub fn new(
        control: libp2p_stream::Control,
    ) -> (
        Self,
        mpsc::Receiver<InboundMessage>,
        mpsc::Sender<OutboundMessage>,
    ) {
        let (inbound_tx, inbound_rx) = mpsc::channel(8192);
        let (open_result_tx, open_result_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound_rx) = mpsc::channel::<OutboundMessage>(8192);
        let (write_fail_tx, write_fail_rx) = mpsc::unbounded_channel();
        let (need_stream_tx, need_stream_rx) = mpsc::unbounded_channel();

        let writer_registry: WriterRegistry = Arc::new(std::sync::RwLock::new(HashMap::new()));
        let pending_acks_registry: PendingAcksRegistry = Arc::new(std::sync::RwLock::new(HashMap::new()));

        tokio::spawn(Self::outbound_writer_loop(
            writer_registry.clone(),
            pending_acks_registry.clone(),
            outbound_rx,
            write_fail_tx,
            need_stream_tx,
        ));

        let (response_tx, response_rx) = mpsc::unbounded_channel();

        let mgr = Self {
            control,
            peers: HashMap::new(),
            inbound_tx,
            open_result_rx,
            open_result_tx,
            opening: HashSet::new(),
            open_cooldown: HashMap::new(),
            writer_registry,
            pending_acks_registry,
            write_fail_rx,
            need_stream_rx,
            response_rx,
            response_tx,
        };

        (mgr, inbound_rx, outbound_tx)
    }

    /// Send a response frame to a peer on our outbound stream (fire-and-forget).
    /// Get a sender for spawned tasks to return responses.
    pub fn get_response_sender(&self) -> mpsc::UnboundedSender<(PeerId, u64, DataCraftResponse)> {
        self.response_tx.clone()
    }

    /// Drain responses from spawned inbound handlers and write them.
    pub fn drain_responses(&mut self) {
        while let Ok((peer, seq_id, response)) = self.response_rx.try_recv() {
            info!("[stream_mgr.rs] drain_responses: sending response to {} seq={}", peer, seq_id);
            self.send_response(peer, seq_id, response);
        }
    }

    pub fn send_response(&self, peer: PeerId, seq_id: u64, response: DataCraftResponse) {
        if let Some(pc) = self.peers.get(&peer) {
            if let Some(ref out) = pc.outbound {
                let writer = out.writer.clone();
                info!("[stream_mgr.rs] send_response: writing response to {} seq={}", peer, seq_id);
                tokio::spawn(async move {
                    let mut w = writer.lock().await;
                    match datacraft_transfer::wire::write_response_frame(&mut *w, seq_id, &response).await {
                        Ok(()) => info!("[stream_mgr.rs] send_response: wrote response to {} seq={} successfully", peer, seq_id),
                        Err(e) => warn!("[stream_mgr.rs] Response write to {} seq={} failed: {}", peer, seq_id, e),
                    }
                });
            } else {
                warn!("[stream_mgr.rs] No outbound to peer {} for response (seq={}) — cannot reply!", peer, seq_id);
            }
        } else {
            warn!("[stream_mgr.rs] No peer state for {} — cannot send response (seq={})", peer, seq_id);
        }
    }

    /// Accept an inbound stream from a peer.
    pub fn accept_stream(&mut self, peer: PeerId, stream: libp2p::Stream) {
        let pc = self.get_or_create_peer(peer);

        if let Some(ref inbound) = pc.inbound {
            if !inbound.reader_handle.is_finished() {
                debug!("Already have healthy inbound from {} — dropping", peer);
                drop(stream);
                return;
            }
        }

        self.open_cooldown.remove(&peer);
        self.register_inbound(peer, stream);
        info!("[stream_mgr.rs] Accepted inbound from peer {}", peer);
        self.ensure_opening(peer);
    }

    /// Clear open cooldown for a peer.
    pub fn clear_open_cooldown(&mut self, peer: &PeerId) {
        self.open_cooldown.remove(peer);
    }

    /// Ensure our outbound stream to this peer is opening.
    pub fn ensure_opening(&mut self, peer: PeerId) {
        if self.peers.get(&peer).map_or(false, |pc| pc.outbound.is_some()) {
            return;
        }
        if self.opening.contains(&peer) {
            return;
        }
        if let Some(&deadline) = self.open_cooldown.get(&peer) {
            if Instant::now() < deadline {
                return;
            }
            self.open_cooldown.remove(&peer);
        }
        self.spawn_open(peer);
    }

    fn spawn_open(&mut self, peer: PeerId) {
        self.opening.insert(peer);
        let mut control = self.control.clone();
        let tx = self.open_result_tx.clone();
        tokio::spawn(async move {
            debug!("Background: opening outbound to {} ...", peer);
            match tokio::time::timeout(
                std::time::Duration::from_secs(10),
                control.open_stream(peer, transfer_stream_protocol()),
            )
            .await
            {
                Ok(Ok(stream)) => { let _ = tx.send((peer, Ok(stream))); }
                Ok(Err(e)) => {
                    warn!("[stream_mgr.rs] Background: outbound open to {} failed: {}", peer, e);
                    let _ = tx.send((peer, Err(std::io::Error::new(std::io::ErrorKind::ConnectionRefused, format!("open_stream failed: {}", e)))));
                }
                Err(_) => {
                    warn!("[stream_mgr.rs] Background: outbound open to {} timed out (10s)", peer);
                    let _ = tx.send((peer, Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "open_stream timed out"))));
                }
            }
        });
    }

    /// Collect completed background outbound opens and drain write failures.
    pub fn poll_open_streams(&mut self) -> usize {
        // Drain responses from spawned inbound handlers first
        self.drain_responses();

        let mut opened = 0;
        while let Ok((peer, result)) = self.open_result_rx.try_recv() {
            self.opening.remove(&peer);
            match result {
                Ok(stream) => {
                    if self.peers.get(&peer).map_or(false, |pc| pc.outbound.is_some()) {
                        continue;
                    }
                    self.register_outbound(peer, stream);
                    info!("[stream_mgr.rs] Opened outbound to peer {}", peer);
                    opened += 1;
                }
                Err(e) => {
                    debug!("Background outbound open to {} failed: {}", peer, e);
                    self.open_cooldown.insert(peer, Instant::now() + std::time::Duration::from_secs(OPEN_RETRY_COOLDOWN_SECS));
                }
            }
        }

        while let Ok(peer) = self.write_fail_rx.try_recv() {
            self.close_outbound(&peer);
            self.ensure_opening(peer);
        }

        {
            let mut need_peers = HashSet::new();
            while let Ok(peer) = self.need_stream_rx.try_recv() {
                need_peers.insert(peer);
            }
            for peer in need_peers {
                self.ensure_opening(peer);
            }
        }

        opened
    }

    pub fn stream_count(&self) -> usize {
        self.peers.values().filter(|pc| pc.outbound.is_some()).count()
    }

    pub fn has_stream(&self, peer: &PeerId) -> bool {
        self.peers.get(peer).map_or(false, |pc| pc.outbound.is_some())
    }

    pub fn on_peer_disconnected(&mut self, peer: &PeerId) {
        self.opening.remove(peer);
        self.open_cooldown.remove(peer);
        if let Some(pc) = self.peers.remove(peer) {
            if pc.outbound.is_some() {
                self.writer_registry.write().unwrap().remove(peer);
            }
            self.pending_acks_registry.write().unwrap().remove(peer);
            if let Some(inbound) = pc.inbound {
                inbound.reader_handle.abort();
            }
        }
    }

    pub fn cleanup_dead_streams(&mut self) {
        let dead: Vec<PeerId> = self.peers.iter()
            .filter(|(_, pc)| pc.inbound.as_ref().map_or(false, |i| i.reader_handle.is_finished()))
            .map(|(peer, _)| *peer)
            .collect();
        for peer in dead {
            self.close_inbound(&peer);
        }
    }

    fn close_outbound(&mut self, peer: &PeerId) {
        if let Some(pc) = self.peers.get_mut(peer) {
            if pc.outbound.take().is_some() {
                self.writer_registry.write().unwrap().remove(peer);
            }
        }
        self.maybe_remove_peer(peer);
    }

    fn close_inbound(&mut self, peer: &PeerId) {
        if let Some(pc) = self.peers.get_mut(peer) {
            if let Some(inbound) = pc.inbound.take() {
                inbound.reader_handle.abort();
            }
        }
        self.maybe_remove_peer(peer);
    }

    fn maybe_remove_peer(&mut self, peer: &PeerId) {
        if let Some(pc) = self.peers.get(peer) {
            if pc.outbound.is_none() && pc.inbound.is_none() {
                self.peers.remove(peer);
                self.pending_acks_registry.write().unwrap().remove(peer);
            }
        }
    }

    fn get_or_create_peer(&mut self, peer: PeerId) -> &mut PeerConnection {
        let registry = &self.pending_acks_registry;
        self.peers.entry(peer).or_insert_with(|| {
            let acks: PendingAcks = Arc::new(std::sync::Mutex::new(HashMap::new()));
            registry.write().unwrap().insert(peer, acks.clone());
            PeerConnection {
                outbound: None,
                inbound: None,
                pending_acks: acks,
            }
        })
    }

    fn register_outbound(&mut self, peer: PeerId, stream: libp2p::Stream) {
        let writer_arc = Arc::new(Mutex::new(stream));
        let seq_arc = Arc::new(AtomicU64::new(0));
        let poisoned_arc = Arc::new(AtomicBool::new(false));

        self.writer_registry.write().unwrap().insert(peer, PeerWriterHandle {
            writer: writer_arc.clone(),
            next_seq: seq_arc.clone(),
            poisoned: poisoned_arc,
        });

        let pc = self.get_or_create_peer(peer);
        pc.outbound = Some(OutboundHandle {
            writer: writer_arc,
            next_seq: seq_arc,
        });
    }

    fn register_inbound(&mut self, peer: PeerId, stream: libp2p::Stream) {
        let pc = self.get_or_create_peer(peer);
        let pending_acks = pc.pending_acks.clone();

        if let Some(old) = pc.inbound.take() {
            old.reader_handle.abort();
        }

        let reader_handle = tokio::spawn(Self::reader_loop(
            peer, stream, pending_acks, self.inbound_tx.clone(),
        ));

        self.peers.get_mut(&peer).unwrap().inbound = Some(InboundHandle { reader_handle });
    }

    /// Background writer task.
    async fn outbound_writer_loop(
        registry: WriterRegistry,
        ack_registry: PendingAcksRegistry,
        mut rx: mpsc::Receiver<OutboundMessage>,
        write_fail_tx: mpsc::UnboundedSender<PeerId>,
        need_stream_tx: mpsc::UnboundedSender<PeerId>,
    ) {
        let mut retry_buf: VecDeque<OutboundMessage> = VecDeque::new();
        let mut flush_interval = tokio::time::interval(std::time::Duration::from_millis(100));
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let (write_retry_tx, mut write_retry_rx) = mpsc::unbounded_channel::<OutboundMessage>();

        loop {
            tokio::select! {
                biased;
                msg = rx.recv() => {
                    let Some(outbound) = msg else { break; };
                    Self::try_write_or_buffer(&registry, &ack_registry, &write_fail_tx, &need_stream_tx, &write_retry_tx, outbound, &mut retry_buf);
                }
                retry_msg = write_retry_rx.recv() => {
                    if let Some(outbound) = retry_msg {
                        if retry_buf.len() < 1024 { retry_buf.push_back(outbound); }
                    }
                }
                _ = flush_interval.tick() => {
                    Self::flush_retry_buffer(&registry, &ack_registry, &write_fail_tx, &need_stream_tx, &write_retry_tx, &mut retry_buf);
                }
            }
        }
    }

    fn try_write_or_buffer(
        registry: &WriterRegistry,
        ack_registry: &PendingAcksRegistry,
        write_fail_tx: &mpsc::UnboundedSender<PeerId>,
        need_stream_tx: &mpsc::UnboundedSender<PeerId>,
        write_retry_tx: &mpsc::UnboundedSender<OutboundMessage>,
        outbound: OutboundMessage,
        retry_buf: &mut VecDeque<OutboundMessage>,
    ) {
        let handle = {
            let reg = registry.read().unwrap();
            reg.get(&outbound.peer).map(|h| (h.writer.clone(), h.next_seq.clone(), h.poisoned.clone()))
        };

        if let Some((writer, next_seq, poisoned)) = handle {
            if poisoned.load(Ordering::Relaxed) {
                if retry_buf.len() < 1024 { retry_buf.push_back(outbound); }
                return;
            }
            let peer = outbound.peer;
            let seq_id = next_seq.fetch_add(1, Ordering::Relaxed);

            // Register ack channel before write
            if let Some(ack_tx) = outbound.ack_tx {
                if let Some(acks) = ack_registry.read().unwrap().get(&peer) {
                    acks.lock().unwrap().insert(seq_id, ack_tx);
                }
            }

            let request = outbound.request;
            let req_desc = format!("{:?}", std::mem::discriminant(&request));
            let wf_tx = write_fail_tx.clone();
            let retry_tx = write_retry_tx.clone();
            let reg = registry.clone();
            let ack_reg = ack_registry.clone();
            info!("[stream_mgr.rs] StreamManager: writing {} to {} (seq={})", req_desc, peer, seq_id);
            tokio::spawn(async move {
                if poisoned.load(Ordering::Relaxed) { 
                    warn!("[stream_mgr.rs] StreamManager: stream to {} is poisoned, skipping write", peer);
                    return; 
                }
                let mut w = writer.lock().await;
                match write_request_frame(&mut *w, seq_id, &request).await {
                    Ok(()) => {
                        info!("[stream_mgr.rs] StreamManager: wrote {} to {} (seq={}) successfully", req_desc, peer, seq_id);
                    }
                    Err(e) => {
                        warn!("[stream_mgr.rs] Outbound write to {} failed: {} (seq={})", peer, e, seq_id);
                        poisoned.store(true, Ordering::Relaxed);
                        drop(w);
                        reg.write().unwrap().remove(&peer);
                        // Recover ack_tx from registry so it can be retried
                        let recovered_ack = ack_reg.read().unwrap()
                            .get(&peer)
                            .and_then(|acks| acks.lock().unwrap().remove(&seq_id));
                        let _ = wf_tx.send(peer);
                        let _ = retry_tx.send(OutboundMessage { peer, request, ack_tx: recovered_ack });
                    }
                }
            });
        } else {
            info!("[stream_mgr.rs] StreamManager: no writer for {}, buffering message (retry_buf={})", outbound.peer, retry_buf.len());
            let _ = need_stream_tx.send(outbound.peer);
            if retry_buf.len() < 1024 { retry_buf.push_back(outbound); }
        }
    }

    fn flush_retry_buffer(
        registry: &WriterRegistry,
        ack_registry: &PendingAcksRegistry,
        write_fail_tx: &mpsc::UnboundedSender<PeerId>,
        need_stream_tx: &mpsc::UnboundedSender<PeerId>,
        write_retry_tx: &mpsc::UnboundedSender<OutboundMessage>,
        retry_buf: &mut VecDeque<OutboundMessage>,
    ) {
        let mut remaining = VecDeque::new();
        let mut need_stream: HashSet<PeerId> = HashSet::new();
        while let Some(outbound) = retry_buf.pop_front() {
            let handle = {
                let reg = registry.read().unwrap();
                reg.get(&outbound.peer).map(|h| (h.writer.clone(), h.next_seq.clone(), h.poisoned.clone()))
            };
            if let Some((writer, next_seq, poisoned)) = handle {
                if poisoned.load(Ordering::Relaxed) {
                    remaining.push_back(outbound);
                    continue;
                }
                let peer = outbound.peer;
                let seq_id = next_seq.fetch_add(1, Ordering::Relaxed);
                if let Some(ack_tx) = outbound.ack_tx {
                    if let Some(acks) = ack_registry.read().unwrap().get(&peer) {
                        acks.lock().unwrap().insert(seq_id, ack_tx);
                    }
                }
                let request = outbound.request;
                let wf_tx = write_fail_tx.clone();
                let retry_tx = write_retry_tx.clone();
                let reg = registry.clone();
                let ack_reg = ack_registry.clone();
                tokio::spawn(async move {
                    if poisoned.load(Ordering::Relaxed) { return; }
                    let mut w = writer.lock().await;
                    if let Err(e) = write_request_frame(&mut *w, seq_id, &request).await {
                        warn!("[stream_mgr.rs] Outbound write to {} failed: {}", peer, e);
                        poisoned.store(true, Ordering::Relaxed);
                        drop(w);
                        reg.write().unwrap().remove(&peer);
                        // Recover ack_tx from registry so it can be retried
                        let recovered_ack = ack_reg.read().unwrap()
                            .get(&peer)
                            .and_then(|acks| acks.lock().unwrap().remove(&seq_id));
                        let _ = wf_tx.send(peer);
                        let _ = retry_tx.send(OutboundMessage { peer, request, ack_tx: recovered_ack });
                    }
                });
            } else {
                need_stream.insert(outbound.peer);
                remaining.push_back(outbound);
            }
        }
        *retry_buf = remaining;
        for peer in need_stream { let _ = need_stream_tx.send(peer); }
    }

    /// Reader loop for a peer's inbound stream.
    async fn reader_loop(
        peer: PeerId,
        mut stream: libp2p::Stream,
        pending_acks: PendingAcks,
        inbound_tx: mpsc::Sender<InboundMessage>,
    ) {
        loop {
            match read_frame(&mut stream).await {
                Ok(StreamFrame::Request { seq_id, request }) => {
                    info!("[stream_mgr.rs] Inbound request from {} seq={}: {:?}", peer, seq_id, std::mem::discriminant(&request));
                    match inbound_tx.try_send(InboundMessage { peer, seq_id, request }) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!("[stream_mgr.rs] Inbound channel full for {} — dropping message seq={}", peer, seq_id);
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => break,
                    }
                }
                Ok(StreamFrame::Response { seq_id, response }) => {
                    info!("[stream_mgr.rs] Inbound response from {} seq={}: {:?}", peer, seq_id, std::mem::discriminant(&response));
                    let sender = pending_acks.lock().unwrap().remove(&seq_id);
                    if let Some(tx) = sender {
                        info!("[stream_mgr.rs] Dispatching response from {} seq={} to waiting ack channel", peer, seq_id);
                        let _ = tx.send(response);
                    } else {
                        warn!("[stream_mgr.rs] Response from {} for unknown seq_id={} (no ack channel found)", peer, seq_id);
                    }
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        debug!("Inbound from {} closed (EOF)", peer);
                    } else {
                        warn!("[stream_mgr.rs] Inbound read error from {}: {}", peer, e);
                    }
                    break;
                }
            }
        }
        debug!("Reader loop ended for peer {}", peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager() -> (StreamManager, mpsc::Receiver<InboundMessage>, mpsc::Sender<OutboundMessage>) {
        let behaviour = libp2p_stream::Behaviour::new();
        let control = behaviour.new_control();
        StreamManager::new(control)
    }

    #[tokio::test]
    async fn test_initial_state() {
        let (mgr, _, _) = make_manager();
        assert_eq!(mgr.stream_count(), 0);
        assert!(!mgr.has_stream(&PeerId::random()));
    }

    #[tokio::test]
    async fn test_on_peer_disconnected() {
        let (mut mgr, _, _) = make_manager();
        let peer = PeerId::random();
        mgr.ensure_opening(peer);
        mgr.on_peer_disconnected(&peer);
        assert!(!mgr.has_stream(&peer));
    }
}
