//! Stream pool for persistent outbound streams per peer.
//!
//! Adapted from TunnelCraft's StreamManager. DataCraft opens a new libp2p stream
//! for every outbound request which causes timeouts when peers restart (stale
//! connection state). This pool maintains pre-opened streams with background open,
//! cooldown on failure, and dead-stream detection.
//!
//! Unlike TunnelCraft, DataCraft uses request-response (not fire-and-forget),
//! so streams are reused sequentially — the caller holds the lock for the
//! duration of one request-response exchange.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, warn};

use datacraft_core::TRANSFER_PROTOCOL;

/// Cooldown after a failed outbound open before retrying (seconds).
const OPEN_RETRY_COOLDOWN_SECS: u64 = 1;

/// Number of consecutive failures before opening the circuit breaker.
const CIRCUIT_BREAKER_THRESHOLD: u32 = 3;

/// Initial circuit-open duration in seconds.
const CIRCUIT_OPEN_DURATION_SECS: u64 = 60;

/// Maximum circuit-open duration in seconds.
const CIRCUIT_MAX_OPEN_SECS: u64 = 600; // 10 minutes

/// Timeout for background stream opens (seconds).
const OPEN_TIMEOUT_SECS: u64 = 10;

/// Circuit breaker state for a peer.
#[derive(Debug)]
enum CircuitState {
    /// Normal operation — requests allowed.
    Closed {
        consecutive_failures: u32,
    },
    /// Circuit is open — requests blocked until deadline.
    Open {
        until: Instant,
        open_duration_secs: u64,
    },
    /// One probe request allowed to test if the peer recovered.
    HalfOpen {
        open_duration_secs: u64,
    },
}

/// Outbound stream pool — one persistent stream per peer.
pub struct StreamPool {
    control: libp2p_stream::Control,
    streams: HashMap<PeerId, Arc<Mutex<libp2p::Stream>>>,
    opening: HashSet<PeerId>,
    open_result_rx: mpsc::UnboundedReceiver<(PeerId, Result<libp2p::Stream, std::io::Error>)>,
    open_result_tx: mpsc::UnboundedSender<(PeerId, Result<libp2p::Stream, std::io::Error>)>,
    open_cooldown: HashMap<PeerId, Instant>,
    /// Circuit breaker state per peer.
    circuits: HashMap<PeerId, CircuitState>,
}

impl StreamPool {
    /// Create a new stream pool with the given libp2p stream control.
    pub fn new(control: libp2p_stream::Control) -> Self {
        let (open_result_tx, open_result_rx) = mpsc::unbounded_channel();
        Self {
            control,
            streams: HashMap::new(),
            opening: HashSet::new(),
            open_result_rx,
            open_result_tx,
            open_cooldown: HashMap::new(),
            circuits: HashMap::new(),
        }
    }

    /// Get an existing stream to a peer, if one is ready.
    pub fn get_stream(&self, peer: &PeerId) -> Option<Arc<Mutex<libp2p::Stream>>> {
        self.streams.get(peer).cloned()
    }

    /// Check if we have a ready stream to a peer.
    pub fn has_stream(&self, peer: &PeerId) -> bool {
        self.streams.contains_key(peer)
    }

    /// Ensure a stream to this peer is being opened (if not already open or opening).
    pub fn ensure_opening(&mut self, peer: PeerId) {
        if self.streams.contains_key(&peer) {
            return;
        }
        if self.opening.contains(&peer) {
            return;
        }
        // Respect cooldown after failed opens
        if let Some(&deadline) = self.open_cooldown.get(&peer) {
            if Instant::now() < deadline {
                return;
            }
            self.open_cooldown.remove(&peer);
        }
        self.spawn_open(peer);
    }

    /// Spawn a background open_stream task.
    fn spawn_open(&mut self, peer: PeerId) {
        self.opening.insert(peer);
        let mut control = self.control.clone();
        let tx = self.open_result_tx.clone();
        tokio::spawn(async move {
            debug!("StreamPool: opening stream to {} ...", peer);
            match tokio::time::timeout(
                std::time::Duration::from_secs(OPEN_TIMEOUT_SECS),
                control.open_stream(peer, libp2p::StreamProtocol::new(TRANSFER_PROTOCOL)),
            )
            .await
            {
                Ok(Ok(stream)) => {
                    let _ = tx.send((peer, Ok(stream)));
                }
                Ok(Err(e)) => {
                    warn!("StreamPool: open to {} failed: {}", peer, e);
                    let _ = tx.send((
                        peer,
                        Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            format!("open_stream failed: {}", e),
                        )),
                    ));
                }
                Err(_) => {
                    warn!("StreamPool: open to {} timed out ({}s)", peer, OPEN_TIMEOUT_SECS);
                    let _ = tx.send((
                        peer,
                        Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "open_stream timed out",
                        )),
                    ));
                }
            }
        });
    }

    /// Poll for completed background opens. Call from the event loop.
    /// Returns the number of newly opened streams.
    pub fn poll(&mut self) -> usize {
        let mut opened = 0;
        while let Ok((peer, result)) = self.open_result_rx.try_recv() {
            self.opening.remove(&peer);
            match result {
                Ok(stream) => {
                    if self.streams.contains_key(&peer) {
                        debug!("StreamPool: stream to {} ready but already have one — dropping", peer);
                        continue;
                    }
                    debug!("StreamPool: opened stream to {}", peer);
                    self.streams.insert(peer, Arc::new(Mutex::new(stream)));
                    opened += 1;
                }
                Err(e) => {
                    debug!("StreamPool: open to {} failed: {}", peer, e);
                    self.open_cooldown.insert(
                        peer,
                        Instant::now()
                            + std::time::Duration::from_secs(OPEN_RETRY_COOLDOWN_SECS),
                    );
                }
            }
        }
        opened
    }

    /// Mark a stream as dead (write/read failed). Removes it and starts re-open.
    pub fn mark_dead(&mut self, peer: &PeerId) {
        if self.streams.remove(peer).is_some() {
            debug!("StreamPool: marked stream to {} as dead", peer);
        }
        self.ensure_opening(*peer);
    }

    /// Check if a peer's circuit breaker allows requests.
    /// Also transitions Open → HalfOpen when the deadline passes.
    pub fn is_circuit_open(&mut self, peer: &PeerId) -> bool {
        let state = match self.circuits.get_mut(peer) {
            Some(s) => s,
            None => return false,
        };
        match state {
            CircuitState::Closed { .. } => false,
            CircuitState::Open { until, open_duration_secs } => {
                if Instant::now() >= *until {
                    // Transition to half-open: allow one probe
                    debug!("StreamPool: circuit for {} transitioning to half-open", peer);
                    let dur = *open_duration_secs;
                    *state = CircuitState::HalfOpen { open_duration_secs: dur };
                    false // Allow the probe
                } else {
                    true // Still blocked
                }
            }
            CircuitState::HalfOpen { .. } => false, // Allow probe
        }
    }

    /// Record a successful request to a peer (closes circuit breaker).
    pub fn record_success(&mut self, peer: &PeerId) {
        self.circuits.insert(*peer, CircuitState::Closed { consecutive_failures: 0 });
    }

    /// Record a failed request to a peer (may open circuit breaker).
    pub fn record_failure(&mut self, peer: &PeerId) {
        let state = self.circuits.entry(*peer).or_insert(CircuitState::Closed { consecutive_failures: 0 });
        match state {
            CircuitState::Closed { consecutive_failures } => {
                *consecutive_failures += 1;
                if *consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD {
                    let dur = CIRCUIT_OPEN_DURATION_SECS;
                    warn!(
                        "StreamPool: circuit breaker OPEN for {} after {} consecutive failures ({}s)",
                        peer, consecutive_failures, dur
                    );
                    *state = CircuitState::Open {
                        until: Instant::now() + std::time::Duration::from_secs(dur),
                        open_duration_secs: dur,
                    };
                }
            }
            CircuitState::HalfOpen { open_duration_secs } => {
                // Probe failed — double the open duration
                let new_dur = (*open_duration_secs * 2).min(CIRCUIT_MAX_OPEN_SECS);
                warn!(
                    "StreamPool: probe failed for {}, circuit OPEN for {}s",
                    peer, new_dur
                );
                *state = CircuitState::Open {
                    until: Instant::now() + std::time::Duration::from_secs(new_dur),
                    open_duration_secs: new_dur,
                };
            }
            CircuitState::Open { .. } => {
                // Shouldn't happen (requests blocked), but ignore
            }
        }
    }

    /// Peer disconnected — clean up everything.
    pub fn on_peer_disconnected(&mut self, peer: &PeerId) {
        self.streams.remove(peer);
        self.opening.remove(peer);
        self.open_cooldown.remove(peer);
        self.circuits.remove(peer);
        debug!("StreamPool: cleaned up peer {}", peer);
    }

    /// New connection established — clear cooldown so we can open immediately.
    pub fn on_peer_connected(&mut self, peer: &PeerId) {
        self.open_cooldown.remove(peer);
    }

    /// Number of ready streams.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Number of opens in flight.
    pub fn pending_count(&self) -> usize {
        self.opening.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_peer() -> PeerId {
        PeerId::random()
    }

    fn make_pool() -> StreamPool {
        let behaviour = libp2p_stream::Behaviour::new();
        let control = behaviour.new_control();
        StreamPool::new(control)
    }

    #[tokio::test]
    async fn test_initial_state() {
        let pool = make_pool();
        let peer = test_peer();
        assert!(!pool.has_stream(&peer));
        assert_eq!(pool.stream_count(), 0);
        assert_eq!(pool.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_ensure_opening_deduplicates() {
        let mut pool = make_pool();
        let peer = test_peer();

        pool.ensure_opening(peer);
        assert!(pool.opening.contains(&peer));
        assert_eq!(pool.pending_count(), 1);

        // Second call should not add another
        pool.ensure_opening(peer);
        assert_eq!(pool.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_on_peer_disconnected_cleans_up() {
        let mut pool = make_pool();
        let peer = test_peer();

        pool.ensure_opening(peer);
        assert!(pool.opening.contains(&peer));

        pool.on_peer_disconnected(&peer);
        assert!(!pool.opening.contains(&peer));
        assert!(!pool.has_stream(&peer));
    }

    #[tokio::test]
    async fn test_on_peer_connected_clears_cooldown() {
        let mut pool = make_pool();
        let peer = test_peer();

        pool.open_cooldown.insert(
            peer,
            Instant::now() + std::time::Duration::from_secs(60),
        );
        assert!(pool.open_cooldown.contains_key(&peer));

        pool.on_peer_connected(&peer);
        assert!(!pool.open_cooldown.contains_key(&peer));
    }

    #[tokio::test]
    async fn test_get_stream_returns_none_initially() {
        let pool = make_pool();
        assert!(pool.get_stream(&test_peer()).is_none());
    }

    #[tokio::test]
    async fn test_mark_dead_removes_and_reopens() {
        let mut pool = make_pool();
        let peer = test_peer();

        // No stream to mark dead — should still trigger opening
        pool.mark_dead(&peer);
        assert!(pool.opening.contains(&peer));
    }
}
