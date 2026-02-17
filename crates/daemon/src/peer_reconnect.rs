//! Peer reconnection with exponential backoff.
//!
//! Tracks disconnected peers and attempts to reconnect with increasing delays.
//! Wired into the `ConnectionClosed` handler in service.rs.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use libp2p::{Multiaddr, PeerId};
use tracing::{debug, info, warn};

/// Initial backoff delay before first reconnection attempt.
const INITIAL_BACKOFF: Duration = Duration::from_secs(5);

/// Maximum backoff delay between reconnection attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(300); // 5 minutes

/// Maximum number of reconnection attempts before giving up.
const MAX_ATTEMPTS: u32 = 10;

/// Backoff multiplier (doubles each attempt).
const BACKOFF_MULTIPLIER: u32 = 2;

/// State for a single disconnected peer.
#[derive(Debug)]
struct ReconnectState {
    /// Last-known addresses for this peer.
    addresses: Vec<Multiaddr>,
    /// Number of attempts so far.
    attempts: u32,
    /// Current backoff duration.
    current_backoff: Duration,
    /// When the next reconnection attempt should happen.
    next_attempt_at: Instant,
}

/// Manages reconnection attempts for disconnected peers.
pub struct PeerReconnector {
    peers: HashMap<PeerId, ReconnectState>,
}

impl PeerReconnector {
    /// Create a new reconnector.
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }

    /// Track a disconnected peer for reconnection.
    /// If already tracked, this is a no-op (preserves existing backoff state).
    pub fn track_disconnected(&mut self, peer_id: PeerId, addresses: Vec<Multiaddr>) {
        if self.peers.contains_key(&peer_id) {
            return; // Already tracking
        }
        if addresses.is_empty() {
            debug!("PeerReconnector: no addresses for {}, skipping", peer_id);
            return;
        }
        info!(
            "PeerReconnector: tracking {} for reconnection ({} addresses)",
            peer_id,
            addresses.len()
        );
        self.peers.insert(
            peer_id,
            ReconnectState {
                addresses,
                attempts: 0,
                current_backoff: INITIAL_BACKOFF,
                next_attempt_at: Instant::now() + INITIAL_BACKOFF,
            },
        );
    }

    /// Called when a peer successfully reconnects. Removes from tracking.
    pub fn on_reconnected(&mut self, peer_id: &PeerId) {
        if self.peers.remove(peer_id).is_some() {
            info!("PeerReconnector: {} reconnected, resetting backoff", peer_id);
        }
    }

    /// Get peers that are due for a reconnection attempt right now.
    /// Returns peer IDs and their addresses. Advances backoff state.
    pub fn peers_due_for_reconnect(&mut self) -> Vec<(PeerId, Vec<Multiaddr>)> {
        let now = Instant::now();
        let mut due = Vec::new();
        let mut expired = Vec::new();

        for (peer_id, state) in self.peers.iter_mut() {
            if now >= state.next_attempt_at {
                state.attempts += 1;
                if state.attempts > MAX_ATTEMPTS {
                    warn!(
                        "PeerReconnector: giving up on {} after {} attempts",
                        peer_id, MAX_ATTEMPTS
                    );
                    expired.push(*peer_id);
                    continue;
                }
                debug!(
                    "PeerReconnector: attempt {}/{} for {} (backoff {:?})",
                    state.attempts, MAX_ATTEMPTS, peer_id, state.current_backoff
                );
                due.push((*peer_id, state.addresses.clone()));

                // Advance backoff
                state.current_backoff = Duration::from_secs(
                    (state.current_backoff.as_secs() * BACKOFF_MULTIPLIER as u64)
                        .min(MAX_BACKOFF.as_secs()),
                );
                state.next_attempt_at = now + state.current_backoff;
            }
        }

        for peer_id in expired {
            self.peers.remove(&peer_id);
        }

        due
    }

    /// Number of peers currently tracked for reconnection.
    pub fn tracked_count(&self) -> usize {
        self.peers.len()
    }

    /// Remove a peer from reconnection tracking (e.g., if we no longer care).
    #[allow(dead_code)]
    pub fn remove(&mut self, peer_id: &PeerId) {
        self.peers.remove(peer_id);
    }
}

impl Default for PeerReconnector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_peer() -> PeerId {
        PeerId::random()
    }

    fn test_addr() -> Multiaddr {
        "/ip4/127.0.0.1/tcp/9000".parse().unwrap()
    }

    #[test]
    fn test_track_and_reconnect() {
        let mut reconnector = PeerReconnector::new();
        let peer = test_peer();
        reconnector.track_disconnected(peer, vec![test_addr()]);
        assert_eq!(reconnector.tracked_count(), 1);

        // Not due yet (initial backoff is 5s)
        let due = reconnector.peers_due_for_reconnect();
        assert!(due.is_empty());

        // Simulate time passing
        reconnector.peers.get_mut(&peer).unwrap().next_attempt_at = Instant::now();
        let due = reconnector.peers_due_for_reconnect();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, peer);
    }

    #[test]
    fn test_on_reconnected_removes() {
        let mut reconnector = PeerReconnector::new();
        let peer = test_peer();
        reconnector.track_disconnected(peer, vec![test_addr()]);
        assert_eq!(reconnector.tracked_count(), 1);

        reconnector.on_reconnected(&peer);
        assert_eq!(reconnector.tracked_count(), 0);
    }

    #[test]
    fn test_max_attempts_gives_up() {
        let mut reconnector = PeerReconnector::new();
        let peer = test_peer();
        reconnector.track_disconnected(peer, vec![test_addr()]);

        // Simulate MAX_ATTEMPTS + 1 attempts
        for _ in 0..=MAX_ATTEMPTS {
            reconnector.peers.get_mut(&peer).map(|s| {
                s.next_attempt_at = Instant::now();
            });
            reconnector.peers_due_for_reconnect();
        }

        assert_eq!(reconnector.tracked_count(), 0);
    }

    #[test]
    fn test_backoff_doubles() {
        let mut reconnector = PeerReconnector::new();
        let peer = test_peer();
        reconnector.track_disconnected(peer, vec![test_addr()]);

        // First attempt
        reconnector.peers.get_mut(&peer).unwrap().next_attempt_at = Instant::now();
        reconnector.peers_due_for_reconnect();
        let backoff1 = reconnector.peers.get(&peer).unwrap().current_backoff;

        // Second attempt
        reconnector.peers.get_mut(&peer).unwrap().next_attempt_at = Instant::now();
        reconnector.peers_due_for_reconnect();
        let backoff2 = reconnector.peers.get(&peer).unwrap().current_backoff;

        assert_eq!(backoff2, backoff1 * BACKOFF_MULTIPLIER);
    }

    #[test]
    fn test_empty_addresses_not_tracked() {
        let mut reconnector = PeerReconnector::new();
        reconnector.track_disconnected(test_peer(), vec![]);
        assert_eq!(reconnector.tracked_count(), 0);
    }

    #[test]
    fn test_duplicate_track_is_noop() {
        let mut reconnector = PeerReconnector::new();
        let peer = test_peer();
        reconnector.track_disconnected(peer, vec![test_addr()]);

        // Simulate some backoff advancement
        reconnector.peers.get_mut(&peer).unwrap().attempts = 3;

        // Re-track should not reset
        reconnector.track_disconnected(peer, vec![test_addr()]);
        assert_eq!(reconnector.peers.get(&peer).unwrap().attempts, 3);
    }
}
