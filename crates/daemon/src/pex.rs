//! Peer Exchange (PEX) Protocol
//!
//! Implements BitTorrent-style peer exchange where nodes share their known peer lists
//! with connected peers to reduce reliance on bootstrap nodes and DHT for peer discovery.
//!
//! ## Design
//! - Periodic exchange (every 60s by default)
//! - Each node sends its N best peers (by score) to connected peers
//! - Receiving nodes merge peers into local table and attempt connections
//! - Simple and proven at 10M+ nodes in BitTorrent

use std::collections::HashMap;
use std::time::{Duration, Instant};
use libp2p::{PeerId, Multiaddr};

/// Manages peer exchange for a CraftOBJ node
/// 
/// Tracks known peers and coordinates periodic exchange of peer information
/// with connected nodes to improve network connectivity and reduce bootstrap dependency.
pub struct PexManager {
    /// Known peers with their addresses
    known_peers: HashMap<PeerId, Vec<Multiaddr>>,
    
    /// When we last sent PEX to each peer
    last_sent: HashMap<PeerId, Instant>,
    
    /// Interval between PEX exchanges
    interval: Duration,
    
    /// Max peers to share per exchange
    max_share: usize,
}

impl PexManager {
    /// Create a new PEX manager with default settings
    pub fn new() -> Self {
        Self {
            known_peers: HashMap::new(),
            last_sent: HashMap::new(),
            interval: Duration::from_secs(60), // 60 seconds default
            max_share: 20, // Share up to 20 peers per exchange
        }
    }

    /// Create a new PEX manager with custom settings
    pub fn with_config(interval: Duration, max_share: usize) -> Self {
        Self {
            known_peers: HashMap::new(),
            last_sent: HashMap::new(),
            interval,
            max_share,
        }
    }

    /// Record a peer we've seen (from connections, DHT, etc.)
    /// 
    /// This should be called whenever we discover a peer through any means:
    /// - Direct connections
    /// - DHT queries
    /// - Bootstrap nodes
    /// - Other PEX exchanges
    pub fn add_peer(&mut self, peer_id: PeerId, addrs: Vec<Multiaddr>) {
        if addrs.is_empty() {
            return;
        }
        
        self.known_peers.insert(peer_id, addrs);
    }

    /// Remove a peer (e.g., when it goes offline or becomes unreachable)
    pub fn remove_peer(&mut self, peer_id: &PeerId) {
        self.known_peers.remove(peer_id);
        self.last_sent.remove(peer_id);
    }

    /// Get peers to share with a given peer (exclude the peer itself)
    /// 
    /// Returns up to `max_share` peers, excluding the requesting peer.
    /// In a full implementation, this would prioritize by peer score/quality.
    /// For now, we use a simple approach that takes the first N peers.
    pub fn peers_to_share(&self, exclude: &PeerId) -> Vec<(PeerId, Vec<Multiaddr>)> {
        self.known_peers
            .iter()
            .filter(|(peer_id, _)| *peer_id != exclude)
            .take(self.max_share)
            .map(|(peer_id, addrs)| (*peer_id, addrs.clone()))
            .collect()
    }

    /// Process received PEX data — returns new peers we should try connecting to
    /// 
    /// Filters out peers we already know and returns genuinely new peers
    /// that we should attempt to connect to.
    pub fn receive_pex(
        &mut self, 
        from: &PeerId, 
        peers: Vec<(PeerId, Vec<Multiaddr>)>
    ) -> Vec<(PeerId, Vec<Multiaddr>)> {
        let mut new_peers = Vec::new();
        
        for (peer_id, addrs) in peers {
            // Skip empty address lists
            if addrs.is_empty() {
                continue;
            }
            
            // Check if this is a genuinely new peer
            if !self.known_peers.contains_key(&peer_id) {
                new_peers.push((peer_id, addrs.clone()));
            }
            
            // Add to our known peers regardless (updates addresses)
            self.add_peer(peer_id, addrs);
        }
        
        new_peers
    }

    /// Check if it's time to send PEX to a peer
    /// 
    /// Returns true if we haven't sent PEX to this peer recently
    /// or if we've never sent to them before.
    pub fn should_send(&self, peer: &PeerId) -> bool {
        match self.last_sent.get(peer) {
            Some(last_time) => last_time.elapsed() >= self.interval,
            None => true, // Never sent to this peer
        }
    }

    /// Mark PEX as sent to peer (updates timestamp)
    pub fn mark_sent(&mut self, peer: &PeerId) {
        self.last_sent.insert(*peer, Instant::now());
    }

    /// Get total number of known peers
    pub fn peer_count(&self) -> usize {
        self.known_peers.len()
    }

    /// Get the configured exchange interval
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Get the configured max peers to share
    pub fn max_share(&self) -> usize {
        self.max_share
    }

    /// Clean up old peers that haven't been seen recently
    /// 
    /// This should be called periodically to prevent the peer table from growing unbounded.
    /// In a full implementation, this would consider peer scores and last-seen timestamps.
    pub fn cleanup_stale_peers(&mut self, max_age: Duration) {
        // For now, just clean up peers we haven't sent PEX to in a while
        let cutoff = Instant::now() - max_age;
        
        let stale_peers: Vec<PeerId> = self.last_sent
            .iter()
            .filter(|(_, &timestamp)| timestamp < cutoff)
            .map(|(peer_id, _)| *peer_id)
            .collect();
            
        for peer_id in stale_peers {
            self.remove_peer(&peer_id);
        }
    }
}

impl Default for PexManager {
    fn default() -> Self {
        Self::new()
    }
}

// TODO: Wire PexManager into service.rs event loop
// 1. Add PexManager field to CraftObjService
// 2. In the event loop, periodically check connected peers:
//    - For each connected peer: if manager.should_send(peer) { send PEX message }
//    - Use peers_to_share(peer) to get the peer list
//    - Serialize using PexMessage and send via libp2p stream
//    - Call manager.mark_sent(peer) after sending
//
// TODO: Handle connection events
// 1. On connection established: manager.add_peer(peer_id, addrs)
// 2. On connection closed: manager.remove_peer(peer_id)
// 3. When discovering peers via DHT: manager.add_peer(peer_id, addrs)
//
// TODO: Handle incoming PEX messages
// 1. Deserialize PexMessage from incoming stream
// 2. Call manager.receive_pex(from_peer, peers)
// 3. For each new peer returned: attempt connection using libp2p::swarm::dial_opts
//
// TODO: Periodic cleanup
// 1. In service.rs event loop, periodically call manager.cleanup_stale_peers()
// 2. Suggested cleanup interval: every 10 minutes
// 3. Suggested max age: 24 hours (peers not communicated with in 24h are removed)

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::{PeerId, Multiaddr};
    use std::time::Duration;

    fn create_test_peer_id() -> PeerId {
        // Generate a test peer ID
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        PeerId::from(keypair.public())
    }

    fn create_test_multiaddr() -> Multiaddr {
        "/ip4/127.0.0.1/tcp/8080".parse().unwrap()
    }

    #[test]
    fn test_add_and_remove_peers() {
        let mut manager = PexManager::new();
        let peer_id = create_test_peer_id();
        let addrs = vec![create_test_multiaddr()];
        
        assert_eq!(manager.peer_count(), 0);
        
        manager.add_peer(peer_id, addrs.clone());
        assert_eq!(manager.peer_count(), 1);
        
        manager.remove_peer(&peer_id);
        assert_eq!(manager.peer_count(), 0);
    }

    #[test]
    fn test_peers_to_share_excludes_self() {
        let mut manager = PexManager::new();
        let peer1 = create_test_peer_id();
        let peer2 = create_test_peer_id();
        let addrs = vec![create_test_multiaddr()];
        
        manager.add_peer(peer1, addrs.clone());
        manager.add_peer(peer2, addrs.clone());
        
        // Should return only peer2, excluding peer1
        let shared = manager.peers_to_share(&peer1);
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].0, peer2);
    }

    #[test]
    fn test_receive_pex_returns_only_new_peers() {
        let mut manager = PexManager::new();
        let known_peer = create_test_peer_id();
        let new_peer = create_test_peer_id();
        let from_peer = create_test_peer_id();
        let addrs = vec![create_test_multiaddr()];
        
        // Add one peer as already known
        manager.add_peer(known_peer, addrs.clone());
        
        // Receive PEX with both known and new peer
        let pex_data = vec![
            (known_peer, addrs.clone()),
            (new_peer, addrs.clone()),
        ];
        
        let new_peers = manager.receive_pex(&from_peer, pex_data);
        
        // Should return only the genuinely new peer
        assert_eq!(new_peers.len(), 1);
        assert_eq!(new_peers[0].0, new_peer);
        
        // But both should now be in known_peers
        assert_eq!(manager.peer_count(), 2);
    }

    #[test]
    fn test_should_send_rate_limiting() {
        let interval = Duration::from_millis(100);
        let mut manager = PexManager::with_config(interval, 20);
        let peer_id = create_test_peer_id();
        
        // Initially should send (never sent before)
        assert!(manager.should_send(&peer_id));
        
        // Mark as sent
        manager.mark_sent(&peer_id);
        
        // Immediately after sending, should not send again
        assert!(!manager.should_send(&peer_id));
        
        // After waiting for the interval, should be able to send again
        std::thread::sleep(Duration::from_millis(150));
        assert!(manager.should_send(&peer_id));
    }

    #[test]
    fn test_pex_message_serialization_round_trip() {
        // This test will verify that PexMessage can be serialized and deserialized correctly
        // It depends on pex_wire.rs which should be implemented alongside this file
        
        use crate::pex_wire::{PexMessage, PexPeer};
        
        let peer1 = create_test_peer_id();
        let peer2 = create_test_peer_id();
        let addr = create_test_multiaddr();
        
        let message = PexMessage {
            peers: vec![
                PexPeer {
                    peer_id_bytes: peer1.to_bytes(),
                    addrs: vec![addr.to_vec()],
                },
                PexPeer {
                    peer_id_bytes: peer2.to_bytes(), 
                    addrs: vec![addr.to_vec()],
                },
            ],
        };
        
        // Test serialization round-trip
        let serialized = serde_json::to_vec(&message).unwrap();
        let deserialized: PexMessage = serde_json::from_slice(&serialized).unwrap();
        
        assert_eq!(deserialized.peers.len(), 2);
        assert_eq!(deserialized.peers[0].peer_id_bytes, peer1.to_bytes());
        assert_eq!(deserialized.peers[1].peer_id_bytes, peer2.to_bytes());
    }

    #[test] 
    fn test_max_share_limit() {
        let max_share = 3;
        let mut manager = PexManager::with_config(Duration::from_secs(60), max_share);
        let exclude_peer = create_test_peer_id();
        let addrs = vec![create_test_multiaddr()];
        
        // Add more peers than max_share limit
        for _ in 0..5 {
            let peer = create_test_peer_id();
            manager.add_peer(peer, addrs.clone());
        }
        
        let shared = manager.peers_to_share(&exclude_peer);
        
        // Should respect max_share limit
        assert!(shared.len() <= max_share);
    }
}