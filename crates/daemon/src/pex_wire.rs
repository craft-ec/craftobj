//! PEX Wire Protocol Types
//!
//! Defines the serializable message types for Peer Exchange (PEX) protocol.
//! Uses simple JSON serialization for the initial implementation.
//!
//! The wire protocol is intentionally simple - we serialize peer IDs as raw bytes
//! and multiaddresses as their byte representation to avoid dependency on libp2p
//! types in the serialized format.

use serde::{Deserialize, Serialize};
use libp2p::{PeerId, Multiaddr};
use std::convert::TryFrom;

/// A PEX message containing a list of peers
/// 
/// This is the top-level message sent between nodes during peer exchange.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PexMessage {
    /// List of peers being shared
    pub peers: Vec<PexPeer>,
}

/// A single peer entry in a PEX message
/// 
/// Contains the peer's ID and known addresses in serializable form.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PexPeer {
    /// Peer ID as raw bytes (to avoid libp2p types in serialization)
    pub peer_id_bytes: Vec<u8>,
    
    /// List of multiaddresses as raw bytes
    pub addrs: Vec<Vec<u8>>,
}

impl PexMessage {
    /// Create a new empty PEX message
    pub fn new() -> Self {
        Self {
            peers: Vec::new(),
        }
    }

    /// Create a PEX message from a list of (PeerId, Vec<Multiaddr>) pairs
    pub fn from_peers(peers: Vec<(PeerId, Vec<Multiaddr>)>) -> Self {
        let pex_peers = peers.into_iter()
            .map(|(peer_id, addrs)| PexPeer::from_peer(peer_id, addrs))
            .collect();
            
        Self {
            peers: pex_peers,
        }
    }

    /// Convert this PEX message back to (PeerId, Vec<Multiaddr>) pairs
    /// 
    /// Returns an error if any peer ID or address cannot be parsed.
    pub fn to_peers(&self) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, PexWireError> {
        self.peers.iter()
            .map(|peer| peer.to_peer())
            .collect()
    }

    /// Serialize this message to bytes using JSON
    pub fn to_bytes(&self) -> Result<Vec<u8>, PexWireError> {
        serde_json::to_vec(self)
            .map_err(|e| PexWireError::SerializationError(e.to_string()))
    }

    /// Deserialize a PEX message from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, PexWireError> {
        serde_json::from_slice(data)
            .map_err(|e| PexWireError::DeserializationError(e.to_string()))
    }
}

impl PexPeer {
    /// Create a PexPeer from libp2p types
    pub fn from_peer(peer_id: PeerId, addrs: Vec<Multiaddr>) -> Self {
        Self {
            peer_id_bytes: peer_id.to_bytes(),
            addrs: addrs.into_iter().map(|addr| addr.to_vec()).collect(),
        }
    }

    /// Convert this PexPeer back to libp2p types
    pub fn to_peer(&self) -> Result<(PeerId, Vec<Multiaddr>), PexWireError> {
        // Parse peer ID
        let peer_id = PeerId::from_bytes(&self.peer_id_bytes)
            .map_err(|e| PexWireError::InvalidPeerId(e.to_string()))?;

        // Parse addresses
        let mut addrs = Vec::new();
        for addr_bytes in &self.addrs {
            let addr = Multiaddr::try_from(addr_bytes.clone())
                .map_err(|e| PexWireError::InvalidAddress(e.to_string()))?;
            addrs.push(addr);
        }

        Ok((peer_id, addrs))
    }
}

impl Default for PexMessage {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur during PEX wire protocol operations
#[derive(Debug, Clone, thiserror::Error)]
pub enum PexWireError {
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    
    #[error("Invalid peer ID: {0}")]
    InvalidPeerId(String),
    
    #[error("Invalid multiaddress: {0}")]
    InvalidAddress(String),
}

// TODO: Integration notes for service.rs and protocol handling:
//
// 1. **Outgoing PEX messages** (in service.rs event loop):
//    ```rust
//    if pex_manager.should_send(&peer_id) {
//        let peers_to_share = pex_manager.peers_to_share(&peer_id);
//        let pex_msg = PexMessage::from_peers(peers_to_share);
//        let bytes = pex_msg.to_bytes()?;
//        
//        // Send via libp2p stream (similar to existing piece transfer protocol)
//        // Use same pattern as StreamManager for reliable delivery
//        stream_manager.send_pex(peer_id, bytes).await?;
//        
//        pex_manager.mark_sent(&peer_id);
//    }
//    ```
//
// 2. **Incoming PEX messages** (in protocol handler):
//    ```rust
//    // In stream protocol handler when receiving PEX message type
//    let pex_msg = PexMessage::from_bytes(&received_bytes)?;
//    let peers = pex_msg.to_peers()?;
//    
//    let new_peers = pex_manager.receive_pex(&from_peer, peers);
//    
//    // Attempt connections to new peers
//    for (peer_id, addrs) in new_peers {
//        // Use same dial pattern as existing connection attempts
//        let dial_opts = DialOpts::peer_id(peer_id)
//            .addresses(addrs)
//            .build();
//        swarm.dial(dial_opts)?;
//    }
//    ```
//
// 3. **Protocol identification**:
//    - Add PEX message type to existing message enum (likely in protocol.rs)
//    - Use same stream multiplexing as piece transfer
//    - Consider using a dedicated protocol string like "/craftobj/pex/1.0.0"
//
// 4. **Error handling**:
//    - Log PEX wire errors but don't fail the connection
//    - Invalid peer information should be ignored, not crash the node
//    - Malformed messages should be logged for debugging but not propagated

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::{PeerId, Multiaddr};

    fn create_test_peer_id() -> PeerId {
        let keypair = libp2p::identity::Keypair::generate_ed25519();
        PeerId::from(keypair.public())
    }

    fn create_test_multiaddr() -> Multiaddr {
        "/ip4/192.168.1.100/tcp/4001".parse().unwrap()
    }

    #[test]
    fn test_pex_peer_round_trip() {
        let peer_id = create_test_peer_id();
        let addrs = vec![
            create_test_multiaddr(),
            "/ip6/::1/tcp/4001".parse().unwrap(),
        ];

        let pex_peer = PexPeer::from_peer(peer_id, addrs.clone());
        let (decoded_peer_id, decoded_addrs) = pex_peer.to_peer().unwrap();

        assert_eq!(decoded_peer_id, peer_id);
        assert_eq!(decoded_addrs, addrs);
    }

    #[test]
    fn test_pex_message_serialization() {
        let peer1 = create_test_peer_id();
        let peer2 = create_test_peer_id();
        let addr1 = create_test_multiaddr();
        let addr2: Multiaddr = "/ip4/10.0.0.1/tcp/8080".parse().unwrap();

        let peers = vec![
            (peer1, vec![addr1.clone()]),
            (peer2, vec![addr2.clone(), addr1]),
        ];

        let message = PexMessage::from_peers(peers.clone());
        
        // Test serialization to bytes
        let bytes = message.to_bytes().unwrap();
        assert!(!bytes.is_empty());

        // Test deserialization
        let decoded_message = PexMessage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded_message, message);

        // Test conversion back to peers
        let decoded_peers = decoded_message.to_peers().unwrap();
        assert_eq!(decoded_peers.len(), 2);
        
        // Verify the peer data matches
        assert_eq!(decoded_peers[0].0, peer1);
        assert_eq!(decoded_peers[1].0, peer2);
    }

    #[test] 
    fn test_empty_pex_message() {
        let empty_msg = PexMessage::new();
        assert!(empty_msg.peers.is_empty());

        let bytes = empty_msg.to_bytes().unwrap();
        let decoded = PexMessage::from_bytes(&bytes).unwrap();
        
        assert_eq!(decoded.peers.len(), 0);
    }

    #[test]
    fn test_pex_message_from_empty_peers() {
        let message = PexMessage::from_peers(vec![]);
        assert!(message.peers.is_empty());
        
        let peers = message.to_peers().unwrap();
        assert!(peers.is_empty());
    }

    #[test]
    fn test_invalid_deserialization() {
        let invalid_json = b"invalid json data";
        let result = PexMessage::from_bytes(invalid_json);
        
        assert!(result.is_err());
        match result.err().unwrap() {
            PexWireError::DeserializationError(_) => {},
            _ => panic!("Expected DeserializationError"),
        }
    }

    #[test]
    fn test_peer_with_no_addresses() {
        let peer_id = create_test_peer_id();
        let empty_addrs = vec![];
        
        let pex_peer = PexPeer::from_peer(peer_id, empty_addrs.clone());
        let (decoded_peer_id, decoded_addrs) = pex_peer.to_peer().unwrap();
        
        assert_eq!(decoded_peer_id, peer_id);
        assert_eq!(decoded_addrs, empty_addrs);
    }

    #[test]
    fn test_message_size_reasonable() {
        // Test that PEX messages don't become unreasonably large
        let mut peers = vec![];
        
        // Create 50 peers with 3 addresses each (larger than typical max_share of 20)
        for _ in 0..50 {
            let peer_id = create_test_peer_id();
            let addrs = vec![
                create_test_multiaddr(),
                "/ip6/::1/tcp/4001".parse().unwrap(),
                "/ip4/172.16.0.1/tcp/9000".parse().unwrap(),
            ];
            peers.push((peer_id, addrs));
        }
        
        let message = PexMessage::from_peers(peers);
        let bytes = message.to_bytes().unwrap();
        
        // With 50 peers × 3 addresses each, should still be reasonable size
        // Each peer ID is ~38 bytes, each address ~20-30 bytes
        // So roughly 50 × (38 + 3×25) = ~6KB, which is reasonable for PEX
        println!("PEX message size with 50 peers: {} bytes", bytes.len());
        assert!(bytes.len() < 50_000); // Should be well under 50KB
    }
}