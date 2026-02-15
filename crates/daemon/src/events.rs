//! Daemon event broadcast system
//!
//! Provides a typed event enum and broadcast channel for pushing real-time
//! events to connected WebSocket clients (CraftStudio dashboard, etc.).

use serde::Serialize;
use tokio::sync::broadcast;

/// Events emitted by the daemon for real-time UI updates.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum DaemonEvent {
    // -- Global / Network events --
    PeerConnected { peer_id: String },
    PeerDisconnected { peer_id: String },
    PeerDiscovered { peer_id: String, address: String },
    DaemonStarted,
    ListeningOn { address: String },

    // -- Announcements --
    CapabilityAnnounced { peer_id: String, capabilities: Vec<String> },
    CapabilityPublished { capabilities: Vec<String> },
    ProviderAnnounced { content_id: String },
    ContentReannounced { content_id: String },

    // -- Gossip --
    StorageReceiptReceived { content_id: String, storage_node: String },
    RemovalNoticeReceived { content_id: String, creator: String, valid: bool },

    // -- Actions (user-initiated) --
    ContentPublished { content_id: String, size: u64, chunks: u32 },
    AccessGranted { content_id: String, recipient: String },
    AccessRevoked { content_id: String, recipient: String },
    ChannelOpened { channel_id: String, receiver: String, amount: u64 },
    ChannelClosed { channel_id: String },
    PoolFunded { creator: String, amount: u64 },
    RemovalPublished { content_id: String },

    // -- DHT results --
    ProvidersResolved { content_id: String, count: usize },
    ManifestRetrieved { content_id: String, chunks: u32 },
    DhtError { content_id: String, error: String },

    // -- PDP --
    ChallengerRoundCompleted { rounds: u32 },
    ShardRequested { content_id: String, peer_id: String, chunk: u32, shard: u32 },
}

pub type EventSender = broadcast::Sender<DaemonEvent>;
pub type EventReceiver = broadcast::Receiver<DaemonEvent>;

pub fn event_channel(capacity: usize) -> (EventSender, EventReceiver) {
    broadcast::channel(capacity)
}
