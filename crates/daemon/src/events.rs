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
    PeerConnected { peer_id: String, address: String, total_peers: usize },
    PeerDisconnected { peer_id: String, remaining_peers: usize },
    PeerDiscovered { peer_id: String, address: String },
    DaemonStarted { listen_addresses: Vec<String> },
    ListeningOn { address: String },

    // -- Discovery status --
    /// Periodic summary of network discovery state
    DiscoveryStatus {
        total_peers: usize,
        storage_peers: usize,
        /// What the node is currently doing to find peers
        action: String,
    },

    // -- Announcements --
    CapabilityAnnounced { peer_id: String, capabilities: Vec<String>, storage_committed: u64, storage_used: u64 },
    CapabilityPublished { capabilities: Vec<String> },
    ProviderAnnounced { content_id: String },
    ContentReannounced { content_id: String },

    // -- Gossip --
    StorageReceiptReceived { content_id: String, storage_node: String },
    RemovalNoticeReceived { content_id: String, creator: String, valid: bool },

    // -- Actions (user-initiated) --
    ContentPublished { content_id: String, size: u64, chunks: u32, shards: usize },
    AccessGranted { content_id: String, recipient: String },
    AccessRevoked { content_id: String, recipient: String },
    ChannelOpened { channel_id: String, receiver: String, amount: u64 },
    ChannelClosed { channel_id: String },
    PoolFunded { creator: String, amount: u64 },
    RemovalPublished { content_id: String },

    // -- DHT results --
    ProvidersResolved { content_id: String, count: usize },
    ManifestRetrieved { content_id: String, chunks: u32 },
    DhtError { content_id: String, error: String, /// What happens next
        next_action: String },

    // -- Distribution --
    ContentDistributed { content_id: String, shards_pushed: usize, total_shards: usize, target_peers: usize },
    /// Distribution not possible — explains why and when retry happens
    DistributionSkipped { reason: String, retry_secs: u64 },

    // -- Maintenance --
    MaintenanceCycleStarted { content_count: usize, needs_announce: usize, needs_distribute: usize },
    MaintenanceCycleCompleted { announced: usize, distributed: usize, next_run_secs: u64 },

    // -- PDP --
    ChallengerRoundCompleted { rounds: u32 },
    ShardRequested { content_id: String, peer_id: String, chunk: u32, shard: u32 },
}

pub type EventSender = broadcast::Sender<DaemonEvent>;
pub type EventReceiver = broadcast::Receiver<DaemonEvent>;

pub fn event_channel(capacity: usize) -> (EventSender, EventReceiver) {
    broadcast::channel(capacity)
}
