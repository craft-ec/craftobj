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
    DiscoveryStatus {
        total_peers: usize,
        storage_peers: usize,
        action: String,
    },

    // -- Announcements --
    CapabilityAnnounced { peer_id: String, capabilities: Vec<String>, storage_committed: u64, storage_used: u64 },
    CapabilityPublished { capabilities: Vec<String>, storage_committed: u64, storage_used: u64 },
    ProviderAnnounced { content_id: String },
    ContentReannounced { content_id: String },

    // -- Gossip --
    StorageReceiptReceived { content_id: String, storage_node: String },
    RemovalNoticeReceived { content_id: String, creator: String, valid: bool },

    // -- Actions (user-initiated) --
    ContentPublished { content_id: String, size: u64, segments: usize, pieces_per_segment: usize },
    AccessGranted { content_id: String, recipient: String },
    AccessRevoked { content_id: String, recipient: String },
    ChannelOpened { channel_id: String, receiver: String, amount: u64 },
    ChannelClosed { channel_id: String },
    PoolFunded { creator: String, amount: u64 },
    RemovalPublished { content_id: String },

    // -- DHT results --
    ProvidersResolved { content_id: String, count: usize },
    ManifestRetrieved { content_id: String, segments: usize },
    DhtError { content_id: String, error: String, next_action: String },

    // -- Content status --
    ContentStatus {
        content_id: String,
        name: String,
        size: u64,
        stage: String,
        local_pieces: usize,
        remote_pieces: usize,
        total_pieces: usize,
        provider_count: usize,
        summary: String,
    },

    // -- Distribution --
    ContentDistributed { content_id: String, pieces_pushed: usize, total_pieces: usize, target_peers: usize },
    DistributionProgress { content_id: String, pieces_pushed: usize, total_pieces: usize, peers_active: usize },
    DistributionSkipped { reason: String, retry_secs: u64 },

    // -- Maintenance --
    MaintenanceCycleStarted { content_count: usize, needs_announce: usize, needs_distribute: usize },
    MaintenanceCycleCompleted { announced: usize, distributed: usize, next_run_secs: u64 },

    // -- PDP --
    ChallengerRoundCompleted { rounds: u32 },
    PieceRequested { content_id: String, peer_id: String, segment: u32 },

    // -- Eviction / Retirement --
    ContentEvicted { content_id: String, reason: String },
    ContentRetired { content_id: String, reason: String },
    GcCompleted { deleted_count: u64, deleted_bytes: u64 },

    // -- Content Health --
    ContentDegraded { content_id: String, health: f64, providers: usize },
    ContentCritical { content_id: String, health: f64, providers: usize },

    // -- Disk Space --
    DiskSpaceWarning { used_bytes: u64, total_bytes: u64, percent: f64 },

    // -- Peer lifecycle --
    PeerGoingOffline { peer_id: String },
    PeerHeartbeatTimeout { peer_id: String },

    // -- Transfer errors --
    TransferError { content_id: String, peer_id: String, error_type: String, message: String },

    // -- Storage pressure --
    StoragePressure { available_bytes: u64, threshold_bytes: u64 },

    // -- Aggregation --
    AggregationComplete { receipt_count: usize, merkle_root: String },
}

pub type EventSender = broadcast::Sender<DaemonEvent>;
pub type EventReceiver = broadcast::Receiver<DaemonEvent>;

pub fn event_channel(capacity: usize) -> (EventSender, EventReceiver) {
    broadcast::channel(capacity)
}
