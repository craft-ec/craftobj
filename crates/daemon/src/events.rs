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
    ProviderAnnounced { content_id: String },
    ContentReannounced { content_id: String },

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
    // -- Maintenance --
    MaintenanceCycleStarted { content_count: usize, needs_announce: usize, needs_distribute: usize },
    MaintenanceCycleCompleted { announced: usize, distributed: usize, next_run_secs: u64 },

    // -- Scaling / equalization --
    /// Emitted when demand-gated equalization pushes pieces to a new provider.
    ScalingPush { content_id: String, pieces_pushed: usize, new_providers: usize },

    // -- PDP --
    ChallengerRoundCompleted { rounds: u32 },

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

    // -- Storage pressure --
    StoragePressure { available_bytes: u64, threshold_bytes: u64 },

    // -- Aggregation --
    AggregationComplete { receipt_count: usize, merkle_root: String },

    // -- Repair --
    /// Emitted when HealthScan starts a repair for a segment (erasure or replication).
    RepairStarted { content_id: String, segment: u32, strategy: String },
    /// Emitted when a repair attempt finishes.
    RepairCompleted { content_id: String, segment: u32, pieces_generated: usize, success: bool },
}

/// Convert a `DaemonEvent` to a JSON-RPC notification string.
///
/// Uses the serde `tag = "type"` field as the notification method name
/// and `content = "data"` as the params. This centralizes the conversion
/// so callers can simply do `let s: String = event.into();`.
impl From<DaemonEvent> for String {
    fn from(event: DaemonEvent) -> String {
        let event_value = serde_json::to_value(&event).unwrap_or_default();
        let method = event_value.get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("event")
            .to_string();
        let params = event_value.get("data").cloned()
            .unwrap_or(serde_json::Value::Null);
        craftec_ipc::event_to_notification(&method, &params)
    }
}

pub type EventSender = broadcast::Sender<DaemonEvent>;
pub type EventReceiver = broadcast::Receiver<DaemonEvent>;

pub fn event_channel(capacity: usize) -> (EventSender, EventReceiver) {
    broadcast::channel(capacity)
}
