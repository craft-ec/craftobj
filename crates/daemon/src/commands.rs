//! Daemon command system
//!
//! Commands sent from IPC handler to the swarm event loop for DHT operations
//! and request_response transfers.

use datacraft_core::{ContentId, ContentManifest, DataCraftCapability};
use datacraft_transfer::DataCraftResponse;
use libp2p::PeerId;
use tokio::sync::oneshot;

/// Commands that can be sent to the swarm event loop.
#[derive(Debug)]
pub enum DataCraftCommand {
    /// Announce this node as a provider for a content ID.
    AnnounceProvider {
        content_id: ContentId,
        manifest: ContentManifest,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Resolve providers for a content ID.
    ResolveProviders {
        content_id: ContentId,
        reply_tx: oneshot::Sender<Result<Vec<libp2p::PeerId>, String>>,
    },
    /// Get a manifest from the DHT.
    GetManifest {
        content_id: ContentId,
        reply_tx: oneshot::Sender<Result<ContentManifest, String>>,
    },
    /// Send a PieceSync request to a peer and return the PieceBatch response.
    PieceSync {
        peer_id: PeerId,
        content_id: ContentId,
        segment_index: u32,
        merkle_root: [u8; 32],
        have_pieces: Vec<[u8; 32]>,
        max_pieces: u16,
        reply_tx: oneshot::Sender<Result<DataCraftResponse, String>>,
    },
    /// Publish a capability announcement via gossipsub.
    PublishCapabilities {
        capabilities: Vec<DataCraftCapability>,
        storage_committed_bytes: u64,
        storage_used_bytes: u64,
        storage_root: [u8; 32],
        piece_counts: std::collections::HashMap<String, usize>,
    },
    /// Store a re-encryption key in the DHT for access grant.
    PutReKey {
        content_id: ContentId,
        entry: datacraft_core::pre::ReKeyEntry,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Remove a re-encryption key from the DHT (revoke access).
    RemoveReKey {
        content_id: ContentId,
        recipient_did: [u8; 32],
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Store an access list in the DHT.
    PutAccessList {
        access_list: datacraft_core::access::AccessList,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Fetch an access list from the DHT.
    GetAccessList {
        content_id: ContentId,
        reply_tx: oneshot::Sender<Result<datacraft_core::access::AccessList, String>>,
    },
    /// Publish a removal notice to DHT and gossipsub.
    PublishRemoval {
        content_id: ContentId,
        notice: datacraft_core::RemovalNotice,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Check if content has been removed (check local cache first, then DHT).
    CheckRemoval {
        content_id: ContentId,
        reply_tx: oneshot::Sender<Result<Option<datacraft_core::RemovalNotice>, String>>,
    },
    /// Broadcast a StorageReceipt via gossipsub (for aggregator collection).
    BroadcastStorageReceipt {
        receipt_data: Vec<u8>,
    },
    /// Broadcast a repair message (signal or announcement) via gossipsub.
    BroadcastRepairMessage {
        repair_data: Vec<u8>,
    },
    /// Broadcast a demand signal via gossipsub (scaling).
    BroadcastDemandSignal {
        signal_data: Vec<u8>,
    },
    /// Trigger an immediate distribution cycle (e.g. after content publish or startup import).
    TriggerDistribution,
    /// Push a manifest to a remote storage peer via request_response.
    PushManifest {
        peer_id: PeerId,
        content_id: ContentId,
        manifest_json: Vec<u8>,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Push a piece to a remote storage peer via request_response.
    PushPiece {
        peer_id: PeerId,
        content_id: ContentId,
        segment_index: u32,
        piece_id: [u8; 32],
        coefficients: Vec<u8>,
        piece_data: Vec<u8>,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Broadcast a "going offline" message via gossipsub before graceful shutdown.
    BroadcastGoingOffline {
        data: Vec<u8>,
    },
}
