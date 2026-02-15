//! Daemon command system
//!
//! Commands sent from IPC handler to the swarm event loop for DHT operations.

use datacraft_core::{ContentId, ChunkManifest, DataCraftCapability};
use libp2p::PeerId;
use tokio::sync::oneshot;

/// Commands that can be sent to the swarm event loop.
#[derive(Debug)]
pub enum DataCraftCommand {
    /// Announce this node as a provider for a content ID.
    AnnounceProvider {
        content_id: ContentId,
        manifest: ChunkManifest,
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
        reply_tx: oneshot::Sender<Result<ChunkManifest, String>>,
    },
    /// Request a shard from a remote peer.
    RequestShard {
        peer_id: PeerId,
        content_id: ContentId,
        chunk_index: u32,
        shard_index: u8,
        /// Requester's public key (for signing TransferReceipts).
        local_public_key: [u8; 32],
        reply_tx: oneshot::Sender<Result<Vec<u8>, String>>,
    },
    /// Publish a capability announcement via gossipsub.
    PublishCapabilities {
        capabilities: Vec<DataCraftCapability>,
    },
    /// Query a peer for their max shard index for a CID.
    QueryMaxShardIndex {
        peer_id: PeerId,
        content_id: ContentId,
        reply_tx: oneshot::Sender<Result<Option<u8>, String>>,
    },
    /// Extend a CID: fetch k shards, coordinate index, generate new parity, store + announce.
    Extend {
        content_id: ContentId,
        reply_tx: oneshot::Sender<Result<u8, String>>,
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
}