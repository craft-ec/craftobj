//! Daemon command system
//!
//! Commands sent from IPC handler to the swarm event loop for DHT operations
//! and stream-based transfers.

use craftobj_core::{ContentId, ContentManifest};
use craftobj_transfer::CraftObjResponse;
use libp2p::PeerId;
use tokio::sync::oneshot;

/// Commands that can be sent to the swarm event loop.
#[derive(Debug)]
pub enum CraftObjCommand {
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
        reply_tx: oneshot::Sender<Result<CraftObjResponse, String>>,
    },
    /// Query a peer's PieceMap entries for a specific segment (lightweight sync).
    PieceMapQuery {
        peer_id: PeerId,
        content_id: ContentId,
        segment_index: u32,
        reply_tx: oneshot::Sender<Result<CraftObjResponse, String>>,
    },
    /// Store a re-encryption key in the DHT for access grant.
    PutReKey {
        content_id: ContentId,
        entry: craftobj_core::pre::ReKeyEntry,
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
        access_list: craftobj_core::access::AccessList,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Fetch an access list from the DHT.
    GetAccessList {
        content_id: ContentId,
        reply_tx: oneshot::Sender<Result<craftobj_core::access::AccessList, String>>,
    },
    /// Publish a removal notice to DHT.
    PublishRemoval {
        content_id: ContentId,
        notice: craftobj_core::RemovalNotice,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Check if content has been removed (check local cache first, then DHT).
    CheckRemoval {
        content_id: ContentId,
        reply_tx: oneshot::Sender<Result<Option<craftobj_core::RemovalNotice>, String>>,
    },
    /// Trigger an immediate distribution cycle (e.g. after content publish or startup import).
    TriggerDistribution,
    /// Push a manifest to a remote storage peer via persistent stream.
    PushManifest {
        peer_id: PeerId,
        content_id: ContentId,
        manifest_json: Vec<u8>,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Push a piece to a remote storage peer via persistent stream.
    PushPiece {
        peer_id: PeerId,
        content_id: ContentId,
        segment_index: u32,
        piece_id: [u8; 32],
        coefficients: Vec<u8>,
        piece_data: Vec<u8>,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Sync PieceMap entries for a newly tracked segment from connected peers.
    SyncPieceMap {
        content_id: ContentId,
        segment_index: u32,
    },
    /// Publish a DHT provider record for a CID+segment (called after storing a piece).
    StartProviding { key: Vec<u8> },
    /// Remove a DHT provider record for a CID+segment (called after dropping all pieces for a segment).
    StopProviding { key: Vec<u8> },
}
