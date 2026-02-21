//! Daemon command system
//!
//! Commands sent from IPC handler to the swarm event loop for DHT operations
//! and stream-based transfers.

use craftobj_core::{ContentId, ContentManifest};
use libp2p::PeerId;
use tokio::sync::oneshot;

use craftobj_transfer::{CraftObjResponse, PiecePayload};

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
    GetRecord {
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

    // NOTE: PutReKey, RemoveReKey, PutAccessList, GetAccessList,
    // PublishRemoval, CheckRemoval removed — COM layer concerns.

    /// Trigger an immediate distribution cycle (e.g. after content publish or startup import).
    TriggerDistribution,
    /// Push a manifest to a remote storage peer via persistent stream.
    PushRecord {
        peer_id: PeerId,
        content_id: ContentId,
        record_json: Vec<u8>,
        reply_tx: oneshot::Sender<Result<(), String>>,
    },
    /// Distribute all pieces for a content to a peer using unified send_pieces().
    DistributePieces {
        peer_id: PeerId,
        content_id: ContentId,
        pieces: Vec<PiecePayload>,
        reply_tx: oneshot::Sender<Result<crate::piece_transfer::TransferResult, String>>,
    },
    /// Fetch pieces from a peer using unified piece_transfer protocol.
    FetchPieces {
        peer_id: PeerId,
        content_id: ContentId,
        segment_index: u32,
        have_pieces: Vec<[u8; 32]>,
        max_pieces: u16,
        reply_tx: oneshot::Sender<Result<Vec<[u8; 32]>, String>>,
    },
    /// Fetch a single piece from a peer and return raw bytes — does NOT write to local store.
    /// Used by ConnectionPool's CommandChannelRequester to enable the independence-checking
    /// parallel fetch path without bypassing the pool's dedup logic.
    FetchOnePieceRaw {
        peer_id: PeerId,
        content_id: ContentId,
        segment_index: u32,
        /// Reply contains (coefficients, data) for one piece.
        reply_tx: oneshot::Sender<Result<(Vec<u8>, Vec<u8>), String>>,
    },
    /// Query a peer's piece count for a specific segment (lightweight health check).
    HealthQuery {
        peer_id: PeerId,
        content_id: ContentId,
        segment_index: u32,
        /// Per-query random nonce — prevents cache/replay of HealthResponse.
        nonce: [u8; 32],
        reply_tx: oneshot::Sender<Result<u32, String>>,
    },
    /// Publish a DHT provider record for a CID+segment.
    StartProviding { key: Vec<u8> },
    /// Remove a DHT provider record for a CID+segment.
    StopProviding { key: Vec<u8> },
    /// Return list of currently connected peer IDs.
    ConnectedPeers {
        reply_tx: oneshot::Sender<Vec<String>>,
    },
    /// Send PDP challenge to a peer for proof of data possession.
    PdpChallenge {
        peer_id: PeerId,
        content_id: ContentId,
        segment_index: u32,
        piece_id: [u8; 32],
        nonce: [u8; 32],
        byte_positions: Vec<u32>,
        reply_tx: oneshot::Sender<Option<CraftObjResponse>>,
    },
    /// Trigger an immediate repair scan for a content ID.
    ///
    /// Sent by the challenger when a PDP challenge fails for a peer — bypasses the
    /// scheduled HealthScan cycle and runs `scan_segment` immediately so underreplication
    /// due to a bad/lost piece is acted on without waiting for the next scan window.
    TriggerRepair {
        content_id: ContentId,
        /// Optional hint: the segment that failed PDP (scan all segments if None).
        failed_segment: Option<u32>,
    },
}
