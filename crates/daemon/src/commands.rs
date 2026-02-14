//! Daemon command system
//!
//! Commands sent from IPC handler to the swarm event loop for DHT operations.

use datacraft_core::{ContentId, ChunkManifest};
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
}