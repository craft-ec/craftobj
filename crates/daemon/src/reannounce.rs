//! Content maintenance loop
//!
//! Periodically re-announces content to the DHT and triggers shard distribution
//! for content that hasn't achieved redundancy targets.

use std::sync::Arc;

use datacraft_client::DataCraftClient;
use datacraft_core::ContentId;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;
use crate::content_tracker::ContentTracker;

/// Default maintenance interval: 10 minutes.
pub const DEFAULT_INTERVAL_SECS: u64 = 600;

/// Run the content maintenance loop.
///
/// Periodically scans the tracker for content needing re-announcement or distribution,
/// then sends the appropriate commands to the swarm event loop.
pub async fn content_maintenance_loop(
    tracker: Arc<Mutex<ContentTracker>>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    client: Arc<Mutex<DataCraftClient>>,
    interval_secs: u64,
) {
    use std::time::Duration;

    // Initial delay to let the swarm establish connections
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        run_maintenance_cycle(&tracker, &command_tx, &client).await;
    }
}

/// Run a single maintenance cycle: re-announce + check distribution.
pub async fn run_maintenance_cycle(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
) {
    // Phase 1: Re-announce content that needs it
    let needs_announce = {
        let t = tracker.lock().await;
        t.needs_announcement()
    };

    if !needs_announce.is_empty() {
        info!("Content maintenance: {} items need (re-)announcement", needs_announce.len());
    }

    for content_id in needs_announce {
        reannounce_content(&content_id, tracker, command_tx, client).await;
    }

    // Phase 2: Check distribution status by resolving providers
    let needs_dist = {
        let t = tracker.lock().await;
        t.needs_distribution()
    };

    for content_id in needs_dist {
        check_providers(&content_id, tracker, command_tx).await;
    }
}

/// Re-announce a single content item to the DHT.
async fn reannounce_content(
    content_id: &ContentId,
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
) {
    // Get manifest from local store
    let manifest = {
        let c = client.lock().await;
        match c.store().get_manifest(content_id) {
            Ok(m) => m,
            Err(e) => {
                warn!("Cannot re-announce {}: manifest not found: {}", content_id, e);
                return;
            }
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = DataCraftCommand::AnnounceProvider {
        content_id: *content_id,
        manifest,
        reply_tx,
    };

    if command_tx.send(cmd).is_err() {
        warn!("Failed to send re-announce command for {}", content_id);
        return;
    }

    match reply_rx.await {
        Ok(Ok(())) => {
            debug!("Re-announced {} to DHT", content_id);
            let mut t = tracker.lock().await;
            t.mark_announced(content_id);
        }
        Ok(Err(e)) => {
            warn!("Re-announce failed for {}: {}", content_id, e);
        }
        Err(e) => {
            warn!("Re-announce reply channel closed for {}: {}", content_id, e);
        }
    }
}

/// Check provider count for a content item via DHT resolution.
async fn check_providers(
    content_id: &ContentId,
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
) {
    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = DataCraftCommand::ResolveProviders {
        content_id: *content_id,
        reply_tx,
    };

    if command_tx.send(cmd).is_err() {
        return;
    }

    match reply_rx.await {
        Ok(Ok(providers)) => {
            let count = providers.len();
            debug!("Content {} has {} providers", content_id, count);
            let mut t = tracker.lock().await;
            t.update_provider_count(content_id, count);
            // Use provider count as a proxy for remote shard availability
            // (each provider should hold total_shards)
            if count > 0 {
                if let Some(state) = t.get(content_id) {
                    let remote_estimate = state.total_shards.min(count * state.total_shards);
                    let cid = *content_id;
                    drop(t);
                    let mut t = tracker.lock().await;
                    t.update_shard_progress(&cid, remote_estimate);
                }
            }
        }
        Ok(Err(e)) => {
            debug!("Provider resolution failed for {}: {}", content_id, e);
        }
        Err(_) => {}
    }
}

/// Trigger immediate re-announcement of all content that needs it.
/// Called when the first storage peer connects.
pub async fn trigger_immediate_reannounce(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
) {
    info!("Triggering immediate re-announcement (first storage peer connected)");
    run_maintenance_cycle(tracker, command_tx, client).await;
}
