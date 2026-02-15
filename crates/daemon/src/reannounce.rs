//! Content maintenance loop
//!
//! Periodically re-announces content to the DHT and triggers shard distribution
//! for content that hasn't achieved redundancy targets.

use std::sync::Arc;

use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, DataCraftCapability};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;
use crate::content_tracker::ContentTracker;
use crate::events::{DaemonEvent, EventSender};
use crate::peer_scorer::PeerScorer;

/// Default maintenance interval in seconds (10 minutes).
/// Prefer using `DaemonConfig::reannounce_interval_secs` instead.
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
    event_tx: EventSender,
    peer_scorer: Arc<Mutex<PeerScorer>>,
) {
    use std::time::Duration;

    // Initial delay to let the swarm establish connections
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        run_maintenance_cycle(&tracker, &command_tx, &client, &event_tx, &peer_scorer).await;
    }
}

/// Run a single maintenance cycle: re-announce + check distribution.
pub async fn run_maintenance_cycle(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    event_tx: &EventSender,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
) {
    info!("Content maintenance cycle starting");

    // Phase 1: Re-announce content that needs it
    let needs_announce = {
        let t = tracker.lock().await;
        t.needs_announcement()
    };

    let needs_dist_preview = {
        let t = tracker.lock().await;
        t.needs_distribution()
    };

    let needs_announce_count = needs_announce.len();

    let _ = event_tx.send(DaemonEvent::MaintenanceCycleStarted {
        content_count: needs_announce_count + needs_dist_preview.len(),
        needs_announce: needs_announce_count,
        needs_distribute: needs_dist_preview.len(),
    });

    if !needs_announce.is_empty() {
        info!("Content maintenance: {} items need (re-)announcement", needs_announce.len());
    }

    for content_id in needs_announce {
        reannounce_content(&content_id, tracker, command_tx, client, event_tx).await;
    }

    // Phase 2: Distribute shards to storage peers
    distribute_content(tracker, command_tx, client, peer_scorer, event_tx).await;

    // Phase 3: Check distribution status by resolving providers
    let needs_dist = {
        let t = tracker.lock().await;
        t.needs_distribution()
    };

    for content_id in needs_dist {
        check_providers(&content_id, tracker, command_tx).await;
    }

    let _ = event_tx.send(DaemonEvent::MaintenanceCycleCompleted {
        announced: needs_announce_count,
        distributed: needs_dist_preview.len(),
        next_run_secs: 0, // caller knows the interval
    });
}

/// Re-announce a single content item to the DHT.
async fn reannounce_content(
    content_id: &ContentId,
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    event_tx: &EventSender,
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
            let _ = event_tx.send(DaemonEvent::ContentReannounced {
                content_id: content_id.to_hex(),
            });
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

/// Distribute undistributed content to storage peers.
///
/// For each content item needing distribution, reads local shards and pushes
/// them to available storage peers in a round-robin fashion.
async fn distribute_content(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
    event_tx: &EventSender,
) {
    // Get storage peers
    let storage_peers: Vec<libp2p::PeerId> = {
        let scorer = peer_scorer.lock().await;
        let total_peers = scorer.iter().count();
        let peers: Vec<_> = scorer
            .iter()
            .filter(|(_, score)| score.capabilities.contains(&DataCraftCapability::Storage))
            .map(|(peer_id, _)| *peer_id)
            .collect();
        info!(
            "Distribution: peer_scorer has {} total peers, {} with Storage capability",
            total_peers,
            peers.len()
        );
        for (peer_id, score) in scorer.iter() {
            info!("  peer {} — capabilities: {:?}", peer_id, score.capabilities);
        }
        peers
    };

    if storage_peers.is_empty() {
        let total_known = {
            let scorer = peer_scorer.lock().await;
            scorer.iter().count()
        };
        let reason = if total_known == 0 {
            "No peers connected — waiting for network discovery".to_string()
        } else {
            format!("{} peers connected but none have Storage capability", total_known)
        };
        info!("Distribution: {} — skipping", reason);
        let _ = event_tx.send(DaemonEvent::DistributionSkipped {
            reason,
            retry_secs: 600,
        });
        return;
    }

    // Rank them by score
    let ranked_peers = {
        let scorer = peer_scorer.lock().await;
        scorer.rank_peers(&storage_peers)
    };

    let needs_dist = {
        let t = tracker.lock().await;
        t.needs_distribution()
    };

    if needs_dist.is_empty() {
        info!("Distribution: all content already distributed — nothing to do");
        return;
    }

    info!(
        "Distributing {} content items to {} storage peers",
        needs_dist.len(),
        ranked_peers.len()
    );

    for content_id in needs_dist {
        // Get manifest to know total shards
        let manifest = {
            let c = client.lock().await;
            match c.store().get_manifest(&content_id) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Cannot distribute {}: manifest not found: {}", content_id, e);
                    continue;
                }
            }
        };

        let total_shards =
            manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards;
        let mut pushed_count = 0usize;
        let mut peer_idx = 0usize;

        for chunk_idx in 0..manifest.chunk_count as u32 {
            for shard_idx in 0..total_shards as u8 {
                // Read shard from local store
                let shard_data = {
                    let c = client.lock().await;
                    match c.store().get_shard(&content_id, chunk_idx, shard_idx) {
                        Ok(data) => data,
                        Err(_) => continue, // Skip shards we don't have
                    }
                };

                // Pick next peer (round-robin)
                let peer = ranked_peers[peer_idx % ranked_peers.len()];
                peer_idx += 1;

                // Push shard — fire and forget (don't block on each one)
                let (reply_tx, reply_rx) = oneshot::channel();
                let cmd = DataCraftCommand::PushShard {
                    peer_id: peer,
                    content_id,
                    chunk_index: chunk_idx,
                    shard_index: shard_idx,
                    shard_data,
                    reply_tx,
                };

                if command_tx.send(cmd).is_err() {
                    warn!("Failed to send push command for {}/{}/{}", content_id, chunk_idx, shard_idx);
                    continue;
                }

                // Await result but don't block the whole loop on failure
                match reply_rx.await {
                    Ok(Ok(())) => {
                        pushed_count += 1;
                    }
                    Ok(Err(e)) => {
                        debug!("Push {}/{}/{} to {} failed: {}", content_id, chunk_idx, shard_idx, peer, e);
                    }
                    Err(_) => {
                        debug!("Push reply channel closed for {}/{}/{}", content_id, chunk_idx, shard_idx);
                    }
                }
            }
        }

        if pushed_count > 0 {
            info!("Distributed {}/{} shards for {}", pushed_count, total_shards * manifest.chunk_count as usize, content_id);
            let _ = event_tx.send(DaemonEvent::ContentDistributed {
                content_id: content_id.to_hex(),
                shards_pushed: pushed_count,
                total_shards: total_shards * manifest.chunk_count as usize,
                target_peers: ranked_peers.len(),
            });
            let mut t = tracker.lock().await;
            t.update_shard_progress(&content_id, pushed_count);
            // Emit content status update
            if let Some((state, summary)) = t.status_summary(&content_id) {
                let _ = event_tx.send(DaemonEvent::ContentStatus {
                    content_id: content_id.to_hex(),
                    name: state.name,
                    size: state.size,
                    stage: state.stage.to_string(),
                    local_shards: state.local_shards,
                    remote_shards: state.remote_shards,
                    total_shards: state.total_shards,
                    provider_count: state.provider_count,
                    summary,
                });
            }
        }
    }
}

/// Trigger immediate re-announcement and distribution of all content.
/// Called when a new storage peer connects.
pub async fn trigger_immediate_reannounce(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    event_tx: &EventSender,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
) {
    info!("Triggering immediate re-announcement and distribution (storage peer connected)");
    run_maintenance_cycle(tracker, command_tx, client, event_tx, peer_scorer).await;
}
