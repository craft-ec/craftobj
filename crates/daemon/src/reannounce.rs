//! Content maintenance loop
//!
//! Periodically re-announces content to the DHT and distributes pieces
//! to storage peers using round-robin push logic.

use std::sync::Arc;

use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, DataCraftCapability};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::commands::DataCraftCommand;
use crate::content_tracker::ContentTracker;
use crate::events::{DaemonEvent, EventSender};
use crate::peer_scorer::PeerScorer;

pub const DEFAULT_INTERVAL_SECS: u64 = 600;

pub async fn content_maintenance_loop(
    tracker: Arc<Mutex<ContentTracker>>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    client: Arc<Mutex<DataCraftClient>>,
    interval_secs: u64,
    event_tx: EventSender,
    peer_scorer: Arc<Mutex<PeerScorer>>,
) {
    use std::time::Duration;
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        run_maintenance_cycle(&tracker, &command_tx, &client, &event_tx, &peer_scorer).await;
    }
}

pub async fn run_maintenance_cycle(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    event_tx: &EventSender,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
) {
    info!("Content maintenance cycle starting");

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

    for content_id in needs_announce {
        reannounce_content(&content_id, tracker, command_tx, client, event_tx).await;
    }

    distribute_content(tracker, command_tx, client, peer_scorer, event_tx).await;
    equalize_pressure(tracker, command_tx, client, peer_scorer, event_tx).await;

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
        next_run_secs: 0,
    });
}

async fn reannounce_content(
    content_id: &ContentId,
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    event_tx: &EventSender,
) {
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
            if count > 0 {
                if let Some(state) = t.get(content_id) {
                    let remote_estimate = state.segment_count * state.k * count.min(3);
                    let cid = *content_id;
                    drop(t);
                    let mut t = tracker.lock().await;
                    t.update_piece_progress(&cid, remote_estimate);
                }
            }
        }
        Ok(Err(e)) => {
            debug!("Provider resolution failed for {}: {}", content_id, e);
        }
        Err(_) => {}
    }
}

/// Distribute pieces to storage peers. Round-robin push.
async fn distribute_content(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
    event_tx: &EventSender,
) {
    let storage_peers: Vec<libp2p::PeerId> = {
        let scorer = peer_scorer.lock().await;
        scorer
            .iter()
            .filter(|(_, score)| score.capabilities.contains(&DataCraftCapability::Storage))
            .map(|(peer_id, _)| *peer_id)
            .collect()
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
        let _ = event_tx.send(DaemonEvent::DistributionSkipped { reason, retry_secs: 600 });
        return;
    }

    let ranked_peers = {
        let scorer = peer_scorer.lock().await;
        scorer.rank_peers(&storage_peers)
    };

    let needs_dist = {
        let t = tracker.lock().await;
        t.needs_distribution()
    };

    if needs_dist.is_empty() {
        return;
    }

    info!(
        "Distributing {} content items to {} storage peers",
        needs_dist.len(),
        ranked_peers.len()
    );

    for content_id in needs_dist {
        let manifest = {
            let c = client.lock().await;
            match c.store().get_manifest(&content_id) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Cannot distribute {}: {}", content_id, e);
                    continue;
                }
            }
        };

        // Push manifest first
        let manifest_json = match serde_json::to_vec(&manifest) {
            Ok(j) => j,
            Err(e) => {
                warn!("Cannot serialize manifest for {}: {}", content_id, e);
                continue;
            }
        };
        for &peer in &ranked_peers {
            let (reply_tx, reply_rx) = oneshot::channel();
            let cmd = DataCraftCommand::PushManifest {
                peer_id: peer,
                content_id,
                manifest_json: manifest_json.clone(),
                reply_tx,
            };
            if command_tx.send(cmd).is_ok() {
                match reply_rx.await {
                    Ok(Ok(())) => info!("Pushed manifest for {} to {}", content_id, peer),
                    Ok(Err(e)) => warn!("Manifest push to {} failed: {}", peer, e),
                    Err(_) => {}
                }
            }
        }

        // Push pieces round-robin, max 2 pieces per peer per CID.
        // Collect all pieces first, then distribute.
        let mut all_pieces: Vec<(u32, [u8; 32])> = Vec::new();
        for seg_idx in 0..manifest.segment_count as u32 {
            let piece_ids = {
                let c = client.lock().await;
                c.store().list_pieces(&content_id, seg_idx).unwrap_or_default()
            };
            for pid in piece_ids {
                all_pieces.push((seg_idx, pid));
            }
        }

        let mut pushed_count = 0usize;
        let mut peer_idx = 0usize;
        let max_per_peer: usize = 2;
        let mut pieces_per_peer: std::collections::HashMap<libp2p::PeerId, usize> =
            std::collections::HashMap::new();

        for (seg_idx, piece_id) in &all_pieces {
            // Find next peer that hasn't hit the per-peer cap
            let mut attempts = 0;
            let peer = loop {
                let candidate = ranked_peers[peer_idx % ranked_peers.len()];
                peer_idx += 1;
                let count = pieces_per_peer.entry(candidate).or_insert(0);
                if *count < max_per_peer {
                    break candidate;
                }
                attempts += 1;
                if attempts >= ranked_peers.len() {
                    break candidate; // all peers at cap, push anyway
                }
            };

            let (piece_data, coefficients) = {
                let c = client.lock().await;
                match c.store().get_piece(&content_id, *seg_idx, piece_id) {
                    Ok(d) => d,
                    Err(_) => continue,
                }
            };

            let (reply_tx, reply_rx) = oneshot::channel();
            let cmd = DataCraftCommand::PushPiece {
                peer_id: peer,
                content_id,
                segment_index: *seg_idx,
                piece_id: *piece_id,
                coefficients,
                piece_data,
                reply_tx,
            };

            if command_tx.send(cmd).is_err() {
                continue;
            }

            match reply_rx.await {
                Ok(Ok(())) => {
                    pushed_count += 1;
                    *pieces_per_peer.entry(peer).or_insert(0) += 1;
                }
                Ok(Err(e)) => {
                    warn!("Push piece to {} failed: {}", peer, e);
                }
                Err(_) => {}
            }
        }

        if pushed_count > 0 {
            info!("Distributed {} pieces for {}", pushed_count, content_id);
            let _ = event_tx.send(DaemonEvent::ContentDistributed {
                content_id: content_id.to_hex(),
                pieces_pushed: pushed_count,
                total_pieces: pushed_count, // approximation
                target_peers: ranked_peers.len(),
            });
            let mut t = tracker.lock().await;
            t.update_piece_progress(&content_id, pushed_count);
            if let Some((state, summary)) = t.status_summary(&content_id) {
                let _ = event_tx.send(DaemonEvent::ContentStatus {
                    content_id: content_id.to_hex(),
                    name: state.name,
                    size: state.size,
                    stage: state.stage.to_string(),
                    local_pieces: state.local_pieces,
                    remote_pieces: state.remote_pieces,
                    total_pieces: state.segment_count * state.k,
                    provider_count: state.provider_count,
                    summary,
                });
            }
        }
    }
}

/// Pressure equalization: if a node holds >2 pieces for any CID, push 1 excess
/// piece per CID per cycle to a random storage peer.
async fn equalize_pressure(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
    event_tx: &EventSender,
) {
    let excess_threshold: usize = 2;

    let storage_peers: Vec<libp2p::PeerId> = {
        let scorer = peer_scorer.lock().await;
        scorer
            .iter()
            .filter(|(_, score)| score.capabilities.contains(&DataCraftCapability::Storage))
            .map(|(peer_id, _)| *peer_id)
            .collect()
    };

    if storage_peers.is_empty() {
        return;
    }

    let all_cids = {
        let t = tracker.lock().await;
        t.needs_distribution()
    };

    for content_id in all_cids {
        // Count total local pieces
        let manifest = {
            let c = client.lock().await;
            match c.store().get_manifest(&content_id) {
                Ok(m) => m,
                Err(_) => continue,
            }
        };

        let mut total_local = 0usize;
        for seg in 0..manifest.segment_count as u32 {
            let c = client.lock().await;
            total_local += c.store().list_pieces(&content_id, seg).unwrap_or_default().len();
        }

        if total_local <= excess_threshold {
            continue;
        }

        // Pick a random peer and push 1 piece
        let peer_idx = (content_id.0[0] as usize) % storage_peers.len();
        let peer = storage_peers[peer_idx];

        // Find the first piece to push
        'outer: for seg in 0..manifest.segment_count as u32 {
            let pieces = {
                let c = client.lock().await;
                c.store().list_pieces(&content_id, seg).unwrap_or_default()
            };

            for pid in pieces {
                let (piece_data, coefficients) = {
                    let c = client.lock().await;
                    match c.store().get_piece(&content_id, seg, &pid) {
                        Ok(d) => d,
                        Err(_) => continue,
                    }
                };

                // Push manifest first
                let manifest_json = match serde_json::to_vec(&manifest) {
                    Ok(j) => j,
                    Err(_) => break 'outer,
                };
                let (reply_tx, _reply_rx) = oneshot::channel();
                let _ = command_tx.send(DataCraftCommand::PushManifest {
                    peer_id: peer,
                    content_id,
                    manifest_json,
                    reply_tx,
                });

                let (reply_tx, reply_rx) = oneshot::channel();
                let cmd = DataCraftCommand::PushPiece {
                    peer_id: peer,
                    content_id,
                    segment_index: seg,
                    piece_id: pid,
                    coefficients,
                    piece_data,
                    reply_tx,
                };

                if command_tx.send(cmd).is_ok() {
                    if let Ok(Ok(())) = reply_rx.await {
                        debug!("Pressure equalization: pushed 1 piece for {} to {}", content_id, peer);
                        let _ = event_tx.send(DaemonEvent::ContentDistributed {
                            content_id: content_id.to_hex(),
                            pieces_pushed: 1,
                            total_pieces: total_local,
                            target_peers: 1,
                        });
                    }
                }
                break 'outer; // 1 piece per CID per cycle
            }
        }
    }
}

pub async fn trigger_immediate_reannounce(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<DataCraftCommand>,
    client: &Arc<Mutex<DataCraftClient>>,
    event_tx: &EventSender,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
) {
    info!("Triggering immediate re-announcement and distribution");
    run_maintenance_cycle(tracker, command_tx, client, event_tx, peer_scorer).await;
}
