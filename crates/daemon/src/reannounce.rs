//! Content maintenance loop
//!
//! Periodically re-announces content to the DHT and equalizes piece pressure.
//! Initial push (Function 1) is triggered inline at publish time.

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

    let needs_equalize = {
        let t = tracker.lock().await;
        t.needs_equalization()
    };

    let needs_announce_count = needs_announce.len();

    let _ = event_tx.send(DaemonEvent::MaintenanceCycleStarted {
        content_count: needs_announce_count + needs_equalize.len(),
        needs_announce: needs_announce_count,
        needs_distribute: needs_equalize.len(),
    });

    for content_id in &needs_announce {
        reannounce_content(content_id, tracker, command_tx, client, event_tx).await;
    }

    // Equalization only (Function 2) — initial push is done at publish time
    equalize_pressure(tracker, command_tx, client, peer_scorer, event_tx).await;

    // Check providers for announced content
    for content_id in &needs_announce {
        check_providers(content_id, tracker, command_tx).await;
    }

    let _ = event_tx.send(DaemonEvent::MaintenanceCycleCompleted {
        announced: needs_announce_count,
        distributed: needs_equalize.len(),
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
            for peer in &providers {
                t.add_provider(content_id, *peer);
            }
            t.update_provider_count(content_id, count);
        }
        Ok(Err(e)) => {
            debug!("Provider resolution failed for {}: {}", content_id, e);
        }
        Err(_) => {}
    }
}

/// Initial push (Function 1): push exactly 2 pieces per storage peer, round-robin.
/// Called inline at publish time. Fire and forget.
pub async fn run_initial_push(
    content_id: &ContentId,
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
        info!("Initial push for {}: no storage peers available, will retry next cycle", content_id);
        return; // Don't mark as done — needs_distribution() will still return it
    }

    let ranked_peers = {
        let scorer = peer_scorer.lock().await;
        scorer.rank_peers(&storage_peers)
    };

    let manifest = {
        let c = client.lock().await;
        match c.store().get_manifest(content_id) {
            Ok(m) => m,
            Err(e) => {
                warn!("Cannot push {}: {}", content_id, e);
                return;
            }
        }
    };

    // Push manifest to all peers first
    let manifest_json = match serde_json::to_vec(&manifest) {
        Ok(j) => j,
        Err(e) => {
            warn!("Cannot serialize manifest for {}: {}", content_id, e);
            return;
        }
    };

    {
        let mut manifest_futs = Vec::new();
        for &peer in &ranked_peers {
            let (reply_tx, reply_rx) = oneshot::channel();
            let cmd = DataCraftCommand::PushManifest {
                peer_id: peer,
                content_id: *content_id,
                manifest_json: manifest_json.clone(),
                reply_tx,
            };
            if command_tx.send(cmd).is_ok() {
                manifest_futs.push(async move { (peer, reply_rx.await) });
            }
        }
        for (peer, result) in futures::future::join_all(manifest_futs).await {
            match result {
                Ok(Ok(())) => debug!("Pushed manifest for {} to {}", content_id, peer),
                Ok(Err(e)) => warn!("Manifest push to {} failed: {}", peer, e),
                Err(_) => {}
            }
        }
    }

    // Collect all local pieces
    let mut all_pieces: Vec<(u32, [u8; 32])> = Vec::new();
    for seg_idx in 0..manifest.segment_count as u32 {
        let piece_ids = {
            let c = client.lock().await;
            c.store().list_pieces(content_id, seg_idx).unwrap_or_default()
        };
        for pid in piece_ids {
            all_pieces.push((seg_idx, pid));
        }
    }

    // Assign exactly 2 pieces per peer, round-robin. Stop when we run out of peers.
    let pieces_per_peer: usize = 2;
    let max_pieces = pieces_per_peer * ranked_peers.len();
    let mut assignments: Vec<(libp2p::PeerId, u32, [u8; 32])> = Vec::new();

    for (i, (seg_idx, piece_id)) in all_pieces.iter().enumerate() {
        if i >= max_pieces {
            break; // Only push 2 per peer, remaining pieces spread via equalization
        }
        let peer = ranked_peers[i / pieces_per_peer];
        assignments.push((peer, *seg_idx, *piece_id));
    }

    // Push assigned pieces with bounded concurrency
    let max_concurrent: usize = 30;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let pushed_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut push_futs = Vec::new();
    let cid = *content_id;
    for (peer, seg_idx, piece_id) in assignments {
        let (piece_data, coefficients) = {
            let c = client.lock().await;
            match c.store().get_piece(&cid, seg_idx, &piece_id) {
                Ok(d) => d,
                Err(_) => continue,
            }
        };

        let sem = semaphore.clone();
        let cmd_tx = command_tx.clone();
        let pushed = pushed_count.clone();

        push_futs.push(async move {
            let _permit = sem.acquire().await;
            let (reply_tx, reply_rx) = oneshot::channel();
            let cmd = DataCraftCommand::PushPiece {
                peer_id: peer,
                content_id: cid,
                segment_index: seg_idx,
                piece_id,
                coefficients,
                piece_data,
                reply_tx,
            };
            if cmd_tx.send(cmd).is_err() {
                return;
            }
            match reply_rx.await {
                Ok(Ok(())) => {
                    pushed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(Err(e)) => {
                    warn!("Push piece to {} failed: {}", peer, e);
                }
                Err(_) => {}
            }
        });
    }
    futures::future::join_all(push_futs).await;
    let total_pushed = pushed_count.load(std::sync::atomic::Ordering::Relaxed);

    info!("Initial push for {}: pushed {} pieces to {} peers", content_id, total_pushed, ranked_peers.len());

    if total_pushed > 0 {
        let _ = event_tx.send(DaemonEvent::ContentDistributed {
            content_id: content_id.to_hex(),
            pieces_pushed: total_pushed,
            total_pieces: total_pushed,
            target_peers: ranked_peers.len(),
        });
    }

    // Mark initial push as done regardless of push count (fire and forget)
    {
        let mut t = tracker.lock().await;
        t.mark_initial_push_done(content_id);
        t.update_piece_progress(content_id, total_pushed);
    }
}

/// Pressure equalization (Function 2): for CIDs where local_pieces > 2,
/// push 1 piece to a storage peer that doesn't already have this CID.
async fn equalize_pressure(
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
        return;
    }

    let needs_equalize = {
        let t = tracker.lock().await;
        t.needs_equalization()
    };

    for content_id in needs_equalize {
        // Find peers that do NOT already have this CID
        let existing_providers: std::collections::HashSet<libp2p::PeerId> = {
            let t = tracker.lock().await;
            t.get_providers(&content_id).into_iter().collect()
        };

        let candidates: Vec<libp2p::PeerId> = storage_peers
            .iter()
            .filter(|p| !existing_providers.contains(p))
            .copied()
            .collect();

        if candidates.is_empty() {
            debug!("Equalization for {}: all storage peers are already providers", content_id);
            continue;
        }

        // Pick one candidate (deterministic from CID for distribution)
        let peer_idx = (content_id.0[0] as usize) % candidates.len();
        let peer = candidates[peer_idx];

        let manifest = {
            let c = client.lock().await;
            match c.store().get_manifest(&content_id) {
                Ok(m) => m,
                Err(_) => continue,
            }
        };

        // Find first available piece to push
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
                        debug!("Equalization: pushed 1 piece for {} to {}", content_id, peer);
                        let _ = event_tx.send(DaemonEvent::ContentDistributed {
                            content_id: content_id.to_hex(),
                            pieces_pushed: 1,
                            total_pieces: 1,
                            target_peers: 1,
                        });
                    }
                }
                break 'outer; // 1 piece per CID per cycle
            }
        }
    }
}
