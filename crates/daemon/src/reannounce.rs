//! Content maintenance loop
//!
//! Periodically re-announces content to the DHT and equalizes piece pressure.
//! Initial push (Function 1) is triggered inline at publish time.

use std::sync::Arc;

use craftobj_client::CraftObjClient;
use craftobj_core::{ContentId, CraftObjCapability};
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::commands::CraftObjCommand;
use crate::content_tracker::ContentTracker;
use crate::events::{DaemonEvent, EventSender};
use crate::peer_scorer::PeerScorer;

pub const DEFAULT_INTERVAL_SECS: u64 = 600;

pub async fn content_maintenance_loop(
    tracker: Arc<Mutex<ContentTracker>>,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    client: Arc<Mutex<CraftObjClient>>,
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
    command_tx: &mpsc::UnboundedSender<CraftObjCommand>,
    client: &Arc<Mutex<CraftObjClient>>,
    event_tx: &EventSender,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
) {
    info!("[reannounce.rs] Content maintenance cycle starting");

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

    // Retry initial push for content that hasn't been distributed yet
    let needs_push = {
        let t = tracker.lock().await;
        t.needs_distribution()
    };
    for cid in &needs_push {
        info!("[reannounce.rs] Maintenance: retrying initial push for {}", cid);
        run_initial_push(cid, tracker, command_tx, client, peer_scorer, event_tx).await;
    }

    // Equalization (Function 2) — for content that has completed initial push
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
    command_tx: &mpsc::UnboundedSender<CraftObjCommand>,
    client: &Arc<Mutex<CraftObjClient>>,
    event_tx: &EventSender,
) {
    let manifest = {
        let c = client.lock().await;
        match c.store().get_record(content_id) {
            Ok(m) => m,
            Err(e) => {
                warn!("[reannounce.rs] Cannot re-announce {}: manifest not found: {}", content_id, e);
                return;
            }
        }
    };

    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = CraftObjCommand::AnnounceProvider {
        content_id: *content_id,
        manifest,
        verification_record: None,
        reply_tx,
    };

    if command_tx.send(cmd).is_err() {
        warn!("[reannounce.rs] Failed to send re-announce command for {}", content_id);
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
            warn!("[reannounce.rs] Re-announce failed for {}: {}", content_id, e);
        }
        Err(e) => {
            warn!("[reannounce.rs] Re-announce reply channel closed for {}: {}", content_id, e);
        }
    }
}

async fn check_providers(
    content_id: &ContentId,
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<CraftObjCommand>,
) {
    let (reply_tx, reply_rx) = oneshot::channel();
    let cmd = CraftObjCommand::ResolveProviders {
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
    command_tx: &mpsc::UnboundedSender<CraftObjCommand>,
    client: &Arc<Mutex<CraftObjClient>>,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
    event_tx: &EventSender,
) {
    info!("[reannounce.rs] run_initial_push: starting for {}", content_id);
    let storage_peers: Vec<libp2p::PeerId> = {
        let scorer = peer_scorer.lock().await;
        let all_peers: Vec<_> = scorer.iter().collect();
        info!("[reannounce.rs] run_initial_push: peer_scorer has {} peers total", all_peers.len());
        for (pid, score) in &all_peers {
            info!("[reannounce.rs] run_initial_push: peer {} caps={:?}", pid, score.capabilities);
        }
        all_peers
            .into_iter()
            .filter(|(_, score)| score.capabilities.contains(&CraftObjCapability::Storage))
            .map(|(peer_id, _)| *peer_id)
            .collect()
    };

    info!("[reannounce.rs] run_initial_push: found {} storage peers for {}", storage_peers.len(), content_id);

    if storage_peers.is_empty() {
        info!("[reannounce.rs] run_initial_push: no storage peers available, will retry next cycle for {}", content_id);
        return;
    }

    let ranked_peers = {
        let mut scorer = peer_scorer.lock().await;
        scorer.rank_peers(&storage_peers)
    };

    let manifest = {
        let c = client.lock().await;
        match c.store().get_record(content_id) {
            Ok(m) => m,
            Err(e) => {
                warn!("[reannounce.rs] Cannot push {}: {}", content_id, e);
                return;
            }
        }
    };

    // Push manifest to all peers first
    let record_json = match serde_json::to_vec(&manifest) {
        Ok(j) => j,
        Err(e) => {
            warn!("[reannounce.rs] Cannot serialize manifest for {}: {}", content_id, e);
            return;
        }
    };

    // Push manifest to all peers — only peers that receive it get pieces
    let mut manifest_ok_peers: Vec<libp2p::PeerId> = Vec::new();
    {
        let mut manifest_futs = Vec::new();
        for &peer in &ranked_peers {
            let (reply_tx, reply_rx) = oneshot::channel();
            let cmd = CraftObjCommand::PushRecord {
                peer_id: peer,
                content_id: *content_id,
                record_json: record_json.clone(),
                reply_tx,
            };
            if command_tx.send(cmd).is_ok() {
                manifest_futs.push(async move { (peer, reply_rx.await) });
            }
        }
        for (peer, result) in futures::future::join_all(manifest_futs).await {
            match result {
                Ok(Ok(())) => {
                    info!("[reannounce.rs] Pushed manifest for {} to {}", content_id, peer);
                    manifest_ok_peers.push(peer);
                }
                Ok(Err(e)) => warn!("[reannounce.rs] Manifest push to {} failed: {}", peer, e),
                Err(_) => { warn!("[reannounce.rs] Manifest push to {} — reply channel dropped", peer); }
            }
        }
    }

    if manifest_ok_peers.is_empty() {
        warn!("[reannounce.rs] Initial push for {}: manifest push failed for ALL peers, will retry next cycle", content_id);
        return;
    }

    info!("[reannounce.rs] Initial push for {}: manifest accepted by {}/{} peers", content_id, manifest_ok_peers.len(), ranked_peers.len());

    // Collect all local pieces
    let mut all_pieces: Vec<(u32, [u8; 32])> = Vec::new();
    for seg_idx in 0..manifest.segment_count() as u32 {
        let piece_ids = {
            let c = client.lock().await;
            c.store().list_pieces(content_id, seg_idx).unwrap_or_default()
        };
        for pid in piece_ids {
            all_pieces.push((seg_idx, pid));
        }
    }

    // Push ALL pieces round-robin across peers. Publisher uploads everything (1.2x content size).
    // Skip failed peers for remaining pieces. Emit progress events periodically.
    let total_pieces = all_pieces.len();
    let mut failed_peers: std::collections::HashSet<libp2p::PeerId> = std::collections::HashSet::new();
    let mut total_pushed: usize = 0;
    let progress_interval = std::cmp::max(total_pieces / 20, 10); // ~5% or every 10

    let cid = *content_id;
    for (i, (seg_idx, piece_id)) in all_pieces.iter().enumerate() {
        // Find next available peer (round-robin, skipping failed)
        let available_peers: Vec<libp2p::PeerId> = manifest_ok_peers.iter()
            .filter(|p| !failed_peers.contains(p))
            .copied()
            .collect();
        if available_peers.is_empty() {
            warn!("[reannounce.rs] Initial push for {}: all peers failed, stopping at piece {}/{}", content_id, i, total_pieces);
            break;
        }
        let peer = available_peers[i % available_peers.len()];

        let (piece_data, coefficients) = {
            let c = client.lock().await;
            match c.store().get_piece(&cid, *seg_idx, piece_id) {
                Ok(d) => d,
                Err(_) => continue,
            }
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        let cmd = CraftObjCommand::PushPiece {
            peer_id: peer,
            content_id: cid,
            segment_index: *seg_idx,
            piece_id: *piece_id,
            coefficients,
            piece_data,
            reply_tx,
        };
        if command_tx.send(cmd).is_err() {
            continue;
        }

        // Timeout per piece push: 10 seconds
        match tokio::time::timeout(std::time::Duration::from_secs(10), reply_rx).await {
            Ok(Ok(Ok(()))) => {
                total_pushed += 1;
                info!("[reannounce.rs] Pushed piece {}/{} for {} to {}", total_pushed, total_pieces, content_id, peer);
            }
            Ok(Ok(Err(e))) => {
                warn!("[reannounce.rs] Push piece {}/{} to {} failed: {}, marking as dead", i+1, total_pieces, peer, e);
                failed_peers.insert(peer);
            }
            Ok(Err(_)) => {
                warn!("[reannounce.rs] Push piece {}/{} to {} — reply channel dropped, marking as dead", i+1, total_pieces, peer);
                failed_peers.insert(peer);
            }
            Err(_) => {
                warn!("[reannounce.rs] Push piece {}/{} to {} timed out (10s), marking as dead", i+1, total_pieces, peer);
                failed_peers.insert(peer);
            }
        }

        // Emit progress event periodically
        if total_pushed > 0 && (total_pushed % progress_interval == 0 || i == total_pieces - 1) {
            let active_peers = ranked_peers.len() - failed_peers.len();
            let _ = event_tx.send(DaemonEvent::DistributionProgress {
                content_id: content_id.to_hex(),
                pieces_pushed: total_pushed,
                total_pieces,
                peers_active: active_peers,
            });
        }
    }

    info!("[reannounce.rs] Initial push for {}: pushed {}/{} pieces to {} peers", content_id, total_pushed, total_pieces, ranked_peers.len());

    if total_pushed > 0 {
        let _ = event_tx.send(DaemonEvent::ContentDistributed {
            content_id: content_id.to_hex(),
            pieces_pushed: total_pushed,
            total_pieces,
            target_peers: ranked_peers.len(),
        });
    }

    // Only mark initial push done and delete local pieces if ALL pieces were pushed successfully.
    // If partial, keep retrying next cycle.
    if total_pushed == total_pieces {
        {
            let mut t = tracker.lock().await;
            t.mark_initial_push_done(content_id);
        }

        // Publisher is a client, not a storage node. Delete local pieces after full distribution.
        let c = client.lock().await;
        if let Err(e) = c.store().delete_content(content_id) {
            warn!("[reannounce.rs] Failed to clean up publisher pieces for {}: {}", content_id, e);
        } else {
            info!("[reannounce.rs] Publisher cleanup: deleted local pieces for {} after distributing all {} pieces", content_id, total_pushed);
        }
    } else {
        info!("[reannounce.rs] Initial push for {}: partial ({}/{}), will retry next cycle", content_id, total_pushed, total_pieces);
    }
}

/// Pressure equalization (Function 2): for CIDs where local_pieces > 2,
/// push HALF of local pieces, split equally across ALL non-provider nodes.
/// After successful push, delete the pushed pieces locally (pieces are UNIQUE).
async fn equalize_pressure(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<CraftObjCommand>,
    client: &Arc<Mutex<CraftObjClient>>,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
    event_tx: &EventSender,
) {
    let storage_peers: Vec<libp2p::PeerId> = {
        let scorer = peer_scorer.lock().await;
        scorer
            .iter()
            .filter(|(_, score)| score.capabilities.contains(&CraftObjCapability::Storage))
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

        let manifest = {
            let c = client.lock().await;
            match c.store().get_record(&content_id) {
                Ok(m) => m,
                Err(_) => continue,
            }
        };

        // Collect all local pieces
        let mut all_pieces: Vec<(u32, [u8; 32])> = Vec::new();
        for seg in 0..manifest.segment_count() as u32 {
            let pieces = {
                let c = client.lock().await;
                c.store().list_pieces(&content_id, seg).unwrap_or_default()
            };
            for pid in pieces {
                all_pieces.push((seg, pid));
            }
        }

        if all_pieces.is_empty() {
            continue;
        }

        // Push HALF of local pieces, split equally across ALL non-provider candidates
        let half = all_pieces.len() / 2;
        if half == 0 {
            continue;
        }
        let pieces_to_push = &all_pieces[..half];
        let per_peer = std::cmp::max(half / candidates.len(), 1);

        // Push manifest to all candidates first
        let record_json = match serde_json::to_vec(&manifest) {
            Ok(j) => j,
            Err(_) => continue,
        };
        for &peer in &candidates {
            let (reply_tx, _reply_rx) = oneshot::channel();
            let _ = command_tx.send(CraftObjCommand::PushRecord {
                peer_id: peer,
                content_id,
                record_json: record_json.clone(),
                reply_tx,
            });
        }

        let mut total_pushed = 0usize;
        let mut pieces_deleted = Vec::new();

        for (i, (seg, pid)) in pieces_to_push.iter().enumerate() {
            let peer = candidates[i / per_peer % candidates.len()];

            let (piece_data, coefficients) = {
                let c = client.lock().await;
                match c.store().get_piece(&content_id, *seg, pid) {
                    Ok(d) => d,
                    Err(_) => continue,
                }
            };

            let (reply_tx, reply_rx) = oneshot::channel();
            let cmd = CraftObjCommand::PushPiece {
                peer_id: peer,
                content_id,
                segment_index: *seg,
                piece_id: *pid,
                coefficients,
                piece_data,
                reply_tx,
            };

            if command_tx.send(cmd).is_ok() {
                if let Ok(Ok(())) = reply_rx.await {
                    total_pushed += 1;
                    pieces_deleted.push((*seg, *pid));
                }
            }
        }

        // Storage providers keep their pieces after equalization — they need them for serving.
        // Only the initial publisher push deletes local copies (publisher is not a storage node).
        // Equalization COPIES pieces to new nodes, it does NOT move them.

        if total_pushed > 0 {
            debug!("Equalization: pushed {} pieces for {} to {} peers", total_pushed, content_id, candidates.len());
            let _ = event_tx.send(DaemonEvent::ContentDistributed {
                content_id: content_id.to_hex(),
                pieces_pushed: total_pushed,
                total_pieces: total_pushed,
                target_peers: candidates.len(),
            });
        }
    }
}
