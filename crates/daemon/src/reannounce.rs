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
use crate::scaling::DemandTracker;

pub const DEFAULT_INTERVAL_SECS: u64 = 600;

pub async fn content_maintenance_loop(
    tracker: Arc<Mutex<ContentTracker>>,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    client: Arc<Mutex<CraftObjClient>>,
    interval_secs: u64,
    event_tx: EventSender,
    peer_scorer: Arc<Mutex<PeerScorer>>,
    demand_tracker: Arc<Mutex<DemandTracker>>,
) {
    use std::time::Duration;
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        run_maintenance_cycle(&tracker, &command_tx, &client, &event_tx, &peer_scorer, &demand_tracker).await;
    }
}

pub async fn run_maintenance_cycle(
    tracker: &Arc<Mutex<ContentTracker>>,
    command_tx: &mpsc::UnboundedSender<CraftObjCommand>,
    client: &Arc<Mutex<CraftObjClient>>,
    event_tx: &EventSender,
    peer_scorer: &Arc<Mutex<PeerScorer>>,
    demand_tracker: &Arc<Mutex<DemandTracker>>,
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

    // Equalization (Function 2) — demand-gated push to new providers
    equalize_pressure(tracker, command_tx, client, peer_scorer, event_tx, demand_tracker).await;

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

    let total_pieces = all_pieces.len();
    let cid = *content_id;

    info!("[reannounce.rs] Initial push for {}: distributing {} pieces to {} peers via send_pieces()", 
        content_id, total_pieces, manifest_ok_peers.len());

    // Group all pieces by target peer (round-robin distribution)
    let mut peer_piece_indices: std::collections::HashMap<libp2p::PeerId, Vec<(u32, [u8; 32])>> = std::collections::HashMap::new();
    for (i, &(seg_idx, piece_id)) in all_pieces.iter().enumerate() {
        let peer = manifest_ok_peers[i % manifest_ok_peers.len()];
        peer_piece_indices.entry(peer).or_default().push((seg_idx, piece_id));
    }

    // Build PiecePayloads per peer and distribute in parallel using send_pieces()
    let mut peer_futures = Vec::new();
    for (peer, piece_refs) in peer_piece_indices {
        let command_tx_clone = command_tx.clone();
        let client_clone = client.clone();
        let manifest_clone = manifest.clone();

        peer_futures.push(tokio::spawn(async move {
            // Read all pieces from store
            let mut payloads = Vec::new();
            let mut read_failed = Vec::new();
            for (seg_idx, piece_id) in &piece_refs {
                let (piece_data, coefficients) = {
                    let c = client_clone.lock().await;
                    match c.store().get_piece(&cid, *seg_idx, piece_id) {
                        Ok(d) => d,
                        Err(e) => {
                            warn!("[reannounce.rs] Failed to read piece {} seg {} for {}: {}", 
                                hex::encode(&piece_id[..4]), seg_idx, cid, e);
                            read_failed.push(*piece_id);
                            continue;
                        }
                    }
                };
                payloads.push(craftobj_transfer::PiecePayload {
                    segment_index: *seg_idx,
                    segment_count: manifest_clone.segment_count() as u32,
                    total_size: manifest_clone.total_size,
                    k: manifest_clone.k_for_segment(*seg_idx as usize) as u32,
                    vtags_cid: manifest_clone.vtags_cid,
                    piece_id: *piece_id,
                    coefficients,
                    data: piece_data,
                    vtag_blob: None,
                });
            }

            if payloads.is_empty() {
                return (peer, 0usize, read_failed.len(), read_failed);
            }

            let _payload_count = payloads.len();
            let (reply_tx, reply_rx) = oneshot::channel();
            let cmd = CraftObjCommand::DistributePieces {
                peer_id: peer,
                content_id: cid,
                pieces: payloads,
                reply_tx,
            };

            if command_tx_clone.send(cmd).is_err() {
                warn!("[reannounce.rs] Command channel closed for peer {}", peer);
                let failed: Vec<_> = piece_refs.iter().map(|(_, pid)| *pid).collect();
                return (peer, 0, failed.len(), failed);
            }

            // send_pieces handles all batching internally — just wait for final result
            match reply_rx.await {
                Ok(Ok(result)) => {
                    info!("[reannounce.rs] Peer {}: {}/{} confirmed, {} failed", 
                        peer, result.total_confirmed, result.total_sent, result.total_failed);
                    let failed_count = result.total_failed + read_failed.len();
                    (peer, result.total_confirmed, failed_count, read_failed)
                }
                Ok(Err(e)) => {
                    warn!("[reannounce.rs] Peer {}: send_pieces error: {}", peer, e);
                    let failed: Vec<_> = piece_refs.iter().map(|(_, pid)| *pid).collect();
                    (peer, 0, failed.len(), failed)
                }
                Err(_) => {
                    warn!("[reannounce.rs] Peer {}: reply channel dropped", peer);
                    let failed: Vec<_> = piece_refs.iter().map(|(_, pid)| *pid).collect();
                    (peer, 0, failed.len(), failed)
                }
            }
        }));
    }

    // Wait for all peer distribution tasks
    let mut total_pushed = 0usize;
    let mut _total_failed_count = 0usize;
    let mut failed_pieces: Vec<[u8; 32]> = Vec::new();

    let peer_results = futures::future::join_all(peer_futures).await;
    for result in peer_results {
        match result {
            Ok((peer, successes, failures, failed_pids)) => {
                total_pushed += successes;
                _total_failed_count += failures;
                failed_pieces.extend(failed_pids);
                info!("[reannounce.rs] Peer {}: {} successes, {} failures", peer, successes, failures);
            }
            Err(e) => {
                warn!("[reannounce.rs] Peer distribution task panicked: {:?}", e);
            }
        }
    }

    let _ = event_tx.send(DaemonEvent::DistributionProgress {
        content_id: content_id.to_hex(),
        pieces_pushed: total_pushed,
        total_pieces,
        peers_active: manifest_ok_peers.len(),
    });

    // Retry failed pieces with different peers
    if !failed_pieces.is_empty() && manifest_ok_peers.len() > 1 {
        info!("[reannounce.rs] Retrying {} failed pieces with alternate peers", failed_pieces.len());

        let mut retry_peer_pieces: std::collections::HashMap<libp2p::PeerId, Vec<(u32, [u8; 32])>> = std::collections::HashMap::new();
        for (i, piece_id) in failed_pieces.iter().enumerate() {
            let retry_peer = manifest_ok_peers[(i + 1) % manifest_ok_peers.len()];
            let seg_idx = all_pieces.iter().find(|(_, pid)| pid == piece_id).map(|(s, _)| *s).unwrap_or(0);
            retry_peer_pieces.entry(retry_peer).or_default().push((seg_idx, *piece_id));
        }

        let mut retry_futures = Vec::new();
        for (retry_peer, piece_refs) in retry_peer_pieces {
            let command_tx_clone = command_tx.clone();
            let client_clone = client.clone();
            let seg_count = manifest.segment_count() as u32;
            let manifest_for_retry = manifest.clone();
            retry_futures.push(tokio::spawn(async move {
                let mut payloads = Vec::new();
                for (seg_idx, piece_id) in &piece_refs {
                    let (data, coefficients) = {
                        let c = client_clone.lock().await;
                        match c.store().get_piece(&cid, *seg_idx, piece_id) {
                            Ok(d) => d,
                            Err(_) => continue,
                        }
                    };
                    payloads.push(craftobj_transfer::PiecePayload {
                        segment_index: *seg_idx,
                        segment_count: seg_count,
                        total_size: manifest_for_retry.total_size,
                        k: manifest_for_retry.k_for_segment(*seg_idx as usize) as u32,
                        vtags_cid: manifest_for_retry.vtags_cid,
                        piece_id: *piece_id,
                        coefficients,
                        data,
                        vtag_blob: None,
                    });
                }
                if payloads.is_empty() { return 0usize; }

                let (reply_tx, reply_rx) = oneshot::channel();
                let cmd = CraftObjCommand::DistributePieces {
                    peer_id: retry_peer,
                    content_id: cid,
                    pieces: payloads,
                    reply_tx,
                };
                if command_tx_clone.send(cmd).is_err() { return 0; }
                match reply_rx.await {
                    Ok(Ok(result)) => result.total_confirmed,
                    _ => 0,
                }
            }));
        }

        let retry_results = futures::future::join_all(retry_futures).await;
        let retry_successes: usize = retry_results.into_iter().filter_map(|r| r.ok()).sum();
        info!("[reannounce.rs] Retry completed: {}/{} pieces recovered", retry_successes, failed_pieces.len());
        total_pushed += retry_successes;
    } else if !failed_pieces.is_empty() {
        warn!("[reannounce.rs] {} pieces failed — no alternative peers for retry", failed_pieces.len());
    }

    info!("[reannounce.rs] Distribution complete for {}: pushed {}/{} pieces to {} peers", content_id, total_pushed, total_pieces, ranked_peers.len());

    if total_pushed > 0 {
        let _ = event_tx.send(DaemonEvent::ContentDistributed {
            content_id: content_id.to_hex(),
            pieces_pushed: total_pushed,
            total_pieces,
            target_peers: ranked_peers.len(),
        });
    }

    // Mark initial push done once ALL pieces are distributed.
    // Publisher retains local pieces — it is a storage node too.
    // Equalization is the path that moves pieces; we do not delete here.
    if total_pushed == total_pieces {
        let mut t = tracker.lock().await;
        t.mark_initial_push_done(content_id);
        info!("[reannounce.rs] Initial push complete for {}: distributed {}/{} pieces", content_id, total_pushed, total_pieces);
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
    demand_tracker: &Arc<Mutex<DemandTracker>>,
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
        // Demand gate: only push to new providers when there is active local demand.
        // No demand → no scaling. Demand + new provider available → push.
        let has_demand = {
            let dt = demand_tracker.lock().await;
            dt.has_demand(&content_id)
        };
        if !has_demand {
            debug!("Equalization for {}: no local demand — skipping push", content_id);
            continue;
        }

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

        // Group pieces by target peer
        let mut peer_payloads: std::collections::HashMap<libp2p::PeerId, Vec<craftobj_transfer::PiecePayload>> = std::collections::HashMap::new();
        for (i, (seg, pid)) in pieces_to_push.iter().enumerate() {
            let peer = candidates[i / per_peer % candidates.len()];
            let (piece_data, coefficients) = {
                let c = client.lock().await;
                match c.store().get_piece(&content_id, *seg, pid) {
                    Ok(d) => d,
                    Err(_) => continue,
                }
            };
            peer_payloads.entry(peer).or_default().push(craftobj_transfer::PiecePayload {
                segment_index: *seg,
                segment_count: manifest.segment_count() as u32,
                total_size: manifest.total_size,
                k: manifest.k_for_segment(*seg as usize) as u32,
                vtags_cid: manifest.vtags_cid,
                piece_id: *pid,
                coefficients,
                data: piece_data,
                vtag_blob: None,
            });
        }

        // Distribute in parallel using send_pieces() via DistributePieces
        let mut eq_futures = Vec::new();
        for (peer, payloads) in peer_payloads {
            let (reply_tx, reply_rx) = oneshot::channel();
            let cmd = CraftObjCommand::DistributePieces {
                peer_id: peer,
                content_id,
                pieces: payloads,
                reply_tx,
            };
            if command_tx.send(cmd).is_ok() {
                eq_futures.push(reply_rx);
            }
        }

        let mut total_pushed = 0usize;
        let results = futures::future::join_all(eq_futures).await;
        for result in results {
            if let Ok(Ok(tr)) = result {
                total_pushed += tr.total_confirmed;
            }
        }

        // Storage providers keep their pieces after equalization — they need them for serving.
        // Only the initial publisher push deletes local copies (publisher is not a storage node).
        // Equalization COPIES pieces to new nodes, it does NOT move them.

        if total_pushed > 0 {
            debug!("Equalization: pushed {} pieces for {} to {} peers", total_pushed, content_id, candidates.len());
            let _ = event_tx.send(DaemonEvent::ScalingPush {
                content_id: content_id.to_hex(),
                pieces_pushed: total_pushed,
                new_providers: candidates.len(),
            });
            // Mark equalized so we don't re-push this CID for another cooldown period.
            {
                let mut t = tracker.lock().await;
                t.mark_equalized(&content_id);
                t.save();
            }
        }
    }
}
