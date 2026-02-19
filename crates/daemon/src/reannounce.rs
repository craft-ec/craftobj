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

    // Use batched distribution to prevent yamux stream saturation.
    // Group pieces by peer, create batches of ~15 pieces, send one batch at a time per peer.
    // Multiple peers can be batched in parallel (one task per peer).
    let total_pieces = all_pieces.len();
    const PIECES_PER_BATCH: usize = 15; // Reasonable batch size: ~1.5MB per batch
    let progress_interval = std::cmp::max(total_pieces / 20, 10); // ~5% or every 10

    info!("[reannounce.rs] Initial push for {}: distributing {} pieces via batching to {} peers", 
        content_id, total_pieces, manifest_ok_peers.len());

    let cid = *content_id;
    
    // Group all pieces by target peer (round-robin distribution)
    let mut peer_pieces: std::collections::HashMap<libp2p::PeerId, Vec<(u32, [u8; 32])>> = std::collections::HashMap::new();
    for (i, (seg_idx, piece_id)) in all_pieces.iter().enumerate() {
        let peer = manifest_ok_peers[i % manifest_ok_peers.len()];
        peer_pieces.entry(peer).or_insert_with(Vec::new).push((*seg_idx, *piece_id));
    }

    info!("[reannounce.rs] Initial push for {}: grouped pieces across {} peers", content_id, peer_pieces.len());

    // Create parallel distribution tasks (one per peer)
    let mut peer_futures = Vec::new();
    for (peer, pieces) in peer_pieces {
        let command_tx_clone = command_tx.clone();
        let client_clone = client.clone();
        let peer_copy = peer;
        let pieces_copy = pieces.clone();

        peer_futures.push(tokio::spawn(async move {
            let mut total_peer_success = 0usize;
            let mut total_peer_failed = 0usize;
            let mut all_failed_pieces = Vec::new();

            // Create batches of pieces for this peer
            let batches: Vec<_> = pieces_copy.chunks(PIECES_PER_BATCH).collect();
            let total_batches = batches.len();
            
            info!("[reannounce.rs] Peer {}: processing {} batches of pieces", peer_copy, total_batches);

            for (batch_idx, batch) in batches.into_iter().enumerate() {
                // Build PiecePayload for each piece in the batch
                let mut piece_payloads = Vec::new();
                for &(seg_idx, piece_id) in batch {
                    let (piece_data, coefficients) = {
                        let c = client_clone.lock().await;
                        match c.store().get_piece(&cid, seg_idx, &piece_id) {
                            Ok(d) => d,
                            Err(e) => {
                                warn!("[reannounce.rs] Failed to read piece {} seg {} for {}: {}", 
                                    hex::encode(&piece_id[..4]), seg_idx, cid, e);
                                all_failed_pieces.push(piece_id);
                                continue;
                            }
                        }
                    };
                    
                    piece_payloads.push(craftobj_transfer::PiecePayload {
                        segment_index: seg_idx,
                        piece_id,
                        coefficients,
                        data: piece_data,
                    });
                }

                if piece_payloads.is_empty() {
                    continue;
                }

                // Send batch via PushPieceBatch command
                let (reply_tx, reply_rx) = oneshot::channel();
                let cmd = CraftObjCommand::PushPieceBatch {
                    peer_id: peer_copy,
                    content_id: cid,
                    pieces: piece_payloads.clone(),
                    reply_tx,
                };

                if command_tx_clone.send(cmd).is_err() {
                    warn!("[reannounce.rs] Failed to send batch {} to {} - command channel closed", batch_idx, peer_copy);
                    for payload in piece_payloads {
                        all_failed_pieces.push(payload.piece_id);
                    }
                    continue;
                }

                // Wait for batch response with timeout
                match tokio::time::timeout(std::time::Duration::from_secs(30), reply_rx).await {
                    Ok(Ok(craftobj_transfer::CraftObjResponse::BatchAck { confirmed_pieces, failed_pieces })) => {
                        total_peer_success += confirmed_pieces.len();
                        total_peer_failed += failed_pieces.len();
                        all_failed_pieces.extend(failed_pieces);
                        
                        info!("[reannounce.rs] Peer {} batch {}: {}/{} pieces confirmed", 
                            peer_copy, batch_idx, confirmed_pieces.len(), piece_payloads.len());
                    }
                    Ok(Ok(craftobj_transfer::CraftObjResponse::Ack { status })) => {
                        if status == craftobj_core::WireStatus::Ok {
                            total_peer_success += piece_payloads.len();
                            info!("[reannounce.rs] Peer {} batch {}: all {} pieces confirmed (Ack)", 
                                peer_copy, batch_idx, piece_payloads.len());
                        } else {
                            let batch_size = piece_payloads.len();
                            total_peer_failed += batch_size;
                            for payload in piece_payloads {
                                all_failed_pieces.push(payload.piece_id);
                            }
                            warn!("[reannounce.rs] Peer {} batch {}: all {} pieces failed with status {:?}", 
                                peer_copy, batch_idx, batch_size, status);
                        }
                    }
                    Ok(Ok(other)) => {
                        warn!("[reannounce.rs] Peer {} batch {}: unexpected response {:?}", 
                            peer_copy, batch_idx, std::mem::discriminant(&other));
                        let batch_size = piece_payloads.len();
                        total_peer_failed += batch_size;
                        for payload in piece_payloads {
                            all_failed_pieces.push(payload.piece_id);
                        }
                    }
                    Ok(Err(_)) => {
                        warn!("[reannounce.rs] Peer {} batch {}: reply channel closed", peer_copy, batch_idx);
                        let batch_size = piece_payloads.len();
                        total_peer_failed += batch_size;
                        for payload in piece_payloads {
                            all_failed_pieces.push(payload.piece_id);
                        }
                    }
                    Err(_) => {
                        warn!("[reannounce.rs] Peer {} batch {}: timed out after 30s", peer_copy, batch_idx);
                        let batch_size = piece_payloads.len();
                        total_peer_failed += batch_size;
                        for payload in piece_payloads {
                            all_failed_pieces.push(payload.piece_id);
                        }
                    }
                }

                // No delay between batches — backpressure from ack-wait is sufficient.
                // The sender waits for each batch ack before sending the next one.
            }

            info!("[reannounce.rs] Peer {} completed: {} successes, {} failures", 
                peer_copy, total_peer_success, total_peer_failed);

            (peer_copy, total_peer_success, total_peer_failed, all_failed_pieces)
        }));
    }

    // Wait for all peer distribution tasks to complete
    let mut confirmed_pieces: Vec<[u8; 32]> = Vec::new();
    let mut failed_pieces: Vec<[u8; 32]> = Vec::new();
    let mut peer_success_count: std::collections::HashMap<libp2p::PeerId, usize> = std::collections::HashMap::new();
    let mut peer_failure_count: std::collections::HashMap<libp2p::PeerId, usize> = std::collections::HashMap::new();

    let peer_results = futures::future::join_all(peer_futures).await;
    for result in peer_results {
        match result {
            Ok((peer, successes, failures, failed_piece_ids)) => {
                peer_success_count.insert(peer, successes);
                peer_failure_count.insert(peer, failures);
                failed_pieces.extend(failed_piece_ids);
                
                // Emit progress events
                let total_confirmed = peer_success_count.values().sum::<usize>();
                if total_confirmed % progress_interval == 0 || total_confirmed + failures >= total_pieces {
                    let _ = event_tx.send(DaemonEvent::DistributionProgress {
                        content_id: content_id.to_hex(),
                        pieces_pushed: total_confirmed,
                        total_pieces,
                        peers_active: peer_success_count.len(),
                    });
                }
            }
            Err(e) => {
                warn!("[reannounce.rs] Peer distribution task failed: {:?}", e);
            }
        }
    }

    let mut total_pushed = peer_success_count.values().sum::<usize>();
    let failed_count = failed_pieces.len();

    info!("[reannounce.rs] Initial push for {}: {}/{} pieces confirmed via batching, {} failed", 
        content_id, total_pushed, total_pieces, failed_count);

    // Log per-peer success/failure stats
    for (peer, count) in &peer_success_count {
        let failures = peer_failure_count.get(peer).copied().unwrap_or(0);
        info!("[reannounce.rs] Peer {}: {} successes, {} failures", peer, count, failures);
    }

    // Retry failed pieces with different peers using batching
    if !failed_pieces.is_empty() && manifest_ok_peers.len() > 1 {
        info!("[reannounce.rs] Retrying {} failed pieces with different peers using batching", failed_pieces.len());
        
        // Group failed pieces by alternative peer (offset assignment)
        let mut retry_peer_pieces: std::collections::HashMap<libp2p::PeerId, Vec<[u8; 32]>> = std::collections::HashMap::new();
        for (i, piece_id) in failed_pieces.iter().enumerate() {
            // Try a different peer (offset by 1 from original round-robin)
            let retry_peer = manifest_ok_peers[(i + 1) % manifest_ok_peers.len()];
            retry_peer_pieces.entry(retry_peer).or_insert_with(Vec::new).push(*piece_id);
        }

        let mut retry_futures = Vec::new();
        for (retry_peer, retry_piece_ids) in retry_peer_pieces {
            let command_tx_clone = command_tx.clone();
            let client_clone = client.clone();
            let all_pieces_clone = all_pieces.clone();

            retry_futures.push(tokio::spawn(async move {
                let mut retry_success = 0usize;
                
                // Create retry batches
                let retry_batches: Vec<_> = retry_piece_ids.chunks(PIECES_PER_BATCH).collect();
                
                for batch in retry_batches {
                    let mut piece_payloads = Vec::new();
                    for &piece_id in batch {
                        // Find the segment index for this piece_id
                        let seg_idx = all_pieces_clone.iter()
                            .find(|(_, pid)| pid == &piece_id)
                            .map(|(seg, _)| *seg)
                            .unwrap_or(0);
                        
                        let (piece_data, coefficients) = {
                            let c = client_clone.lock().await;
                            match c.store().get_piece(&cid, seg_idx, &piece_id) {
                                Ok(d) => d,
                                Err(_) => continue,
                            }
                        };
                        
                        piece_payloads.push(craftobj_transfer::PiecePayload {
                            segment_index: seg_idx,
                            piece_id,
                            coefficients,
                            data: piece_data,
                        });
                    }

                    if piece_payloads.is_empty() {
                        continue;
                    }

                    let (reply_tx, reply_rx) = oneshot::channel();
                    let cmd = CraftObjCommand::PushPieceBatch {
                        peer_id: retry_peer,
                        content_id: cid,
                        pieces: piece_payloads.clone(),
                        reply_tx,
                    };

                    if command_tx_clone.send(cmd).is_err() {
                        continue;
                    }

                    match tokio::time::timeout(std::time::Duration::from_secs(30), reply_rx).await {
                        Ok(Ok(craftobj_transfer::CraftObjResponse::BatchAck { confirmed_pieces, .. })) => {
                            retry_success += confirmed_pieces.len();
                        }
                        Ok(Ok(craftobj_transfer::CraftObjResponse::Ack { status })) => {
                            if status == craftobj_core::WireStatus::Ok {
                                retry_success += piece_payloads.len();
                            }
                        }
                        _ => {
                            // Retry failed - no additional handling needed
                        }
                    }
                }
                
                retry_success
            }));
        }

        if !retry_futures.is_empty() {
            let retry_results = futures::future::join_all(retry_futures).await;
            let retry_successes: usize = retry_results.into_iter()
                .filter_map(|r| r.ok())
                .sum();
            
            info!("[reannounce.rs] Retry completed: {}/{} pieces recovered via batching", 
                retry_successes, failed_pieces.len());
            total_pushed += retry_successes;
        }
    } else if !failed_pieces.is_empty() {
        warn!("[reannounce.rs] {} pieces failed distribution - no alternative peers for retry", failed_pieces.len());
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
