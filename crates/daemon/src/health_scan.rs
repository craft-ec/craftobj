//! HealthScan — periodic scan of owned segments for repair and degradation.
//!
//! For each owned segment, polls providers via `HealthQuery` (direct stream)
//! to get a total piece count across the network.
//! If count < target_piece_count(k) → repair.
//! If count > target_piece_count(k) AND no demand → degrade.
//!
//! No gossip. No PieceMap. No Merkle diff state machine.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use craftobj_core::{ContentId, HealthAction, HealthSnapshot, SegmentSnapshot};
use std::io::{BufRead, Write};
use craftobj_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::CraftObjCommand;
use crate::events::{DaemonEvent, EventSender};
use crate::scaling::DemandTracker;

// ---------------------------------------------------------------------------
// VtagCheckResult — result of a homomorphic piece spot-check
// ---------------------------------------------------------------------------

/// Result returned by `vtag_spot_check`.
#[derive(Debug)]
enum VtagCheckResult {
    /// The sampled piece passed the homomorphic hash check.
    Pass,
    /// The sampled piece failed — indicates silent corruption.
    Fail { piece_id: [u8; 32] },
    /// vtag blob not available locally — can't check, skip.
    NoVtagBlob,
}

/// Minimum pieces a node must keep per segment.
const MIN_PIECES_PER_SEGMENT: usize = 2;

/// Default scan interval in seconds (5 minutes).
pub const DEFAULT_SCAN_INTERVAL_SECS: u64 = 300;

/// Maximum age of health snapshots to keep (24 hours).
const SNAPSHOT_MAX_AGE_MS: u64 = 24 * 60 * 60 * 1000;

// ---------------------------------------------------------------------------
// HealthScan
// ---------------------------------------------------------------------------

/// HealthScan periodically scans owned segments and triggers repair/degradation.
///
/// Remote piece counts are obtained on-demand via `HealthQuery` — a lightweight
/// direct-stream request that returns the peer's piece count for a
/// (CID, segment) pair. No gossip, no cached PieceMap.
pub struct HealthScan {
    store: Arc<Mutex<FsStore>>,
    demand_tracker: Arc<Mutex<DemandTracker>>,
    local_peer_id: PeerId,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    /// Optional broadcast sender for real-time repair events to the UI.
    event_tx: Option<EventSender>,
    scan_interval: Duration,
    data_dir: Option<PathBuf>,
    /// Current position in the sorted CID list for rotational scanning.
    rotation_cursor: usize,
}

impl HealthScan {
    /// Create a new HealthScan.
    pub fn new(
        store: Arc<Mutex<FsStore>>,
        demand_tracker: Arc<Mutex<DemandTracker>>,
        local_peer_id: PeerId,
        command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    ) -> Self {
        Self {
            store,
            demand_tracker,
            local_peer_id,
            command_tx,
            event_tx: None,
            scan_interval: Duration::from_secs(DEFAULT_SCAN_INTERVAL_SECS),
            data_dir: None,
            rotation_cursor: 0,
        }
    }

    /// Attach an event sender so repair events are broadcast to the UI.
    pub fn set_event_tx(&mut self, tx: EventSender) {
        self.event_tx = Some(tx);
    }

    /// Set custom scan interval.
    pub fn set_scan_interval(&mut self, interval: Duration) {
        self.scan_interval = interval;
    }

    /// Set data directory for health history persistence.
    pub fn set_data_dir(&mut self, dir: PathBuf) {
        self.data_dir = Some(dir);
    }

    /// Get the scan interval.
    pub fn scan_interval(&self) -> Duration {
        self.scan_interval
    }

    // -----------------------------------------------------------------------
    // Helpers: DHT resolve + HealthQuery
    // -----------------------------------------------------------------------

    /// Resolve DHT providers for a content ID (excluding self).
    async fn resolve_providers(&self, cid: ContentId) -> Vec<PeerId> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .command_tx
            .send(CraftObjCommand::ResolveProviders {
                content_id: cid,
                reply_tx: tx,
            })
            .is_err()
        {
            warn!("HealthScan: failed to send ResolveProviders command");
            return Vec::new();
        }
        match rx.await {
            Ok(Ok(peers)) => peers.into_iter().filter(|p| *p != self.local_peer_id).collect(),
            Ok(Err(e)) => {
                debug!("HealthScan: ResolveProviders error for {}: {}", cid, e);
                Vec::new()
            }
            Err(_) => Vec::new(),
        }
    }

    /// Query a single peer for its piece count for (cid, segment).
    /// Returns 0 on timeout/error. Generates a fresh random nonce per call.
    async fn health_query(&self, peer_id: PeerId, cid: ContentId, segment: u32) -> Option<u32> {
        use rand::Rng;
        let nonce: [u8; 32] = rand::thread_rng().gen();
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .command_tx
            .send(CraftObjCommand::HealthQuery {
                peer_id,
                content_id: cid,
                segment_index: segment,
                nonce,
                reply_tx: tx,
            })
            .is_err()
        {
            return None;
        }
        match tokio::time::timeout(std::time::Duration::from_secs(5), rx).await {
            Ok(Ok(Ok(count))) => Some(count),
            Ok(Ok(Err(_))) | Ok(Err(_)) => None, // peer error
            Err(_) => None,                        // timeout — peer is slow/dead
        }
    }

    /// Query all providers for (cid, segment) and return total network piece count.
    /// Peers that fail or time out are skipped for the rest of this scan call.
    async fn query_network_count(&self, cid: ContentId, segment: u32) -> (u32, usize) {
        let providers = self.resolve_providers(cid).await;
        if providers.is_empty() {
            return (0, 0);
        }
        let provider_count = providers.len();
        let mut total: u32 = 0;
        for peer in providers {
            if let Some(count) = self.health_query(peer, cid, segment).await {
                total += count;
            } else {
                debug!("HealthScan: peer {} timed out / failed for {}/seg{} — skipping", peer, cid, segment);
            }
        }
        (total, provider_count)
    }

    // -----------------------------------------------------------------------
    // Scan loop
    // -----------------------------------------------------------------------

    /// Immediately scan all segments for a specific CID, bypassing the rotation batch.
    ///
    /// Called by the repair queue drain loop when a `TriggerRepair` command arrives
    /// (e.g. after a PDP challenge failure).
    pub async fn run_scan_for_cid(&mut self, cid: &ContentId) {
        let segments: Vec<u32> = {
            let s = self.store.lock().await;
            s.list_segments(cid).unwrap_or_default()
        };
        if segments.is_empty() {
            debug!("HealthScan::run_scan_for_cid: no local segments for {}", cid);
            return;
        }
        info!("HealthScan::run_scan_for_cid: scanning {} segments for {}", segments.len(), cid);
        for seg in segments {
            let _ = self.scan_segment(*cid, seg).await;
        }
    }

    /// Run a single scan over all owned segments.
    ///
    /// 1. List CIDs from local store.
    /// 2. For each segment in the 1% batch: query network, repair or degrade.
    /// 3. Persist health snapshots.
    pub async fn run_scan(&mut self) {
        // List all CIDs this node holds pieces for
        let all_cids_sorted: Vec<ContentId> = {
            let s = self.store.lock().await;
            let mut cids = s.list_content().unwrap_or_default();
            cids.sort_by(|a, b| a.0.cmp(&b.0));
            cids
        };

        if all_cids_sorted.is_empty() {
            debug!("HealthScan: no owned segments to scan");
            return;
        }

        // 1% rotation batch (at least 1)
        let total = all_cids_sorted.len();
        let batch_size = (total / 100).max(1);
        let cursor = self.rotation_cursor % total;
        let end = std::cmp::min(cursor + batch_size, total);
        let batch_cids = &all_cids_sorted[cursor..end];

        info!(
            "HealthScan: scanning batch {}..{} of {} CIDs (1% rotation)",
            cursor, end, total
        );

        self.rotation_cursor = if end >= total { 0 } else { end };

        // Collect owned (cid, segment) pairs from local store
        let owned_segments: Vec<(ContentId, u32)> = {
            let s = self.store.lock().await;
            let mut out = Vec::new();
            for cid in batch_cids {
                if let Ok(segs) = s.list_segments(cid) {
                    for seg in segs {
                        out.push((*cid, seg));
                    }
                }
            }
            out
        };

        if owned_segments.is_empty() {
            debug!("HealthScan: no owned segments in current batch");
            return;
        }

        debug!("HealthScan: scanning {} owned segments", owned_segments.len());

        // Group by CID for snapshot generation
        let mut cid_segments: HashMap<ContentId, Vec<u32>> = HashMap::new();
        for (cid, seg) in &owned_segments {
            cid_segments.entry(*cid).or_default().push(*seg);
        }

        let mut actions_by_cid: HashMap<ContentId, Vec<HealthAction>> = HashMap::new();

        for (cid, segment) in &owned_segments {
            let action = self.scan_segment(*cid, *segment).await;
            if let Some(a) = action {
                actions_by_cid.entry(*cid).or_default().push(a);
            }
        }

        // Persist snapshots
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let default_k = craftobj_core::SEGMENT_SIZE.div_ceil(craftobj_core::PIECE_SIZE);

        for (cid, segments) in &cid_segments {
            // Prefer piece header for k; fall back to manifest
            let header_k = {
                let s = self.store.lock().await;
                s.get_any_piece_header(cid).ok().flatten().map(|h| h.k as usize)
            };
            let manifest = {
                let s = self.store.lock().await;
                s.get_record(cid).ok()
            };

            let mut seg_snapshots = Vec::new();
            let mut min_ratio = f64::MAX;

            for &seg in segments {
                let (local_count, local_rank) = {
                    let s = self.store.lock().await;
                    let piece_ids = s.list_pieces(cid, seg).unwrap_or_default();
                    let count = piece_ids.len();
                    // Compute true rank via GF(256) Gaussian elimination on coefficient vectors.
                    // This detects silent linear dependence (dependent repair pieces, duplicates).
                    let coeffs: Vec<Vec<u8>> = piece_ids.iter().filter_map(|pid| {
                        s.get_piece(cid, seg, pid).ok().map(|(_, coeff)| coeff)
                    }).collect();
                    let rank = if coeffs.len() >= 2 {
                        craftec_erasure::check_independence(&coeffs)
                    } else {
                        coeffs.len() // 0 or 1 piece → rank = count
                    };
                    (count as u32, rank)
                };
                let (remote_total, provider_count) = self.query_network_count(*cid, seg).await;
                let total_pieces = local_count + remote_total;
                let k = header_k
                    .or_else(|| manifest.as_ref().map(|m| m.k_for_segment(seg as usize)))
                    .unwrap_or(default_k);
                // health_ratio uses TRUE local rank, not raw piece count
                let ratio = if k > 0 { local_rank as f64 / k as f64 } else { 0.0 };
                if ratio < min_ratio {
                    min_ratio = ratio;
                }
                seg_snapshots.push(SegmentSnapshot {
                    index: seg,
                    rank: local_rank,   // GF(256) independence rank — the actual decodability metric
                    k,
                    total_pieces: total_pieces as usize,
                    provider_count: provider_count + if local_count > 0 { 1 } else { 0 },
                });
            }

            // Total distinct providers for this CID across all segments
            let (_, any_provider_count) = self.query_network_count(*cid, 0).await;

            let snapshot = HealthSnapshot {
                timestamp: now,
                content_id: *cid,
                segment_count: segments.len(),
                segments: seg_snapshots,
                provider_count: any_provider_count + 1, // +1 for self
                health_ratio: if min_ratio == f64::MAX { 0.0 } else { min_ratio },
                actions: actions_by_cid.remove(cid).unwrap_or_default(),
            };

            self.persist_snapshot(&snapshot);
        }
    }

    // -----------------------------------------------------------------------
    // vtag homomorphic spot-check
    // -----------------------------------------------------------------------

    /// Pick one random local piece per scan cycle and verify it against the
    /// stored homomorphic segment hashes. Runs in O(piece_size * HASH_PROJECTIONS)
    /// time — negligible compared to DHT queries.
    async fn vtag_spot_check(
        &self,
        cid: &ContentId,
        segment: u32,
        vtag_cid: &[u8; 32],
    ) -> VtagCheckResult {
        // Load the vtag blob from local store
        let vtag_bytes = {
            let s = self.store.lock().await;
            match s.get_vtag_blob(vtag_cid) {
                Ok(b) => b,
                Err(_) => return VtagCheckResult::NoVtagBlob,
            }
        };

        // Deserialize ContentVerificationRecord (bincode)
        let record: craftec_erasure::homomorphic::ContentVerificationRecord =
            match bincode::deserialize(&vtag_bytes) {
                Ok(r) => r,
                Err(e) => {
                    warn!("HealthScan: could not deserialize vtag blob for {}: {}", cid, e);
                    return VtagCheckResult::NoVtagBlob;
                }
            };

        // Get the SegmentHashes for this specific segment
        let seg_hashes = match record.segment_hashes.get(segment as usize) {
            Some(h) => h,
            None => {
                debug!("HealthScan: vtag blob has no hashes for segment {} of {}", segment, cid);
                return VtagCheckResult::NoVtagBlob;
            }
        };

        // Load one random local piece (deterministic: unix_secs % piece_count)
        let (piece_ids, piece_data) = {
            let s = self.store.lock().await;
            let mut ids = s.list_pieces(cid, segment).unwrap_or_default();
            if ids.is_empty() {
                return VtagCheckResult::Pass; // nothing to check
            }
            // Sort for determinism, rotate by time for sampling variety across cycles
            ids.sort_unstable();
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let pick = (now_secs as usize) % ids.len();
            let chosen_id = ids[pick];
            match s.get_piece(cid, segment, &chosen_id) {
                Ok((data, coeff)) => (chosen_id, Some((data, coeff))),
                Err(_) => (chosen_id, None),
            }
        };

        let (data, coefficients) = match piece_data {
            Some(pd) => pd,
            None => return VtagCheckResult::Pass, // couldn't load — skip
        };

        let coded = craftec_erasure::CodedPiece { data, coefficients };

        if craftec_erasure::homomorphic::verify_piece(&coded, seg_hashes) {
            VtagCheckResult::Pass
        } else {
            VtagCheckResult::Fail { piece_id: piece_ids }
        }
    }

    /// Scan a single segment for repair or degradation needs.
    async fn scan_segment(&self, cid: ContentId, segment: u32) -> Option<HealthAction> {
        let (local_count, local_rank) = {
            use sha2::{Digest, Sha256};
            let s = self.store.lock().await;
            let piece_ids = s.list_pieces(&cid, segment).unwrap_or_default();
            let count = piece_ids.len();
            let mut valid_coeffs: Vec<Vec<u8>> = Vec::new();
            for pid in &piece_ids {
                if let Ok((_, coeff)) = s.get_piece(&cid, segment, pid) {
                    // Coefficient commitment check: SHA-256(coeff) must equal the piece_id.
                    // Mismatch means the stored coefficient vector is corrupt/tampered.
                    let computed: [u8; 32] = Sha256::digest(&coeff).into();
                    if &computed != pid {
                        warn!(
                            "HealthScan: coeff commitment FAIL for {}/seg{} piece={} — dropping corrupt piece",
                            cid, segment, hex::encode(&pid[..8])
                        );
                        // Drop the piece so it doesn't pollute rank or get served to peers
                        if let Err(e) = s.delete_piece(&cid, segment, pid) {
                            warn!("HealthScan: failed to delete corrupt piece: {}", e);
                        }
                    } else {
                        valid_coeffs.push(coeff);
                    }
                }
            }
            let rank = if valid_coeffs.len() >= 2 {
                craftec_erasure::check_independence(&valid_coeffs)
            } else {
                valid_coeffs.len()
            };
            (count, rank)
        };
        let local_node = self.local_peer_id.to_bytes();

        let (remote_total, _provider_count) = self.query_network_count(cid, segment).await;

        // Read k from piece header (self-describing) instead of manifest
        let (k, vtags_cid) = {
            let s = self.store.lock().await;
            match s.get_any_piece_header(&cid) {
                Ok(Some(header)) => (header.k as usize, header.vtags_cid),
                _ => {
                    // Fallback: try manifest, then default
                    let k = s.get_record(&cid)
                        .ok()
                        .map(|m| m.k_for_segment(segment as usize))
                        .unwrap_or(4); // default k
                    (k, None)
                }
            }
        };

        // Network health = local RANK (not raw count) + remote reported count
        // Using rank here surfaces silent dependence at the local node level.
        let total_network_rank = local_rank + remote_total as usize;

        // Kernel target: redundancy(k) = 2.0 + 16/k
        // Tier multipliers are COM layer concerns, not kernel.
        let target_pieces = craftobj_core::target_piece_count(k as u32) as usize;

        debug!(
            "HealthScan: {}/seg{} k={} target={} network_rank={} local_count={} local_rank={} vtags={}",
            cid, segment, k, target_pieces, total_network_rank, local_count, local_rank,
            if vtags_cid.is_some() { "erasure" } else { "replicate" }
        );

        // vtag homomorphic spot-check:
        // If we hold vtags for this CID, verify one random local piece via the
        // homomorphic inner-product check. A failure indicates silent corruption
        // and triggers immediate repair even if piece counts look healthy.
        if let Some(vtag_cid_bytes) = vtags_cid {
            if local_count > 0 {
                let spot_result = self.vtag_spot_check(&cid, segment, &vtag_cid_bytes).await;
                match spot_result {
                    VtagCheckResult::Pass => {
                        debug!("HealthScan: vtag spot-check PASS for {}/seg{}", cid, segment);
                    }
                    VtagCheckResult::Fail { piece_id } => {
                        warn!(
                            "HealthScan: vtag spot-check FAIL for {}/seg{} piece={} — silent corruption detected, forcing repair",
                            cid, segment, hex::encode(piece_id)
                        );
                        // Drop the corrupt piece so it doesn't pollute the segment
                        {
                            let s = self.store.lock().await;
                            if let Err(e) = s.delete_piece(&cid, segment, &piece_id) {
                                warn!("HealthScan: could not delete corrupt piece: {}", e);
                            }
                        }
                        // Force repair regardless of network count
                        self.attempt_repair(cid, segment, &local_node, 0).await;
                        return Some(HealthAction::Repaired { segment, offset: 0 });
                    }
                    VtagCheckResult::NoVtagBlob => {
                        debug!("HealthScan: vtag blob not available locally for {}/seg{} — skipping spot-check", cid, segment);
                    }
                }
            }
        }

        // Under-replication → repair (based on true network rank)
        if total_network_rank < target_pieces && local_count >= MIN_PIECES_PER_SEGMENT {
            // Deterministic: sort providers by piece count (piece is the same for all),
            // use local position. Since we have no per-provider counts here, use
            // a position of 0 (self is always willing to repair when under-replicated).
            self.attempt_repair(cid, segment, &local_node, 0).await;
            return Some(HealthAction::Repaired {
                segment,
                offset: 0,
            });
        }

        // Over-replication → degrade if no demand (based on raw network count)
        // Use raw remote + local pieces for over-replication: rank can't exceed k,
        // but raw count above target still burns bandwidth.
        let total_network_pieces = local_count + remote_total as usize;
        if total_network_pieces > target_pieces && local_rank > MIN_PIECES_PER_SEGMENT {
            let has_demand = {
                let dt = self.demand_tracker.lock().await;
                dt.has_demand(&cid)
            };
            if !has_demand {
                debug!(
                    "HealthScan: {}/seg{} over-replicated (pieces={}, target={}), degrading",
                    cid, segment, total_network_pieces, target_pieces
                );
                self.attempt_degradation(cid, segment).await;
                return Some(HealthAction::Degraded { segment });
            }
        }

        None
    }

    /// Attempt to repair a segment.
    ///
    /// Strategy depends on vtags_cid from piece header:
    /// - `Some(vtags_cid)` → erasure extension (RLNC recombination) + distribute new piece
    /// - `None` → replication (copy blob to more nodes)
    async fn attempt_repair(
        &self,
        cid: ContentId,
        segment: u32,
        _local_node: &[u8],
        _offset: usize,
    ) {
        use craftobj_transfer::PiecePayload;
        use std::collections::HashSet;

        // Determine repair strategy from piece header (also captures metadata for payload)
        let (vtags_cid, total_size, segment_count, k) = {
            let s = self.store.lock().await;
            let header = s.get_any_piece_header(&cid).ok().flatten();
            let vtags = header.as_ref().and_then(|h| h.vtags_cid);
            let ts = header.as_ref().map(|h| h.total_size).unwrap_or(0);
            let sc = header.as_ref().map(|h| h.segment_count).unwrap_or(1);
            let kv = header.as_ref().map(|h| h.k).unwrap_or(0);
            (vtags, ts, sc, kv)
        };

        let strategy = if vtags_cid.is_some() { "erasure" } else { "replication" };
        info!("HealthScan: repair starting for {}/seg{} strategy={}", cid, segment, strategy);
        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(DaemonEvent::RepairStarted {
                content_id: cid.to_hex(),
                segment,
                strategy: strategy.to_string(),
            });
        }

        if vtags_cid.is_some() {
            // Erasure extension: capture existing piece IDs before healing to diff afterward.
            let pieces_before: HashSet<[u8; 32]> = {
                let s = self.store.lock().await;
                s.list_pieces(&cid, segment).unwrap_or_default().into_iter().collect()
            };

            // Generate 1 new RLNC piece
            let result = {
                let store_guard = self.store.lock().await;
                crate::health::heal_segment(&store_guard, &cid, segment, 1)
            };

            if result.pieces_generated == 0 {
                warn!(
                    "HealthScan erasure repair failed for {}/seg{}: {:?}",
                    cid, segment, result.errors
                );
                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(DaemonEvent::RepairCompleted {
                        content_id: cid.to_hex(),
                        segment,
                        pieces_generated: 0,
                        success: false,
                    });
                }
            } else {
                info!(
                    "HealthScan erasure repair for {}/seg{}: generated {} new piece(s)",
                    cid, segment, result.pieces_generated
                );

                // ── Distribute the new piece to a non-provider peer ──────────────
                // Diff: find piece IDs that don't exist in the pre-heal set.
                let new_piece_ids: Vec<[u8; 32]> = {
                    let s = self.store.lock().await;
                    s.list_pieces(&cid, segment)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|pid| !pieces_before.contains(pid))
                        .collect()
                };

                if !new_piece_ids.is_empty() {
                    // Build PiecePayload for each new piece
                    let payloads: Vec<PiecePayload> = {
                        let s = self.store.lock().await;
                        new_piece_ids.iter().filter_map(|pid| {
                            s.get_piece(&cid, segment, pid).ok().map(|(data, coeff)| PiecePayload {
                                segment_index: segment,
                                piece_id: *pid,
                                coefficients: coeff,
                                data,
                                total_size,
                                segment_count,
                                k,
                                vtags_cid,
                                vtag_blob: None,
                            })
                        }).collect()
                    };

                    if !payloads.is_empty() {
                        // Get connected peers and pick one not already a provider.
                        // (pieces_before is a reasonable proxy for "already has this CID")
                        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                        if self.command_tx.send(CraftObjCommand::ConnectedPeers { reply_tx }).is_ok() {
                            if let Ok(peer_strings) = tokio::time::timeout(
                                std::time::Duration::from_secs(3), reply_rx
                            ).await.unwrap_or(Ok(vec![])) {
                                // Pick first connected peer (already scorer-sorted in practice)
                                let target = peer_strings.iter()
                                    .find_map(|s| s.parse::<libp2p::PeerId>().ok());

                                if let Some(target_peer) = target {
                                    let (dist_tx, _dist_rx) = tokio::sync::oneshot::channel();
                                    let _ = self.command_tx.send(CraftObjCommand::DistributePieces {
                                        peer_id: target_peer,
                                        content_id: cid,
                                        pieces: payloads,
                                        reply_tx: dist_tx,
                                    });
                                    info!(
                                        "HealthScan: pushed repair piece for {}/seg{} to {}",
                                        cid, segment, &target_peer.to_string()[..8]
                                    );
                                }
                            }
                        }
                    }
                }

                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(DaemonEvent::RepairCompleted {
                        content_id: cid.to_hex(),
                        segment,
                        pieces_generated: result.pieces_generated,
                        success: true,
                    });
                }
            }
        } else {
            // Replication: push existing pieces to underserved peers.
            // No RLNC generation — just redistribute what we already hold locally.
            use craftobj_transfer::PiecePayload;

            let (piece_ids, total_size, segment_count, k, vtags_cid_val) = {
                let s = self.store.lock().await;
                let header = s.get_any_piece_header(&cid).ok().flatten();
                let ts = header.as_ref().map(|h| h.total_size).unwrap_or(0);
                let sc = header.as_ref().map(|h| h.segment_count).unwrap_or(1);
                let kv = header.as_ref().map(|h| h.k).unwrap_or(0);
                let vtags = header.as_ref().and_then(|h| h.vtags_cid);
                let pids = s.list_pieces(&cid, segment).unwrap_or_default();
                (pids, ts, sc, kv, vtags)
            };

            if piece_ids.is_empty() {
                warn!("HealthScan replication repair for {}/seg{}: no local pieces to push", cid, segment);
                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(DaemonEvent::RepairCompleted {
                        content_id: cid.to_hex(),
                        segment,
                        pieces_generated: 0,
                        success: false,
                    });
                }
            } else {
                // Build payloads from all locally-held pieces for this segment
                let payloads: Vec<PiecePayload> = {
                    let s = self.store.lock().await;
                    piece_ids.iter().filter_map(|pid| {
                        s.get_piece(&cid, segment, pid).ok().map(|(data, coeff)| PiecePayload {
                            segment_index: segment,
                            piece_id: *pid,
                            coefficients: coeff,
                            data,
                            total_size,
                            segment_count,
                            k,
                            vtags_cid: vtags_cid_val,
                            vtag_blob: None,
                        })
                    }).collect()
                };

                // Get known providers so we can push to a peer that isn't already one
                let known_providers: std::collections::HashSet<String> = {
                    let (rtx, rrx) = tokio::sync::oneshot::channel();
                    if self.command_tx.send(CraftObjCommand::ResolveProviders {
                        content_id: cid,
                        reply_tx: rtx,
                    }).is_ok() {
                        tokio::time::timeout(std::time::Duration::from_secs(3), rrx)
                            .await
                            .ok()
                            .and_then(|r| r.ok())
                            .and_then(|r| r.ok())
                            .unwrap_or_default()
                            .iter()
                            .map(|p| p.to_string())
                            .collect()
                    } else {
                        std::collections::HashSet::new()
                    }
                };

                // Find a connected peer not already a provider
                let (ctxr, crxr) = tokio::sync::oneshot::channel();
                let target = if self.command_tx.send(CraftObjCommand::ConnectedPeers { reply_tx: ctxr }).is_ok() {
                    tokio::time::timeout(std::time::Duration::from_secs(3), crxr)
                        .await
                        .ok()
                        .and_then(|r| r.ok())
                        .unwrap_or_default()
                        .into_iter()
                        .find_map(|s| {
                            let peer = s.parse::<libp2p::PeerId>().ok()?;
                            if !known_providers.contains(&peer.to_string()) { Some(peer) } else { None }
                        })
                } else {
                    None
                };

                if let Some(target_peer) = target {
                    let pushed = payloads.len();
                    let (dist_tx, _dist_rx) = tokio::sync::oneshot::channel();
                    let _ = self.command_tx.send(CraftObjCommand::DistributePieces {
                        peer_id: target_peer,
                        content_id: cid,
                        pieces: payloads,
                        reply_tx: dist_tx,
                    });
                    info!(
                        "HealthScan replication repair: pushed {} piece(s) for {}/seg{} to {}",
                        pushed, cid, segment, &target_peer.to_string()[..8]
                    );
                    if let Some(ref tx) = self.event_tx {
                        let _ = tx.send(DaemonEvent::RepairCompleted {
                            content_id: cid.to_hex(),
                            segment,
                            pieces_generated: pushed,
                            success: true,
                        });
                    }
                } else {
                    warn!(
                        "HealthScan replication repair for {}/seg{}: no non-provider peer available",
                        cid, segment
                    );
                    if let Some(ref tx) = self.event_tx {
                        let _ = tx.send(DaemonEvent::RepairCompleted {
                            content_id: cid.to_hex(),
                            segment,
                            pieces_generated: 0,
                            success: false,
                        });
                    }
                }
            }
        }

        // Publish per-segment DHT provider record for the repaired segment
        let pkey = craftobj_routing::provider_key(&cid, segment);
        let _ = self.command_tx.send(CraftObjCommand::StartProviding { key: pkey });
    }

    /// Attempt to degrade a segment by dropping 1 piece.
    ///
    /// Drops the most linearly dependent piece — one whose removal doesn't decrease
    /// the segment rank (i.e. it contributes no unique information). Falls back to
    /// alphabetical last if all pieces are found to be independent.
    async fn attempt_degradation(&self, cid: ContentId, segment: u32) {
        let store_guard = self.store.lock().await;
        let mut pieces = match store_guard.list_pieces(&cid, segment) {
            Ok(p) if p.len() > MIN_PIECES_PER_SEGMENT => p,
            _ => return,
        };

        // Load all coefficient vectors for rank computation
        let all_coeffs: Vec<(usize, Vec<u8>)> = pieces.iter().enumerate().filter_map(|(i, pid)| {
            store_guard.get_piece(&cid, segment, pid).ok().map(|(_, coeff)| (i, coeff))
        }).collect();

        // Find the first piece whose removal leaves rank unchanged (most dependent).
        // If all are independent, fall back to dropping alphabetically last.
        let piece_to_drop = if all_coeffs.len() >= 2 {
            let all_c: Vec<Vec<u8>> = all_coeffs.iter().map(|(_, c)| c.clone()).collect();
            let full_rank = craftec_erasure::check_independence(&all_c);

            all_coeffs.iter().find_map(|(idx, _)| {
                let reduced: Vec<Vec<u8>> = all_coeffs.iter()
                    .enumerate()
                    .filter(|(i, _)| i != idx)
                    .map(|(_, (_, c))| c.clone())
                    .collect();
                let reduced_rank = if reduced.len() >= 2 {
                    craftec_erasure::check_independence(&reduced)
                } else {
                    reduced.len()
                };
                // If rank is unchanged after removing this piece → it's dependent → safe to drop
                if reduced_rank >= full_rank {
                    Some(pieces[*idx])
                } else {
                    None
                }
            }).unwrap_or_else(|| {
                // All pieces are independent — drop alphabetically last (least valuable by key)
                pieces.sort();
                *pieces.last().unwrap()
            })
        } else {
            pieces.sort();
            *pieces.last().unwrap()
        };

        if let Err(e) = store_guard.delete_piece(&cid, segment, &piece_to_drop) {
            warn!(
                "HealthScan degradation failed for {}/seg{}: {}",
                cid, segment, e
            );
            return;
        }

        info!(
            "HealthScan degradation: dropped piece {} for {}/seg{}",
            hex::encode(&piece_to_drop[..8]),
            cid,
            segment
        );

        // If no pieces remain for this segment, remove ourselves as provider from the DHT.
        let remaining = store_guard.list_pieces(&cid, segment).unwrap_or_default().len();
        if remaining == 0 {
            let pkey = craftobj_routing::provider_key(&cid, segment);
            let _ = self.command_tx.send(CraftObjCommand::StopProviding { key: pkey });
            info!("HealthScan: no pieces remain for {}/seg{}, sent StopProviding", cid, segment);
        }
    }

    /// Persist a health snapshot to JSONL file.
    fn persist_snapshot(&self, snapshot: &HealthSnapshot) {
        let Some(ref data_dir) = self.data_dir else {
            return;
        };
        let dir = data_dir.join("health_history");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!("Failed to create health_history dir: {}", e);
            return;
        }
        let path = dir.join(format!("{}.jsonl", snapshot.content_id));
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to open snapshot file: {}", e);
                return;
            }
        };
        if let Ok(json) = serde_json::to_string(snapshot) {
            let _ = writeln!(file, "{}", json);
        }

        self.prune_snapshots(&path, snapshot.timestamp);
    }

    /// Remove snapshot entries older than SNAPSHOT_MAX_AGE_MS.
    fn prune_snapshots(&self, path: &std::path::Path, now: u64) {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return,
        };
        let cutoff = now.saturating_sub(SNAPSHOT_MAX_AGE_MS);
        let kept: Vec<&str> = content
            .lines()
            .filter(|line| {
                serde_json::from_str::<HealthSnapshot>(line)
                    .map(|s| s.timestamp >= cutoff)
                    .unwrap_or(false)
            })
            .collect();

        if kept.len() < content.lines().count() {
            let _ = std::fs::write(path, kept.join("\n") + "\n");
        }
    }

    /// Load health snapshots for a CID, optionally filtered by `since` (unix millis).
    pub fn load_snapshots(&self, cid: &ContentId, since: Option<u64>) -> Vec<HealthSnapshot> {
        let Some(ref data_dir) = self.data_dir else {
            return vec![];
        };
        let path = data_dir.join("health_history").join(format!("{}.jsonl", cid));
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return vec![],
        };
        let reader = std::io::BufReader::new(file);
        let cutoff = since.unwrap_or(0);
        reader
            .lines()
            .map_while(Result::ok)
            .filter_map(|line| serde_json::from_str::<HealthSnapshot>(&line).ok())
            .filter(|s| s.timestamp >= cutoff)
            .collect()
    }
}

/// Run the HealthScan loop as a periodic task.
pub async fn run_health_scan_loop(health_scan: Arc<Mutex<HealthScan>>) {
    // Initial delay to let the daemon stabilize
    tokio::time::sleep(Duration::from_secs(10)).await;

    let interval = {
        let hs = health_scan.lock().await;
        hs.scan_interval()
    };

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let mut hs = health_scan.lock().await;
        hs.run_scan().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craftobj_core::ContentId;

    fn make_tx() -> mpsc::UnboundedSender<CraftObjCommand> {
        let (tx, _rx) = mpsc::unbounded_channel();
        tx
    }

    fn setup_test() -> (
        Arc<Mutex<FsStore>>,
        Arc<Mutex<DemandTracker>>,
        PeerId,
        std::path::PathBuf,
    ) {
        let local = PeerId::random();
        let dir = std::env::temp_dir().join(format!(
            "health-scan-test-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        let store = FsStore::new(&dir).unwrap();
        (
            Arc::new(Mutex::new(store)),
            Arc::new(Mutex::new(DemandTracker::new())),
            local,
            dir,
        )
    }

    #[test]
    fn test_health_scan_creation() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let scan = HealthScan::new(store, dt, local, tx);
        assert_eq!(scan.scan_interval(), Duration::from_secs(300));
        std::fs::remove_dir_all(&dir).ok();
    }

    // test_set_tier_target removed — tier_target is a COM-layer concern,
    // not part of the kernel HealthScan.

    #[test]
    fn test_set_scan_interval() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(store, dt, local, tx);
        scan.set_scan_interval(Duration::from_secs(60));
        assert_eq!(scan.scan_interval(), Duration::from_secs(60));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_scan_empty() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx();
        let mut scan = HealthScan::new(store, dt, local, tx);
        // Should complete without error on empty store
        scan.run_scan().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_health_query_no_response() {
        let (store, dt, local, dir) = setup_test();
        let tx = make_tx(); // rx dropped immediately — simulates no handler
        let scan = HealthScan::new(store, dt, local, tx);
        let peer = PeerId::random();
        let cid = ContentId([1u8; 32]);
        // HealthQuery with no handler returns 0
        let count = scan.health_query(peer, cid, 0).await;
        assert_eq!(count, 0, "No handler should return 0");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_health_query_response() {
        let (store, dt, local, dir) = setup_test();
        let (tx, mut rx) = mpsc::unbounded_channel();

        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    CraftObjCommand::HealthQuery { reply_tx, .. } => {
                        let _ = reply_tx.send(Ok(7));
                    }
                    _ => {}
                }
            }
        });

        let scan = HealthScan::new(store, dt, local, tx);
        let peer = PeerId::random();
        let cid = ContentId([2u8; 32]);
        let count = scan.health_query(peer, cid, 0).await;
        assert_eq!(count, 7, "Should return the peer's piece count");
        std::fs::remove_dir_all(&dir).ok();
    }
}
