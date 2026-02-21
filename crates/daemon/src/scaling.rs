//! Local Demand-Driven Scaling
//!
//! When a CID is fetched frequently by remote peers, this node creates a new
//! RLNC-recombined piece and pushes it to the highest-score storage node that
//! does not yet hold pieces for the CID.
//!
//! This is **entirely local** — no DemandSignal broadcasts, no coordination with
//! other nodes. Each node acts independently based on what IT observes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use craftobj_core::{ContentId, CraftObjCapability};
use craftobj_store::FsStore;
use libp2p::PeerId;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::commands::CraftObjCommand;

/// Minimum number of distinct CIDs with fetch data required before percentile
/// detection is meaningful. Below this count all fetched CIDs are treated as hot
/// (conservative: can't compute a distribution from fewer data points).
const MIN_CIDS_FOR_PERCENTILE: usize = 10;

/// Percentile cutoff: top 1% of CIDs by fetch rate are considered hot.
const HOT_PERCENTILE: f64 = 0.99;

/// Time window for measuring demand (seconds).
const DEMAND_WINDOW_SECS: u64 = 300; // 5 minutes

/// How often the scaling loop checks for hot content.
const SCALING_CHECK_INTERVAL_SECS: u64 = 30;

/// Minimum time between scaling pushes for the same CID.
const SCALING_COOLDOWN_SECS: u64 = 300; // 5 minutes

/// Tracks fetch request rates per CID to detect demand.
#[derive(Default)]
pub struct DemandTracker {
    /// Fetch timestamps per content ID.
    fetches: HashMap<ContentId, Vec<Instant>>,
}


impl DemandTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that we served a piece for this content.
    pub fn record_fetch(&mut self, content_id: ContentId) {
        self.fetches.entry(content_id).or_default().push(Instant::now());
    }

    /// Return hot CIDs using adaptive percentile detection.
    ///
    /// Algorithm:
    /// - Prune timestamps older than DEMAND_WINDOW_SECS.
    /// - If fewer than MIN_CIDS_FOR_PERCENTILE CIDs have data → treat all as hot
    ///   (conservative: no meaningful baseline exists).
    /// - Otherwise → hot = CIDs whose fetch count ≥ 99th percentile of all counts.
    ///
    /// Returns `Vec<(ContentId, fetch_count)>`.
    pub fn check_demand(&mut self) -> Vec<(ContentId, u32)> {
        let cutoff = Instant::now() - Duration::from_secs(DEMAND_WINDOW_SECS);

        // Prune stale timestamps and build (cid, count) list.
        self.fetches.retain(|_, timestamps| {
            timestamps.retain(|t| *t > cutoff);
            !timestamps.is_empty()
        });

        let counts: Vec<(ContentId, u32)> = self
            .fetches
            .iter()
            .map(|(cid, ts)| (*cid, ts.len() as u32))
            .collect();

        if counts.is_empty() {
            return vec![];
        }

        if counts.len() < MIN_CIDS_FOR_PERCENTILE {
            // Below minimum cardinality — no meaningful baseline to compare against.
            // Can't distinguish a spike from normal traffic with this few CIDs.
            // Skip scaling and wait until we serve enough distinct CIDs to rank them.
            return vec![];
        }

        // Compute the 99th percentile threshold from the actual distribution.
        // Non-parametric: no distribution assumption, works for Zipf/Pareto traffic.
        let mut sorted: Vec<u32> = counts.iter().map(|(_, c)| *c).collect();
        sorted.sort_unstable();
        let p99_idx = ((sorted.len() as f64 * HOT_PERCENTILE) as usize)
            .min(sorted.len() - 1);
        let threshold = sorted[p99_idx];

        counts
            .into_iter()
            .filter(|(_, c)| *c >= threshold)
            .collect()
    }

    /// Check if a CID has any recent fetches (demand exists).
    pub fn has_demand(&self, content_id: &ContentId) -> bool {
        let cutoff = Instant::now() - Duration::from_secs(DEMAND_WINDOW_SECS);
        if let Some(timestamps) = self.fetches.get(content_id) {
            timestamps.iter().any(|t| *t > cutoff)
        } else {
            false
        }
    }
}

/// Periodically checks for hot content and pushes a new piece to a storage
/// peer that does not yet have pieces for that CID.
///
/// Algorithm per hot CID:
///   1. Find the segment with the most local pieces (best recombination source)
///   2. Create one new RLNC piece via `create_piece_from_existing`
///   3. Find all storage-capable peers from the peer scorer
///   4. Exclude peers that already hold pieces (from PieceMap)
///   5. Pick the highest-score non-provider peer
///   6. Push the piece via DistributePieces
pub async fn run_local_scaling_loop(
    demand_tracker: Arc<Mutex<DemandTracker>>,
    store: Arc<Mutex<FsStore>>,
    piece_map: Arc<Mutex<crate::piece_map::PieceMap>>,
    peer_scorer: Option<Arc<Mutex<crate::peer_scorer::PeerScorer>>>,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    local_peer_id: PeerId,
) {
    use std::time::Duration;

    // Stagger startup so scaling doesn't interfere with initial distribution.
    tokio::time::sleep(Duration::from_secs(120)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(SCALING_CHECK_INTERVAL_SECS));
    // Track last push time per CID to avoid hammering the same content.
    let mut last_scaled: HashMap<ContentId, Instant> = HashMap::new();

    loop {
        interval.tick().await;

        let hot_cids = {
            let mut dt = demand_tracker.lock().await;
            dt.check_demand()
        };

        if hot_cids.is_empty() {
            continue;
        }

        let scorer_guard = match &peer_scorer {
            Some(ps) => Some(ps.lock().await),
            None => {
                debug!("[scaling] No peer scorer — skipping scaling check");
                continue;
            }
        };

        for (cid, demand_count) in &hot_cids {
            // Rate-limit per CID
            if let Some(last) = last_scaled.get(cid) {
                if last.elapsed().as_secs() < SCALING_COOLDOWN_SECS {
                    continue;
                }
            }

            // ── 1. Find a capable non-provider TARGET first ──────────────────
            // No point creating a piece if there is nobody to send it to.
            let providers_bytes: std::collections::HashSet<Vec<u8>> = {
                let map = piece_map.lock().await;
                map.providers(cid).into_iter().collect()
            };

            let target_peer = match scorer_guard.as_ref().and_then(|scorer| {
                let local_bytes = local_peer_id.to_bytes();
                scorer
                    .iter()
                    .filter(|(peer_id, score)| {
                        let bytes = peer_id.to_bytes();
                        bytes != local_bytes
                            && !providers_bytes.contains(&bytes)
                            && score.capabilities.contains(&CraftObjCapability::Storage)
                            && score.score() > 0.0
                    })
                    .max_by(|(_, a), (_, b)| {
                        a.score().partial_cmp(&b.score()).unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|(peer_id, _)| *peer_id)
            }) {
                Some(p) => p,
                None => {
                    debug!("[scaling] No non-provider storage peer found for {} — skipping", cid);
                    continue;
                }
            };

            // ── 2. Find a segment with enough local pieces for recombination ─
            let (best_segment, existing_pieces, seg_count_val, total_size_val, k_val, vtags_val) = {
                let store_guard = store.lock().await;
                let manifest = match store_guard.get_record(cid) {
                    Ok(m) => m,
                    Err(_) => {
                        debug!("[scaling] No manifest for {}, skipping", cid);
                        continue;
                    }
                };
                let seg_count_val = manifest.segment_count() as u32;
                let total_size_val = manifest.total_size;
                let vtags_val = manifest.vtags_cid;

                let mut best: Option<(u32, Vec<craftec_erasure::CodedPiece>)> = None;
                for seg in 0..manifest.segment_count() as u32 {
                    let piece_ids = store_guard.list_pieces(cid, seg).unwrap_or_default();
                    if piece_ids.len() < 2 {
                        continue;
                    }
                    if best.as_ref().is_none_or(|(_, p)| piece_ids.len() > p.len()) {
                        let mut coded = Vec::new();
                        for pid in &piece_ids {
                            if let Ok((data, coeff)) = store_guard.get_piece(cid, seg, pid) {
                                coded.push(craftec_erasure::CodedPiece { data, coefficients: coeff });
                            }
                        }
                        if coded.len() >= 2 {
                            best = Some((seg, coded));
                        }
                    }
                }
                match best {
                    Some((seg, pieces)) => {
                        let k_val = manifest.k_for_segment(seg as usize) as u32;
                        (seg, pieces, seg_count_val, total_size_val, k_val, vtags_val)
                    }
                    None => {
                        debug!("[scaling] {} has no segments with ≥2 local pieces", cid);
                        continue;
                    }
                }
            };

            // ── 3. Create one new RLNC piece ──────────────────────────────────
            let new_piece = match craftec_erasure::create_piece_from_existing(&existing_pieces) {
                Ok(p) => p,
                Err(e) => {
                    warn!("[scaling] Recombination failed for {}: {}", cid, e);
                    continue;
                }
            };
            let new_pid = craftobj_store::piece_id_from_coefficients(&new_piece.coefficients);

            info!(
                "[scaling] Hot CID {} (demand={}): pushing new piece to {}",
                cid,
                demand_count,
                &target_peer.to_string()[..8]
            );

            let payload = craftobj_transfer::PiecePayload {
                segment_index: best_segment,
                segment_count: seg_count_val,
                total_size: total_size_val,
                k: k_val,
                vtags_cid: vtags_val,
                piece_id: new_pid,
                coefficients: new_piece.coefficients,
                data: new_piece.data,
                vtag_blob: None,
            };

            let (reply_tx, _reply_rx) = tokio::sync::oneshot::channel();
            if command_tx
                .send(CraftObjCommand::DistributePieces {
                    peer_id: target_peer,
                    content_id: *cid,
                    pieces: vec![payload],
                    reply_tx,
                })
                .is_err()
            {
                warn!("[scaling] Command channel closed, stopping scaling loop");
                return;
            }

            last_scaled.insert(*cid, Instant::now());
        }

        // Drop scorer guard at end of loop iteration (not held across awaits in the inner loop)
        drop(scorer_guard);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_demand_no_fetches() {
        let tracker = DemandTracker::new();
        let cid = ContentId([2u8; 32]);
        assert!(!tracker.has_demand(&cid));
    }

    #[test]
    fn test_has_demand_with_fetches() {
        let mut tracker = DemandTracker::new();
        let cid = ContentId([3u8; 32]);
        tracker.record_fetch(cid);
        assert!(tracker.has_demand(&cid));
    }

    #[test]
    fn test_below_min_cids_returns_empty() {
        // Fewer than MIN_CIDS_FOR_PERCENTILE → no baseline → skip scaling.
        let mut tracker = DemandTracker::new();
        for i in 0..5u8 {
            let cid = ContentId([i; 32]);
            tracker.record_fetch(cid);
        }
        let hot = tracker.check_demand();
        assert!(hot.is_empty(), "below min cardinality should return empty, not trigger scaling");
    }

    #[test]
    fn test_percentile_detects_outlier() {
        // With >= MIN_CIDS_FOR_PERCENTILE CIDs, only the top 1% should be hot.
        let mut tracker = DemandTracker::new();

        // Add 10 CIDs with 1 fetch each (baseline)
        for i in 0..10u8 {
            let cid = ContentId([i; 32]);
            tracker.record_fetch(cid);
        }

        // Spike one CID with 50 fetches — it should be the clear outlier
        let hot_cid = ContentId([99u8; 32]);
        for _ in 0..50 {
            tracker.record_fetch(hot_cid);
        }

        let hot = tracker.check_demand();
        // The spiked CID should be in the hot list
        assert!(
            hot.iter().any(|(cid, _)| *cid == hot_cid),
            "spiked CID should be hot"
        );
        // Not all 11 CIDs should be hot
        assert!(hot.len() < 11, "only spiked CID(s) should be hot, got {:?} hot", hot.len());
    }

    #[test]
    fn test_empty_returns_empty() {
        let mut tracker = DemandTracker::new();
        assert!(tracker.check_demand().is_empty());
    }

    #[test]
    fn test_uniform_traffic_returns_all_at_threshold() {
        // All CIDs with equal count → all at 99th percentile → all hot.
        let mut tracker = DemandTracker::new();
        for i in 0..20u8 {
            let cid = ContentId([i; 32]);
            for _ in 0..5 {
                tracker.record_fetch(cid);
            }
        }
        let hot = tracker.check_demand();
        // When all are equal, the 99th percentile equals the common value so all qualify.
        assert!(!hot.is_empty(), "uniform traffic: all at p99 so all should be hot");
    }
}
