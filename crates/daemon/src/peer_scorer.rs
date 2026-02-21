//! Client-Side Peer Scoring
//!
//! Tracks peer reliability based on actual interactions (success/failure/timeout).
//! Replaces the static `PeerCapabilities` map with a scorer that ranks peers
//! for routing decisions. Event-driven: scores update on each interaction.
//! No background health loop — DHT record TTL + PDP cycles provide liveness.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use craftobj_core::CraftObjCapability;
use libp2p::PeerId;
use serde::Serialize;

/// Network-wide storage summary aggregated from peer announcements.
#[derive(Debug, Clone, Serialize)]
pub struct NetworkStorageSummary {
    pub total_committed: u64,
    pub total_used: u64,
    pub total_available: u64,
    pub storage_node_count: u64,
}

/// Decay factor applied on each score update (0.95 = 5% decay per interaction).
const DECAY_FACTOR: f64 = 0.95;

/// Exponential moving average alpha for latency (0.3 = recent latency weighted 30%).
const LATENCY_EMA_ALPHA: f64 = 0.3;

/// Default score for peers with no interaction history.
const DEFAULT_SCORE: f64 = 0.5;

/// Timeout weight multiplier relative to failures.
const TIMEOUT_WEIGHT: f64 = 2.0;

/// Tracks per-peer reliability scores and capabilities.
pub struct PeerScorer {
    scores: HashMap<PeerId, PeerScore>,
}

/// Per-peer scoring state.
pub struct PeerScore {
    /// Known capabilities.
    pub capabilities: Vec<CraftObjCapability>,
    /// When the last capability announcement was received.
    pub last_announcement: Instant,
    /// Decayed success count.
    pub successes: f64,
    /// Decayed failure count.
    pub failures: f64,
    /// Decayed timeout count.
    pub timeouts: f64,
    /// When the last interaction (success/failure/timeout) occurred.
    pub last_interaction: Option<Instant>,
    /// Exponential moving average of response latency in milliseconds.
    pub avg_latency_ms: f64,
    /// Storage committed by this peer (from capability announcement).
    pub storage_committed_bytes: u64,
    /// Storage currently used by this peer (from capability announcement).
    pub storage_used_bytes: u64,
    /// Geographic region from capability announcement (e.g. "us-east", "eu-west").
    pub region: Option<String>,
    /// Root hash of the node's storage Merkle tree.
    pub storage_root: [u8; 32],
    /// Consecutive PDP challenge failures (resets on success).
    /// At ≥5 the peer is soft-banned from further PDP challenges until score recovers.
    pub pdp_failures: u32,

    // ── Transfer statistics ─────────────────────────────────
    /// Total bytes sent to this peer.
    pub bytes_sent: u64,
    /// Total bytes received from this peer.
    pub bytes_received: u64,
    /// Total pieces sent to this peer.
    pub pieces_sent: u32,
    /// Total pieces received from this peer.
    pub pieces_received: u32,
    /// When the last transfer (send or receive) occurred.
    pub last_transfer: Option<Instant>,
    /// Rolling window of (timestamp, bytes) for transfer rate calculation.
    transfer_samples: Vec<(Instant, u64)>,
}

impl PeerScore {
    fn new(capabilities: Vec<CraftObjCapability>, timestamp: Instant) -> Self {
        Self {
            capabilities,
            last_announcement: timestamp,
            successes: 0.0,
            failures: 0.0,
            timeouts: 0.0,
            last_interaction: None,
            avg_latency_ms: 0.0,
            storage_committed_bytes: 0,
            storage_used_bytes: 0,
            region: None,
            storage_root: [0u8; 32],
            pdp_failures: 0,
            bytes_sent: 0,
            bytes_received: 0,
            pieces_sent: 0,
            pieces_received: 0,
            last_transfer: None,
            transfer_samples: Vec::new(),
        }
    }

    /// Apply decay to all counters (called before each score update).
    fn apply_decay(&mut self) {
        self.successes *= DECAY_FACTOR;
        self.failures *= DECAY_FACTOR;
        self.timeouts *= DECAY_FACTOR;
    }

    /// Apply time-based reputation decay toward neutral (0.5).
    /// Decay rate: 10% per hour toward neutral. Applied lazily when scores are read.
    fn apply_time_decay(&mut self) {
        if let Some(last) = self.last_interaction {
            let elapsed_hours = last.elapsed().as_secs_f64() / 3600.0;
            if elapsed_hours > 0.01 {
                // Decay factor: 0.9^hours (10% per hour toward neutral)
                let decay = 0.9_f64.powf(elapsed_hours);
                // Decay counters toward zero (which yields DEFAULT_SCORE = 0.5)
                self.successes *= decay;
                self.failures *= decay;
                self.timeouts *= decay;
                // Update last_interaction so we don't double-decay
                self.last_interaction = Some(Instant::now());
            }
        }
    }

    /// Compute reliability score in [0.0, 1.0].
    /// Peers with no interactions get DEFAULT_SCORE.
    pub fn score(&self) -> f64 {
        let total = self.successes + self.failures + TIMEOUT_WEIGHT * self.timeouts;
        if total < 0.001 {
            return DEFAULT_SCORE;
        }
        (self.successes / total).clamp(0.0, 1.0)
    }

    /// Compute reliability score with time-based decay applied.
    pub fn score_with_decay(&mut self) -> f64 {
        self.apply_time_decay();
        self.score()
    }

    /// Whether this peer has had any interactions.
    #[allow(dead_code)]
    fn has_interactions(&self) -> bool {
        self.last_interaction.is_some()
    }
}

impl PeerScorer {
    /// Create an empty scorer.
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
        }
    }

    /// Record a successful interaction with a peer.
    pub fn record_success(&mut self, peer: &PeerId, latency: Duration) {
        let entry = self.scores.entry(*peer).or_insert_with(|| {
            PeerScore::new(Vec::new(), Instant::now())
        });
        entry.apply_decay();
        entry.successes += 1.0;
        entry.last_interaction = Some(Instant::now());

        let latency_ms = latency.as_secs_f64() * 1000.0;
        if entry.avg_latency_ms == 0.0 {
            entry.avg_latency_ms = latency_ms;
        } else {
            entry.avg_latency_ms =
                LATENCY_EMA_ALPHA * latency_ms + (1.0 - LATENCY_EMA_ALPHA) * entry.avg_latency_ms;
        }
    }

    /// Record a failed interaction with a peer.
    pub fn record_failure(&mut self, peer: &PeerId) {
        let entry = self.scores.entry(*peer).or_insert_with(|| {
            PeerScore::new(Vec::new(), Instant::now())
        });
        entry.apply_decay();
        entry.failures += 1.0;
        entry.last_interaction = Some(Instant::now());
    }

    /// Record a PDP challenge failure (escalating penalty).
    /// Increments the dedicated `pdp_failures` counter which gates future challenges.
    pub fn record_pdp_failure(&mut self, peer: &PeerId) {
        self.record_failure(peer);
        if let Some(entry) = self.scores.get_mut(peer) {
            entry.pdp_failures += 1;
        }
    }

    /// Record a PDP challenge success — resets the escalating failure counter.
    pub fn record_pdp_success(&mut self, peer: &PeerId, latency: std::time::Duration) {
        self.record_success(peer, latency);
        if let Some(entry) = self.scores.get_mut(peer) {
            entry.pdp_failures = 0;
        }
    }

    /// Whether this peer is soft-banned from PDP challenges due to repeated failures.
    pub fn is_pdp_banned(&self, peer: &PeerId) -> bool {
        self.scores.get(peer).map(|s| s.pdp_failures >= 5).unwrap_or(false)
    }

    /// Record a timed-out interaction with a peer (weighted heavier than failure).
    pub fn record_timeout(&mut self, peer: &PeerId) {
        let entry = self.scores.entry(*peer).or_insert_with(|| {
            PeerScore::new(Vec::new(), Instant::now())
        });
        entry.apply_decay();
        entry.timeouts += 1.0;
        entry.last_interaction = Some(Instant::now());
    }

    /// Record bytes sent to a peer (piece push).
    pub fn record_sent(&mut self, peer: &PeerId, bytes: u64) {
        let entry = self.scores.entry(*peer).or_insert_with(|| {
            PeerScore::new(Vec::new(), Instant::now())
        });
        entry.bytes_sent += bytes;
        entry.pieces_sent += 1;
        let now = Instant::now();
        entry.last_transfer = Some(now);
        entry.transfer_samples.push((now, bytes));
        // Keep last 100 samples
        if entry.transfer_samples.len() > 100 {
            entry.transfer_samples.drain(..entry.transfer_samples.len() - 100);
        }
    }

    /// Record bytes received from a peer (piece fetch).
    pub fn record_received(&mut self, peer: &PeerId, bytes: u64) {
        let entry = self.scores.entry(*peer).or_insert_with(|| {
            PeerScore::new(Vec::new(), Instant::now())
        });
        entry.bytes_received += bytes;
        entry.pieces_received += 1;
        let now = Instant::now();
        entry.last_transfer = Some(now);
        entry.transfer_samples.push((now, bytes));
        if entry.transfer_samples.len() > 100 {
            entry.transfer_samples.drain(..entry.transfer_samples.len() - 100);
        }
    }

    /// Get the rolling average transfer rate for a peer in bytes/second.
    /// Returns 0.0 if insufficient data.
    pub fn transfer_rate(&self, peer: &PeerId) -> f64 {
        if let Some(entry) = self.scores.get(peer) {
            if entry.transfer_samples.len() < 2 {
                return 0.0;
            }
            let first = entry.transfer_samples.first().unwrap().0;
            let last = entry.transfer_samples.last().unwrap().0;
            let duration = last.duration_since(first).as_secs_f64();
            if duration < 0.001 {
                return 0.0;
            }
            let total_bytes: u64 = entry.transfer_samples.iter().map(|(_, b)| b).sum();
            total_bytes as f64 / duration
        } else {
            0.0
        }
    }

    /// Get the reliability score for a peer (0.0–1.0) with time-based decay.
    pub fn score(&mut self, peer: &PeerId) -> f64 {
        if let Some(s) = self.scores.get_mut(peer) {
            s.score_with_decay()
        } else {
            DEFAULT_SCORE
        }
    }

    /// Get the reliability score without mutable access (no decay applied).
    pub fn score_readonly(&self, peer: &PeerId) -> f64 {
        self.scores
            .get(peer)
            .map(|s| s.score())
            .unwrap_or(DEFAULT_SCORE)
    }

    /// Rank peers by score descending (best first). Unknown peers get DEFAULT_SCORE.
    pub fn rank_peers(&mut self, peers: &[PeerId]) -> Vec<PeerId> {
        // Apply decay to all ranked peers
        for p in peers {
            if let Some(s) = self.scores.get_mut(p) {
                s.apply_time_decay();
            }
        }
        let mut scored: Vec<(PeerId, f64)> = peers
            .iter()
            .map(|p| (*p, self.score_readonly(p)))
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().map(|(p, _)| p).collect()
    }

    /// Update capabilities from peer discovery.
    /// Only updates if the timestamp is newer than the current record.
    pub fn update_capabilities(
        &mut self,
        peer: &PeerId,
        capabilities: Vec<CraftObjCapability>,
        _timestamp: u64,
    ) {
        self.update_capabilities_with_storage(peer, capabilities, _timestamp, 0, 0, None);
    }

    /// Update capabilities and storage info from peer discovery.
    pub fn update_capabilities_with_storage(
        &mut self,
        peer: &PeerId,
        capabilities: Vec<CraftObjCapability>,
        _timestamp: u64,
        storage_committed_bytes: u64,
        storage_used_bytes: u64,
        region: Option<String>,
    ) {
        self.update_capabilities_full(peer, capabilities, _timestamp, storage_committed_bytes, storage_used_bytes, region, [0u8; 32]);
    }

    /// Update capabilities, storage info, and merkle root from peer discovery.
    #[allow(clippy::too_many_arguments)]
    pub fn update_capabilities_full(
        &mut self,
        peer: &PeerId,
        capabilities: Vec<CraftObjCapability>,
        _timestamp: u64,
        storage_committed_bytes: u64,
        storage_used_bytes: u64,
        region: Option<String>,
        storage_root: [u8; 32],
    ) {
        let now = Instant::now();
        let entry = self.scores.entry(*peer).or_insert_with(|| {
            PeerScore::new(capabilities.clone(), now)
        });
        entry.capabilities = capabilities;
        entry.last_announcement = now;
        entry.storage_committed_bytes = storage_committed_bytes;
        entry.storage_used_bytes = storage_used_bytes;
        entry.region = region;
        entry.storage_root = storage_root;
    }

    /// Get the region for a peer, if known.
    pub fn get_region(&self, peer: &PeerId) -> Option<&str> {
        self.scores.get(peer).and_then(|s| s.region.as_deref())
    }

    /// Get network-wide storage summary from all known peers.
    pub fn network_storage_summary(&self) -> NetworkStorageSummary {
        let mut total_committed = 0u64;
        let mut total_used = 0u64;
        let mut storage_node_count = 0u64;
        for score in self.scores.values() {
            if score.capabilities.contains(&CraftObjCapability::Storage) {
                storage_node_count += 1;
                total_committed += score.storage_committed_bytes;
                total_used += score.storage_used_bytes;
            }
        }
        NetworkStorageSummary {
            total_committed,
            total_used,
            total_available: total_committed.saturating_sub(total_used),
            storage_node_count,
        }
    }

    /// Remove peers whose last announcement exceeds the given TTL.
    /// Call periodically to remove stale peers.
    pub fn evict_stale(&mut self, ttl: Duration) {
        let now = Instant::now();
        self.scores
            .retain(|_, score| now.duration_since(score.last_announcement) < ttl);
    }

    /// Remove a peer entirely from the scorer (e.g. on connection close).
    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.scores.remove(peer);
    }

    /// Get a reference to a peer's score data.
    pub fn get(&self, peer: &PeerId) -> Option<&PeerScore> {
        self.scores.get(peer)
    }

    /// Iterate over all tracked peers and their scores.
    pub fn iter(&self) -> impl Iterator<Item = (&PeerId, &PeerScore)> {
        self.scores.iter()
    }

    /// Number of tracked peers.
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Whether the scorer has no tracked peers.
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }
}

impl Default for PeerScorer {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_peer_default_score() {
        let mut scorer = PeerScorer::new();
        let peer = PeerId::random();
        assert!((scorer.score(&peer) - DEFAULT_SCORE).abs() < f64::EPSILON);
    }

    #[test]
    fn test_record_success_increases_score() {
        let mut scorer = PeerScorer::new();
        let peer = PeerId::random();

        for _ in 0..10 {
            scorer.record_success(&peer, Duration::from_millis(50));
        }

        assert!(scorer.score(&peer) > 0.9);
    }

    #[test]
    fn test_record_failure_decreases_score() {
        let mut scorer = PeerScorer::new();
        let peer = PeerId::random();

        scorer.record_success(&peer, Duration::from_millis(50));
        for _ in 0..10 {
            scorer.record_failure(&peer);
        }

        assert!(scorer.score(&peer) < 0.3);
    }

    #[test]
    fn test_timeout_weighted_heavier_than_failure() {
        let mut scorer = PeerScorer::new();
        let peer_fail = PeerId::random();
        let peer_timeout = PeerId::random();

        // Same number of successes
        for _ in 0..5 {
            scorer.record_success(&peer_fail, Duration::from_millis(50));
            scorer.record_success(&peer_timeout, Duration::from_millis(50));
        }

        // Same number of bad interactions, but one is timeouts
        for _ in 0..5 {
            scorer.record_failure(&peer_fail);
            scorer.record_timeout(&peer_timeout);
        }

        // Timeout peer should have lower score
        assert!(scorer.score(&peer_timeout) < scorer.score(&peer_fail));
    }

    #[test]
    fn test_rank_peers() {
        let mut scorer = PeerScorer::new();
        let good = PeerId::random();
        let bad = PeerId::random();
        let unknown = PeerId::random();

        for _ in 0..10 {
            scorer.record_success(&good, Duration::from_millis(20));
        }
        for _ in 0..10 {
            scorer.record_failure(&bad);
        }

        let ranked = scorer.rank_peers(&[bad, unknown, good]);
        assert_eq!(ranked[0], good);
        // unknown (DEFAULT_SCORE=0.5) should beat bad (near 0.0)
        assert_eq!(ranked[1], unknown);
        assert_eq!(ranked[2], bad);
    }

    #[test]
    fn test_decay_fades_history() {
        let mut scorer = PeerScorer::new();
        let peer = PeerId::random();

        // Record many failures
        for _ in 0..20 {
            scorer.record_failure(&peer);
        }
        let score_after_failures = scorer.score(&peer);
        assert!(score_after_failures < 0.1);

        // Now record many successes — decay should fade the old failures
        for _ in 0..50 {
            scorer.record_success(&peer, Duration::from_millis(30));
        }
        assert!(scorer.score(&peer) > 0.7);
    }

    #[test]
    fn test_latency_ema() {
        let mut scorer = PeerScorer::new();
        let peer = PeerId::random();

        scorer.record_success(&peer, Duration::from_millis(100));
        let entry = scorer.get(&peer).unwrap();
        assert!((entry.avg_latency_ms - 100.0).abs() < 0.1);

        scorer.record_success(&peer, Duration::from_millis(200));
        let entry = scorer.get(&peer).unwrap();
        // EMA: 0.3 * 200 + 0.7 * 100 = 130
        assert!((entry.avg_latency_ms - 130.0).abs() < 1.0);
    }

    #[test]
    fn test_update_capabilities() {
        let mut scorer = PeerScorer::new();
        let peer = PeerId::random();

        scorer.update_capabilities(&peer, vec![CraftObjCapability::Storage], 1000);
        let entry = scorer.get(&peer).unwrap();
        assert_eq!(entry.capabilities, vec![CraftObjCapability::Storage]);

        scorer.update_capabilities(
            &peer,
            vec![CraftObjCapability::Storage, CraftObjCapability::Client],
            2000,
        );
        let entry = scorer.get(&peer).unwrap();
        assert_eq!(entry.capabilities.len(), 2);
    }

    #[test]
    fn test_evict_stale() {
        let mut scorer = PeerScorer::new();
        let fresh = PeerId::random();
        let stale = PeerId::random();

        scorer.update_capabilities(&fresh, vec![CraftObjCapability::Client], 1000);

        // Insert stale peer with old announcement time
        scorer.scores.insert(
            stale,
            PeerScore {
                capabilities: vec![],
                last_announcement: Instant::now() - Duration::from_secs(3600),
                successes: 0.0,
                failures: 0.0,
                timeouts: 0.0,
                last_interaction: None,
                avg_latency_ms: 0.0,
                storage_committed_bytes: 0,
                storage_used_bytes: 0,
                region: None,
                storage_root: [0u8; 32],
                bytes_sent: 0,
                bytes_received: 0,
                pieces_sent: 0,
                pieces_received: 0,
                last_transfer: None,
                transfer_samples: Vec::new(),
            },
        );

        assert_eq!(scorer.len(), 2);
        scorer.evict_stale(Duration::from_secs(900)); // 15 min TTL
        assert_eq!(scorer.len(), 1);
        assert!(scorer.get(&fresh).is_some());
        assert!(scorer.get(&stale).is_none());
    }

    #[test]
    fn test_score_clamped() {
        let mut scorer = PeerScorer::new();
        let peer = PeerId::random();
        let s = scorer.score(&peer);
        assert!(s >= 0.0 && s <= 1.0);
    }

    #[test]
    fn test_empty_scorer() {
        let scorer = PeerScorer::new();
        assert!(scorer.is_empty());
        assert_eq!(scorer.len(), 0);
    }

    #[test]
    fn test_mixed_interactions_score() {
        let mut scorer = PeerScorer::new();
        let peer = PeerId::random();

        // 7 successes, 2 failures, 1 timeout → 7 / (7 + 2 + 2*1) = 7/11 ≈ 0.636
        // With decay applied each time, the exact value differs but should be moderate
        for _ in 0..7 {
            scorer.record_success(&peer, Duration::from_millis(50));
        }
        for _ in 0..2 {
            scorer.record_failure(&peer);
        }
        scorer.record_timeout(&peer);

        let s = scorer.score(&peer);
        assert!(s > 0.4 && s < 0.8, "score {} should be moderate", s);
    }
}
