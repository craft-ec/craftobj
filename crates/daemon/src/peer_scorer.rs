//! Client-Side Peer Scoring
//!
//! Tracks peer reliability based on actual interactions (success/failure/timeout).
//! Replaces the static `PeerCapabilities` map with a scorer that ranks peers
//! for routing decisions. Event-driven: scores update on each interaction.
//! No background health loop — DHT record TTL + PDP cycles provide liveness.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use datacraft_core::DataCraftCapability;
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
    /// Known capabilities from gossipsub announcements.
    pub capabilities: Vec<DataCraftCapability>,
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
}

impl PeerScore {
    fn new(capabilities: Vec<DataCraftCapability>, timestamp: Instant) -> Self {
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
        }
    }

    /// Apply decay to all counters (called before each score update).
    fn apply_decay(&mut self) {
        self.successes *= DECAY_FACTOR;
        self.failures *= DECAY_FACTOR;
        self.timeouts *= DECAY_FACTOR;
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

    /// Record a timed-out interaction with a peer (weighted heavier than failure).
    pub fn record_timeout(&mut self, peer: &PeerId) {
        let entry = self.scores.entry(*peer).or_insert_with(|| {
            PeerScore::new(Vec::new(), Instant::now())
        });
        entry.apply_decay();
        entry.timeouts += 1.0;
        entry.last_interaction = Some(Instant::now());
    }

    /// Get the reliability score for a peer (0.0–1.0).
    pub fn score(&self, peer: &PeerId) -> f64 {
        self.scores
            .get(peer)
            .map(|s| s.score())
            .unwrap_or(DEFAULT_SCORE)
    }

    /// Rank peers by score descending (best first). Unknown peers get DEFAULT_SCORE.
    pub fn rank_peers(&self, peers: &[PeerId]) -> Vec<PeerId> {
        let mut scored: Vec<(PeerId, f64)> = peers
            .iter()
            .map(|p| (*p, self.score(p)))
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().map(|(p, _)| p).collect()
    }

    /// Update capabilities from a gossipsub announcement.
    /// Only updates if the timestamp is newer than the current record.
    pub fn update_capabilities(
        &mut self,
        peer: &PeerId,
        capabilities: Vec<DataCraftCapability>,
        _timestamp: u64,
    ) {
        self.update_capabilities_with_storage(peer, capabilities, _timestamp, 0, 0);
    }

    /// Update capabilities and storage info from a gossipsub announcement.
    pub fn update_capabilities_with_storage(
        &mut self,
        peer: &PeerId,
        capabilities: Vec<DataCraftCapability>,
        _timestamp: u64,
        storage_committed_bytes: u64,
        storage_used_bytes: u64,
    ) {
        let now = Instant::now();
        let entry = self.scores.entry(*peer).or_insert_with(|| {
            PeerScore::new(capabilities.clone(), now)
        });
        // Always update capabilities and announcement time on new gossipsub message
        // (the caller already filters by timestamp freshness)
        entry.capabilities = capabilities;
        entry.last_announcement = now;
        entry.storage_committed_bytes = storage_committed_bytes;
        entry.storage_used_bytes = storage_used_bytes;
    }

    /// Get network-wide storage summary from all known peers.
    pub fn network_storage_summary(&self) -> NetworkStorageSummary {
        let mut total_committed = 0u64;
        let mut total_used = 0u64;
        let mut storage_node_count = 0u64;
        for score in self.scores.values() {
            if score.capabilities.contains(&DataCraftCapability::Storage) {
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
    /// Call after processing gossipsub messages, not on a timer.
    pub fn evict_stale(&mut self, ttl: Duration) {
        let now = Instant::now();
        self.scores
            .retain(|_, score| now.duration_since(score.last_announcement) < ttl);
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
        let scorer = PeerScorer::new();
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

        scorer.update_capabilities(&peer, vec![DataCraftCapability::Storage], 1000);
        let entry = scorer.get(&peer).unwrap();
        assert_eq!(entry.capabilities, vec![DataCraftCapability::Storage]);

        scorer.update_capabilities(
            &peer,
            vec![DataCraftCapability::Storage, DataCraftCapability::Client],
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

        scorer.update_capabilities(&fresh, vec![DataCraftCapability::Client], 1000);

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
        let scorer = PeerScorer::new();
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
