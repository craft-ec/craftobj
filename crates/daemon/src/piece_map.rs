//! PieceMap — scoped, event-sourced materialized view of piece locations.
//!
//! Only tracks segments the local node holds pieces in (scoped PieceMap).
//! Built from PieceStored/PieceDropped events (local and P2P sync).
//! Events for untracked segments are discarded.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use craftobj_core::{ContentId, ContentManifest, PieceEvent};
#[cfg(test)]
use craftobj_core::{PieceStored, PieceDropped};
use libp2p::PeerId;

/// Scoped materialized view of piece locations across the network.
///
/// Only tracks segments where the local node holds at least one piece.
/// Events for untracked segments are discarded.
pub struct PieceMap {
    /// (node_bytes, cid, segment, piece_id) → coefficients
    pieces: HashMap<(Vec<u8>, ContentId, u32, [u8; 32]), Vec<u8>>,
    /// Per-node latest processed sequence number
    node_seqs: HashMap<Vec<u8>, u64>,
    /// Node online status (derived from capability announcements)
    node_online: HashMap<Vec<u8>, bool>,
    /// Last time we saw activity from each node
    node_last_seen: HashMap<Vec<u8>, Instant>,
    /// Local node's sequence counter
    local_seq: u64,
    /// Local node's PeerId bytes
    local_node: Vec<u8>,
    /// Tracked segments — only segments where local node holds pieces.
    /// Events for segments NOT in this set are discarded.
    tracked_segments: HashSet<(ContentId, u32)>,
}

impl PieceMap {
    /// Create a new PieceMap for the given local peer.
    pub fn new(local_peer_id: PeerId) -> Self {
        Self {
            pieces: HashMap::new(),
            node_seqs: HashMap::new(),
            node_online: HashMap::new(),
            node_last_seen: HashMap::new(),
            local_seq: 0,
            local_node: local_peer_id.to_bytes().to_vec(),
            tracked_segments: HashSet::new(),
        }
    }

    /// Apply a PieceEvent to the map.
    ///
    /// Skips if seq <= known seq for that node.
    /// Skips events for untracked segments (unless the event is from the local node,
    /// which auto-tracks via `track_segment`).
    pub fn apply_event(&mut self, event: &PieceEvent) {
        let node = event.node().to_vec();
        let seq = event.seq();
        let (cid, segment) = match event {
            PieceEvent::Stored(s) => (s.cid, s.segment),
            PieceEvent::Dropped(d) => (d.cid, d.segment),
        };

        // Only process events for tracked segments
        if !self.tracked_segments.contains(&(cid, segment)) {
            return;
        }

        // Skip if we've already seen this or a later seq from this node
        if let Some(&known) = self.node_seqs.get(&node) {
            if seq <= known {
                return;
            }
        }

        self.node_seqs.insert(node.clone(), seq);
        self.update_last_seen(&node);

        match event {
            PieceEvent::Stored(s) => {
                let key = (node, s.cid, s.segment, s.piece_id);
                self.pieces.insert(key, s.coefficients.clone());
            }
            PieceEvent::Dropped(d) => {
                let key = (node, d.cid, d.segment, d.piece_id);
                self.pieces.remove(&key);
            }
        }
    }

    /// Increment and return the next local sequence number.
    pub fn next_seq(&mut self) -> u64 {
        self.local_seq += 1;
        self.local_seq
    }

    /// Start tracking a segment. Called when node stores its first piece for a new segment.
    /// Returns true if this is a newly tracked segment (first piece).
    pub fn track_segment(&mut self, cid: ContentId, segment: u32) -> bool {
        self.tracked_segments.insert((cid, segment))
    }

    /// Stop tracking a segment. Called when node drops all pieces for a segment.
    /// Removes all PieceMap entries for that segment.
    pub fn untrack_segment(&mut self, cid: &ContentId, segment: u32) {
        self.tracked_segments.remove(&(*cid, segment));
        // Remove all entries for this segment
        self.pieces.retain(|(_, c, s, _), _| !(c == cid && *s == segment));
    }

    /// Check if a segment is being tracked.
    pub fn is_tracked(&self, cid: &ContentId, segment: u32) -> bool {
        self.tracked_segments.contains(&(*cid, segment))
    }

    /// Get all tracked segments.
    pub fn tracked_segments(&self) -> &HashSet<(ContentId, u32)> {
        &self.tracked_segments
    }

    /// Compute the rank (number of linearly independent coefficient vectors) for a segment.
    /// If `online_only` is true, only include pieces from online nodes.
    pub fn compute_rank(&self, cid: &ContentId, segment: u32, online_only: bool) -> usize {
        let vectors: Vec<Vec<u8>> = self.pieces.iter()
            .filter(|((node, c, s, _), _)| {
                c == cid && *s == segment && (!online_only || self.is_node_online(node))
            })
            .map(|(_, coeff)| coeff.clone())
            .collect();

        if vectors.is_empty() {
            return 0;
        }

        craftec_erasure::check_independence(&vectors)
    }

    /// Compute health as min(rank/k) across all segments.
    /// Returns 0.0 if manifest has no segments or k=0.
    pub fn compute_health(&self, cid: &ContentId, manifest: &ContentManifest, online_only: bool) -> f64 {
        if manifest.segment_count() == 0 {
            return 0.0;
        }

        let mut min_health = f64::MAX;
        for seg in 0..manifest.segment_count() {
            let k = manifest.k_for_segment(seg);
            if k == 0 {
                continue;
            }
            let rank = self.compute_rank(cid, seg as u32, online_only);
            let health = rank as f64 / k as f64;
            if health < min_health {
                min_health = health;
            }
        }

        if min_health == f64::MAX {
            0.0
        } else {
            min_health
        }
    }

    /// Unique nodes holding pieces for this CID.
    pub fn providers(&self, cid: &ContentId) -> Vec<Vec<u8>> {
        let mut nodes: HashSet<Vec<u8>> = HashSet::new();
        for (node, c, _, _) in self.pieces.keys() {
            if c == cid {
                nodes.insert(node.clone());
            }
        }
        nodes.into_iter().collect()
    }

    /// Count of unique provider nodes for a CID, optionally filtered by online status.
    pub fn provider_count(&self, cid: &ContentId, online_only: bool) -> usize {
        let mut nodes: HashSet<&Vec<u8>> = HashSet::new();
        for (node, c, _, _) in self.pieces.keys() {
            if c == cid && (!online_only || self.is_node_online(node)) {
                nodes.insert(node);
            }
        }
        nodes.len()
    }

    /// Total number of pieces stored for a CID across all nodes and segments.
    pub fn total_pieces(&self, cid: &ContentId) -> usize {
        self.pieces.keys().filter(|(_, c, _, _)| c == cid).count()
    }

    /// Number of pieces for a specific CID and segment across all nodes.
    pub fn segment_pieces(&self, cid: &ContentId, segment: u32) -> usize {
        self.pieces.keys().filter(|(_, c, s, _)| c == cid && *s == segment).count()
    }

    /// Number of pieces held by the local node for a specific CID and segment.
    pub fn local_pieces(&self, cid: &ContentId, segment: u32) -> usize {
        self.pieces.keys()
            .filter(|(node, c, s, _)| node == &self.local_node && c == cid && *s == segment)
            .count()
    }

    /// Does the local node have at least `min_count` pieces for this CID and segment?
    pub fn has_pieces(&self, cid: &ContentId, segment: u32, min_count: usize) -> bool {
        self.local_pieces(cid, segment) >= min_count
    }

    /// Set a node's online status.
    pub fn set_node_online(&mut self, node: &[u8], online: bool) {
        self.node_online.insert(node.to_vec(), online);
        if online {
            self.node_last_seen.insert(node.to_vec(), Instant::now());
        }
    }

    /// Update the last-seen timestamp for a node.
    pub fn update_last_seen(&mut self, node: &[u8]) {
        self.node_last_seen.insert(node.to_vec(), Instant::now());
    }

    /// Mark nodes as offline if they haven't been seen within the timeout.
    pub fn check_timeouts(&mut self, timeout: Duration) {
        let now = Instant::now();
        let timed_out: Vec<Vec<u8>> = self.node_last_seen.iter()
            .filter(|(_, last)| now.duration_since(**last) > timeout)
            .map(|(node, _)| node.clone())
            .collect();
        for node in timed_out {
            self.node_online.insert(node, false);
        }
    }

    /// Get the known sequence numbers for all nodes (for sync requests).
    pub fn known_seqs(&self) -> HashMap<Vec<u8>, u64> {
        self.node_seqs.clone()
    }

    /// Get all CIDs tracked in the map.
    pub fn all_cids(&self) -> HashSet<ContentId> {
        self.pieces.keys().map(|(_, cid, _, _)| *cid).collect()
    }

    /// Get all pieces for a specific segment: (node, piece_id, coefficients).
    pub fn pieces_for_segment(&self, cid: &ContentId, segment: u32) -> Vec<(&Vec<u8>, &[u8; 32], &Vec<u8>)> {
        self.pieces.iter()
            .filter(|((_, c, s, _), _)| c == cid && *s == segment)
            .map(|((node, _, _, pid), coeff)| (node, pid, coeff))
            .collect()
    }

    /// Get all local pieces for a CID: (segment, piece_id).
    pub fn pieces_for_cid_local(&self, cid: &ContentId) -> Vec<(u32, [u8; 32])> {
        self.pieces.keys()
            .filter(|(node, c, _, _)| node == &self.local_node && c == cid)
            .map(|(_, _, seg, pid)| (*seg, *pid))
            .collect()
    }

    /// Get the local node's PeerId bytes.
    pub fn local_node(&self) -> &[u8] {
        &self.local_node
    }

    /// Check if a node is marked as online.
    pub fn is_node_online_pub(&self, node: &[u8]) -> bool {
        self.node_online.get(node).copied().unwrap_or(false)
    }

    fn is_node_online(&self, node: &[u8]) -> bool {
        self.is_node_online_pub(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craftobj_core::ContentId;

    fn test_peer_id() -> PeerId {
        PeerId::random()
    }

    fn make_stored(node: &[u8], cid: ContentId, segment: u32, piece_id: [u8; 32], coefficients: Vec<u8>, seq: u64) -> PieceEvent {
        PieceEvent::Stored(PieceStored {
            node: node.to_vec(),
            cid,
            segment,
            piece_id,
            coefficients,
            seq,
            timestamp: 1000,
            signature: vec![],
        })
    }

    fn make_dropped(node: &[u8], cid: ContentId, segment: u32, piece_id: [u8; 32], seq: u64) -> PieceEvent {
        PieceEvent::Dropped(PieceDropped {
            node: node.to_vec(),
            cid,
            segment,
            piece_id,
            seq,
            timestamp: 1000,
            signature: vec![],
        })
    }

    #[test]
    fn test_apply_stored_and_dropped() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        let cid = ContentId([1u8; 32]);
        let node = vec![10u8; 32];
        let pid = [20u8; 32];

        // Track segment first
        map.track_segment(cid, 0);

        // Store
        map.apply_event(&make_stored(&node, cid, 0, pid, vec![1, 2, 3], 1));
        assert_eq!(map.total_pieces(&cid), 1);
        assert_eq!(map.segment_pieces(&cid, 0), 1);

        // Drop
        map.apply_event(&make_dropped(&node, cid, 0, pid, 2));
        assert_eq!(map.total_pieces(&cid), 0);
    }

    #[test]
    fn test_untracked_segment_ignored() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        let cid = ContentId([1u8; 32]);
        let node = vec![10u8; 32];
        let pid = [20u8; 32];

        // Don't track segment — event should be ignored
        map.apply_event(&make_stored(&node, cid, 0, pid, vec![1, 2, 3], 1));
        assert_eq!(map.total_pieces(&cid), 0);
    }

    #[test]
    fn test_track_untrack_segment() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        let cid = ContentId([1u8; 32]);
        let node = vec![10u8; 32];

        assert!(!map.is_tracked(&cid, 0));
        assert!(map.track_segment(cid, 0));
        assert!(map.is_tracked(&cid, 0));
        // Second call returns false (already tracked)
        assert!(!map.track_segment(cid, 0));

        // Add a piece
        map.apply_event(&make_stored(&node, cid, 0, [1u8; 32], vec![1], 1));
        assert_eq!(map.total_pieces(&cid), 1);

        // Untrack — removes all entries
        map.untrack_segment(&cid, 0);
        assert!(!map.is_tracked(&cid, 0));
        assert_eq!(map.total_pieces(&cid), 0);
    }

    #[test]
    fn test_seq_dedup() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        let cid = ContentId([1u8; 32]);
        let node = vec![10u8; 32];
        let pid1 = [20u8; 32];
        let pid2 = [30u8; 32];

        map.track_segment(cid, 0);

        // Apply seq 5
        map.apply_event(&make_stored(&node, cid, 0, pid1, vec![1, 2, 3], 5));
        assert_eq!(map.total_pieces(&cid), 1);

        // Try to apply seq 3 (should be skipped)
        map.apply_event(&make_stored(&node, cid, 0, pid2, vec![4, 5, 6], 3));
        assert_eq!(map.total_pieces(&cid), 1);

        // Try same seq 5 (should be skipped)
        map.apply_event(&make_stored(&node, cid, 0, pid2, vec![4, 5, 6], 5));
        assert_eq!(map.total_pieces(&cid), 1);

        // Apply seq 6 (should work)
        map.apply_event(&make_stored(&node, cid, 0, pid2, vec![4, 5, 6], 6));
        assert_eq!(map.total_pieces(&cid), 2);
    }

    #[test]
    fn test_compute_rank() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        let cid = ContentId([1u8; 32]);
        map.track_segment(cid, 0);

        // Add 3 linearly independent vectors (identity-like for 3 dimensions)
        let node1 = vec![1u8; 32];
        let node2 = vec![2u8; 32];
        let node3 = vec![3u8; 32];

        map.apply_event(&make_stored(&node1, cid, 0, [1u8; 32], vec![1, 0, 0], 1));
        map.apply_event(&make_stored(&node2, cid, 0, [2u8; 32], vec![0, 1, 0], 1));
        map.apply_event(&make_stored(&node3, cid, 0, [3u8; 32], vec![0, 0, 1], 1));

        assert_eq!(map.compute_rank(&cid, 0, false), 3);

        // Add a dependent vector (sum of first two)
        let node4 = vec![4u8; 32];
        map.apply_event(&make_stored(&node4, cid, 0, [4u8; 32], vec![1, 1, 0], 1));
        // Rank should still be 3
        assert_eq!(map.compute_rank(&cid, 0, false), 3);
    }

    #[test]
    fn test_online_only_filtering() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        let cid = ContentId([1u8; 32]);
        map.track_segment(cid, 0);

        let node1 = vec![1u8; 32];
        let node2 = vec![2u8; 32];

        map.apply_event(&make_stored(&node1, cid, 0, [1u8; 32], vec![1, 0], 1));
        map.apply_event(&make_stored(&node2, cid, 0, [2u8; 32], vec![0, 1], 1));

        // Both offline by default
        assert_eq!(map.compute_rank(&cid, 0, true), 0);
        assert_eq!(map.compute_rank(&cid, 0, false), 2);

        // Set node1 online
        map.set_node_online(&node1, true);
        assert_eq!(map.compute_rank(&cid, 0, true), 1);
        assert_eq!(map.provider_count(&cid, true), 1);
        assert_eq!(map.provider_count(&cid, false), 2);
    }

    #[test]
    fn test_providers() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        let cid = ContentId([1u8; 32]);
        map.track_segment(cid, 0);
        map.track_segment(cid, 1);

        let node1 = vec![1u8; 32];
        let node2 = vec![2u8; 32];

        map.apply_event(&make_stored(&node1, cid, 0, [1u8; 32], vec![1], 1));
        map.apply_event(&make_stored(&node1, cid, 1, [2u8; 32], vec![2], 2));
        map.apply_event(&make_stored(&node2, cid, 0, [3u8; 32], vec![3], 1));

        let providers = map.providers(&cid);
        assert_eq!(providers.len(), 2);
        assert_eq!(map.total_pieces(&cid), 3);
    }

    #[test]
    fn test_local_pieces() {
        let local = test_peer_id();
        let local_bytes = local.to_bytes().to_vec();
        let mut map = PieceMap::new(local);
        let cid = ContentId([1u8; 32]);
        map.track_segment(cid, 0);

        map.apply_event(&make_stored(&local_bytes, cid, 0, [1u8; 32], vec![1], 1));
        map.apply_event(&make_stored(&local_bytes, cid, 0, [2u8; 32], vec![2], 2));

        assert_eq!(map.local_pieces(&cid, 0), 2);
        assert!(map.has_pieces(&cid, 0, 2));
        assert!(!map.has_pieces(&cid, 0, 3));
    }

    #[test]
    fn test_next_seq() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        assert_eq!(map.next_seq(), 1);
        assert_eq!(map.next_seq(), 2);
        assert_eq!(map.next_seq(), 3);
    }

    #[test]
    fn test_all_cids() {
        let local = test_peer_id();
        let mut map = PieceMap::new(local);
        let cid1 = ContentId([1u8; 32]);
        let cid2 = ContentId([2u8; 32]);
        let node = vec![10u8; 32];
        map.track_segment(cid1, 0);
        map.track_segment(cid2, 0);

        map.apply_event(&make_stored(&node, cid1, 0, [1u8; 32], vec![1], 1));
        map.apply_event(&make_stored(&node, cid2, 0, [2u8; 32], vec![2], 2));

        let cids = map.all_cids();
        assert_eq!(cids.len(), 2);
        assert!(cids.contains(&cid1));
        assert!(cids.contains(&cid2));
    }
}
