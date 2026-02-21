//! PieceMap — tracks known coefficient vectors for each (CID, segment, node).
//!
//! Uses `craftobj_core::PieceEvent` (Stored/Dropped) for event-sourced updates.
//! Node identifiers are `Vec<u8>` (PeerId bytes) to match core types.

use std::collections::{HashMap, HashSet};

use craftobj_core::{ContentId, PieceEvent};

/// Entry in the piece map: (node_bytes, piece_id, coefficients).
type PieceEntry = (Vec<u8>, [u8; 32], Vec<u8>);

/// In-memory piece map tracking which pieces (by coefficient vector) are
/// known to exist on which nodes for each (CID, segment).
#[derive(Debug, Default)]
pub struct PieceMap {
    /// Map: (ContentId, segment_index) → Vec<(node, piece_id, coefficients)>
    map: HashMap<(ContentId, u32), Vec<PieceEntry>>,
    /// Local node ID (PeerId bytes).
    local: Vec<u8>,
    /// Sequence counter for events.
    seq: u64,
}

impl PieceMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the local node's ID (PeerId bytes).
    pub fn set_local_node(&mut self, node: Vec<u8>) {
        self.local = node;
    }

    /// Get the local node's ID (PeerId bytes).
    pub fn local_node(&self) -> &Vec<u8> {
        &self.local
    }

    /// Get next sequence number.
    pub fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }

    /// Apply a PieceEvent from core (Stored or Dropped).
    pub fn apply_event(&mut self, event: &PieceEvent) {
        match event {
            PieceEvent::Stored(s) => {
                self.insert(&s.cid, s.segment, s.node.clone(), s.piece_id, s.coefficients.clone());
            }
            PieceEvent::Dropped(d) => {
                self.remove_piece(&d.cid, d.segment, &d.node, &d.piece_id);
            }
        }
    }

    /// Track a segment (ensures map entry exists).
    pub fn track_segment(&mut self, cid: &ContentId, segment_index: u32) {
        self.map.entry((*cid, segment_index)).or_default();
    }

    /// Remove a tracked segment (and all its pieces) from the map.
    pub fn untrack_segment(&mut self, cid: &ContentId, segment_index: u32) {
        self.map.remove(&(*cid, segment_index));
    }

    /// Record that a node holds a piece with given coefficients for a (CID, segment).
    pub fn insert(
        &mut self,
        cid: &ContentId,
        segment_index: u32,
        node: Vec<u8>,
        piece_id: [u8; 32],
        coefficients: Vec<u8>,
    ) {
        let entries = self.map.entry((*cid, segment_index)).or_default();
        // Avoid duplicates
        if !entries.iter().any(|(n, pid, _)| n == &node && *pid == piece_id) {
            entries.push((node, piece_id, coefficients));
        }
    }

    /// Remove a specific piece from the map.
    fn remove_piece(&mut self, cid: &ContentId, segment_index: u32, node: &[u8], piece_id: &[u8; 32]) {
        if let Some(entries) = self.map.get_mut(&(*cid, segment_index)) {
            entries.retain(|(n, pid, _)| n != node || pid != piece_id);
        }
    }

    /// Get all pieces for a (CID, segment).
    pub fn pieces_for_segment(&self, cid: &ContentId, segment_index: u32) -> &[PieceEntry] {
        self.map
            .get(&(*cid, segment_index))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Get total piece count across all CIDs.
    pub fn total_pieces(&self) -> usize {
        self.map.values().map(|v| v.len()).sum()
    }

    /// Get all known providers for a CID (across all segments), as node byte vecs.
    pub fn providers(&self, cid: &ContentId) -> HashSet<Vec<u8>> {
        let mut nodes = HashSet::new();
        for ((c, _), entries) in &self.map {
            if c == cid {
                for (node, _, _) in entries {
                    nodes.insert(node.clone());
                }
            }
        }
        nodes
    }

    /// Get all CIDs tracked in the map.
    pub fn all_cids(&self) -> HashSet<ContentId> {
        self.map.keys().map(|(c, _)| *c).collect()
    }

    /// Remove all entries for a CID.
    pub fn remove_cid(&mut self, cid: &ContentId) {
        self.map.retain(|(c, _), _| c != cid);
    }

    /// Number of tracked (CID, segment) pairs.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Mark a node as online/offline (no-op for now, could track liveness).
    pub fn set_node_online(&mut self, _node_bytes: &[u8], _online: bool) {
        // Placeholder — could track peer liveness in a HashSet
    }

    /// Get all (segment, piece_id) pairs for a CID that belong to the local node.
    pub fn pieces_for_cid_local(&self, cid: &ContentId) -> Vec<(u32, [u8; 32])> {
        let mut result = Vec::new();
        for ((c, seg), entries) in &self.map {
            if c == cid {
                for (node, pid, _) in entries {
                    if node == &self.local {
                        result.push((*seg, *pid));
                    }
                }
            }
        }
        result
    }

    /// Total pieces for a specific CID.
    pub fn total_pieces_for_cid(&self, cid: &ContentId) -> usize {
        self.map.iter()
            .filter(|((c, _), _)| c == cid)
            .map(|(_, v)| v.len())
            .sum()
    }
}
