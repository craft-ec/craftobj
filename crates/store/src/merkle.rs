//! Storage Merkle Tree
//!
//! A per-node Merkle tree over all stored pieces. Provides proof of total storage
//! inventory with a single root hash, inclusion proofs, and incremental updates.
//!
//! Leaf = SHA-256(content_id || segment_index_be || piece_id)
//! Internal nodes = SHA-256(left || right)
//! Leaves are sorted for deterministic ordering.

use sha2::{Digest, Sha256};

use crate::FsStore;
use datacraft_core::{ContentId, Result};

/// Compute a Merkle leaf from its components.
pub fn compute_leaf(content_id: &ContentId, segment_index: u32, piece_id: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(content_id.0);
    hasher.update(segment_index.to_be_bytes());
    hasher.update(piece_id);
    let hash = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&hash);
    out
}

/// A sibling node in a Merkle proof, with its position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofEntry {
    /// The sibling hash.
    pub hash: [u8; 32],
    /// True if this sibling is on the left (i.e., the proven node is the right child).
    pub is_left: bool,
}

/// Merkle inclusion proof for a single leaf.
#[derive(Debug, Clone)]
pub struct MerkleProof {
    pub leaf: [u8; 32],
    pub path: Vec<ProofEntry>,
}

/// The empty tree root: SHA-256 of the empty string.
fn empty_root() -> [u8; 32] {
    let hash = Sha256::digest(b"");
    let mut out = [0u8; 32];
    out.copy_from_slice(&hash);
    out
}

/// SHA-256(left || right)
fn hash_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    let h = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&h);
    out
}

/// Diff result between two tree states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MerkleDiff {
    /// Leaves present in the current tree but not in the other (additions).
    pub added: Vec<[u8; 32]>,
    /// Leaves present in the other tree but not in the current tree (removals).
    pub removed: Vec<[u8; 32]>,
}

/// Storage Merkle tree over all pieces held by a node.
#[derive(Debug, Clone)]
pub struct StorageMerkleTree {
    /// Sorted leaves.
    leaves: Vec<[u8; 32]>,
    /// Cached root.
    cached_root: [u8; 32],
}

impl StorageMerkleTree {
    /// Build from an existing store by scanning all pieces.
    pub fn build_from_store(store: &FsStore) -> Result<Self> {
        let mut leaves = Vec::new();
        let content_ids = store.list_content_with_pieces()?;
        for cid in &content_ids {
            let segments = store.list_segments(cid)?;
            for seg in segments {
                let pieces = store.list_pieces(cid, seg)?;
                for piece_id in &pieces {
                    leaves.push(compute_leaf(cid, seg, piece_id));
                }
            }
        }
        leaves.sort();
        let root = Self::compute_root_from_leaves(&leaves);
        Ok(Self {
            leaves,
            cached_root: root,
        })
    }

    /// Create an empty tree.
    pub fn new() -> Self {
        Self {
            leaves: Vec::new(),
            cached_root: empty_root(),
        }
    }

    /// The storage root hash.
    pub fn root(&self) -> [u8; 32] {
        self.cached_root
    }

    /// Number of leaves.
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// Whether the tree is empty.
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    /// Insert a piece (incremental). Maintains sorted order and recomputes root.
    pub fn insert(&mut self, content_id: &ContentId, segment_index: u32, piece_id: &[u8; 32]) {
        let leaf = compute_leaf(content_id, segment_index, piece_id);
        let pos = self.leaves.binary_search(&leaf).unwrap_or_else(|p| p);
        // Don't insert duplicates
        if pos < self.leaves.len() && self.leaves[pos] == leaf {
            return;
        }
        self.leaves.insert(pos, leaf);
        self.cached_root = Self::compute_root_from_leaves(&self.leaves);
    }

    /// Insert multiple pieces at once (more efficient than repeated single inserts).
    pub fn insert_batch(&mut self, pieces: &[(&ContentId, u32, &[u8; 32])]) {
        for &(content_id, segment_index, piece_id) in pieces {
            let leaf = compute_leaf(content_id, segment_index, piece_id);
            let pos = self.leaves.binary_search(&leaf).unwrap_or_else(|p| p);
            if pos < self.leaves.len() && self.leaves[pos] == leaf {
                continue;
            }
            self.leaves.insert(pos, leaf);
        }
        self.cached_root = Self::compute_root_from_leaves(&self.leaves);
    }

    /// Remove a piece (incremental). Recomputes root.
    pub fn remove(&mut self, content_id: &ContentId, segment_index: u32, piece_id: &[u8; 32]) {
        let leaf = compute_leaf(content_id, segment_index, piece_id);
        if let Ok(pos) = self.leaves.binary_search(&leaf) {
            self.leaves.remove(pos);
            self.cached_root = Self::compute_root_from_leaves(&self.leaves);
        }
    }

    /// Generate an inclusion proof for a piece.
    pub fn proof_for_piece(
        &self,
        content_id: &ContentId,
        segment_index: u32,
        piece_id: &[u8; 32],
    ) -> Option<MerkleProof> {
        let leaf = compute_leaf(content_id, segment_index, piece_id);
        let idx = self.leaves.binary_search(&leaf).ok()?;
        let path = self.build_proof_path(idx);
        Some(MerkleProof { leaf, path })
    }

    /// Verify an inclusion proof against a root.
    pub fn verify_proof(root: &[u8; 32], proof: &MerkleProof) -> bool {
        if proof.path.is_empty() {
            // Single-leaf tree or empty proof: root should equal the leaf
            return *root == proof.leaf;
        }
        let mut current = proof.leaf;
        for entry in &proof.path {
            if entry.is_left {
                current = hash_pair(&entry.hash, &current);
            } else {
                current = hash_pair(&current, &entry.hash);
            }
        }
        current == *root
    }

    /// Compute root from a slice of sorted leaves.
    fn compute_root_from_leaves(leaves: &[[u8; 32]]) -> [u8; 32] {
        if leaves.is_empty() {
            return empty_root();
        }
        if leaves.len() == 1 {
            return leaves[0];
        }
        // Build tree bottom-up
        let mut level: Vec<[u8; 32]> = leaves.to_vec();
        while level.len() > 1 {
            let mut next = Vec::with_capacity((level.len() + 1) / 2);
            let mut i = 0;
            while i < level.len() {
                if i + 1 < level.len() {
                    next.push(hash_pair(&level[i], &level[i + 1]));
                } else {
                    // Odd node: hash with itself
                    next.push(hash_pair(&level[i], &level[i]));
                }
                i += 2;
            }
            level = next;
        }
        level[0]
    }

    /// Export leaves for transmission to another node.
    pub fn leaves(&self) -> &[[u8; 32]] {
        &self.leaves
    }

    /// Build a tree from a set of leaves (for remote tree reconstruction).
    /// Leaves will be sorted and deduplicated.
    pub fn from_leaves(mut leaves: Vec<[u8; 32]>) -> Self {
        leaves.sort();
        leaves.dedup();
        let root = Self::compute_root_from_leaves(&leaves);
        Self {
            leaves,
            cached_root: root,
        }
    }

    /// Quick check: has anything changed since the given root?
    pub fn has_changed_since(&self, old_root: &[u8; 32]) -> bool {
        self.cached_root != *old_root
    }

    /// Compute diff: what changed between `other` (old state) and `self` (current state).
    /// Uses O(n+m) sorted merge comparison.
    pub fn diff(&self, other: &StorageMerkleTree) -> MerkleDiff {
        self.diff_from_leaves(&other.leaves)
    }

    /// Compute diff from a previous sorted leaf set.
    /// `old_leaves` must be sorted.
    pub fn diff_from_leaves(&self, old_leaves: &[[u8; 32]]) -> MerkleDiff {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut i = 0; // index into self.leaves (current)
        let mut j = 0; // index into old_leaves

        while i < self.leaves.len() && j < old_leaves.len() {
            match self.leaves[i].cmp(&old_leaves[j]) {
                std::cmp::Ordering::Equal => {
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => {
                    // In current but not in old → added
                    added.push(self.leaves[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    // In old but not in current → removed
                    removed.push(old_leaves[j]);
                    j += 1;
                }
            }
        }
        // Remaining in current = added
        while i < self.leaves.len() {
            added.push(self.leaves[i]);
            i += 1;
        }
        // Remaining in old = removed
        while j < old_leaves.len() {
            removed.push(old_leaves[j]);
            j += 1;
        }

        MerkleDiff { added, removed }
    }

    /// Build proof path for leaf at given index.
    fn build_proof_path(&self, leaf_index: usize) -> Vec<ProofEntry> {
        if self.leaves.len() <= 1 {
            return Vec::new();
        }
        let mut path = Vec::new();
        let mut level: Vec<[u8; 32]> = self.leaves.clone();
        let mut idx = leaf_index;

        while level.len() > 1 {
            let sibling_idx = if idx % 2 == 0 { idx + 1 } else { idx - 1 };
            let sibling_hash = if sibling_idx < level.len() {
                level[sibling_idx]
            } else {
                // Odd: duplicate self
                level[idx]
            };
            let is_left = idx % 2 == 1; // sibling is on the left if we're the right child
            path.push(ProofEntry {
                hash: sibling_hash,
                is_left,
            });
            // Build next level
            let mut next = Vec::with_capacity((level.len() + 1) / 2);
            let mut i = 0;
            while i < level.len() {
                if i + 1 < level.len() {
                    next.push(hash_pair(&level[i], &level[i + 1]));
                } else {
                    next.push(hash_pair(&level[i], &level[i]));
                }
                i += 2;
            }
            idx /= 2;
            level = next;
        }
        path
    }
}

impl Default for StorageMerkleTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(n: u8) -> ContentId {
        let mut id = [0u8; 32];
        id[0] = n;
        ContentId(id)
    }

    fn pid(n: u8) -> [u8; 32] {
        let mut id = [0u8; 32];
        id[0] = n;
        id
    }

    #[test]
    fn test_empty_tree() {
        let tree = StorageMerkleTree::new();
        assert_eq!(tree.root(), empty_root());
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
    }

    #[test]
    fn test_single_leaf() {
        let mut tree = StorageMerkleTree::new();
        tree.insert(&cid(1), 0, &pid(1));
        assert_eq!(tree.len(), 1);
        let leaf = compute_leaf(&cid(1), 0, &pid(1));
        assert_eq!(tree.root(), leaf);
    }

    #[test]
    fn test_deterministic_ordering() {
        // Insert in different orders, same root
        let mut tree1 = StorageMerkleTree::new();
        tree1.insert(&cid(1), 0, &pid(1));
        tree1.insert(&cid(2), 0, &pid(2));
        tree1.insert(&cid(3), 1, &pid(3));

        let mut tree2 = StorageMerkleTree::new();
        tree2.insert(&cid(3), 1, &pid(3));
        tree2.insert(&cid(1), 0, &pid(1));
        tree2.insert(&cid(2), 0, &pid(2));

        assert_eq!(tree1.root(), tree2.root());
    }

    #[test]
    fn test_incremental_insert_remove() {
        let mut tree = StorageMerkleTree::new();
        tree.insert(&cid(1), 0, &pid(1));
        tree.insert(&cid(2), 0, &pid(2));
        let root_with_two = tree.root();

        tree.insert(&cid(3), 0, &pid(3));
        assert_ne!(tree.root(), root_with_two);

        tree.remove(&cid(3), 0, &pid(3));
        assert_eq!(tree.root(), root_with_two);
    }

    #[test]
    fn test_duplicate_insert() {
        let mut tree = StorageMerkleTree::new();
        tree.insert(&cid(1), 0, &pid(1));
        let root1 = tree.root();
        tree.insert(&cid(1), 0, &pid(1)); // duplicate
        assert_eq!(tree.root(), root1);
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut tree = StorageMerkleTree::new();
        tree.insert(&cid(1), 0, &pid(1));
        let root = tree.root();
        tree.remove(&cid(99), 0, &pid(99)); // doesn't exist
        assert_eq!(tree.root(), root);
    }

    #[test]
    fn test_proof_generation_and_verification() {
        let mut tree = StorageMerkleTree::new();
        for i in 0..10u8 {
            tree.insert(&cid(i), 0, &pid(i));
        }

        let root = tree.root();

        // Verify proof for each leaf
        for i in 0..10u8 {
            let proof = tree.proof_for_piece(&cid(i), 0, &pid(i)).unwrap();
            assert!(
                StorageMerkleTree::verify_proof(&root, &proof),
                "Proof failed for piece {}",
                i
            );
        }

        // Non-existent piece should return None
        assert!(tree.proof_for_piece(&cid(99), 0, &pid(99)).is_none());
    }

    #[test]
    fn test_proof_fails_with_wrong_root() {
        let mut tree = StorageMerkleTree::new();
        tree.insert(&cid(1), 0, &pid(1));
        tree.insert(&cid(2), 0, &pid(2));

        let proof = tree.proof_for_piece(&cid(1), 0, &pid(1)).unwrap();
        let wrong_root = [0xFFu8; 32];
        assert!(!StorageMerkleTree::verify_proof(&wrong_root, &proof));
    }

    #[test]
    fn test_proof_with_odd_number_of_leaves() {
        let mut tree = StorageMerkleTree::new();
        for i in 0..7u8 {
            tree.insert(&cid(i), i as u32, &pid(i));
        }
        let root = tree.root();
        for i in 0..7u8 {
            let proof = tree.proof_for_piece(&cid(i), i as u32, &pid(i)).unwrap();
            assert!(StorageMerkleTree::verify_proof(&root, &proof));
        }
    }

    #[test]
    fn test_build_from_store() {
        let dir = std::env::temp_dir()
            .join("datacraft-merkle-test")
            .join(format!(
                "{}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .subsec_nanos()
            ));
        let store = FsStore::new(&dir).unwrap();

        let cid1 = ContentId::from_bytes(b"merkle-test-1");
        let cid2 = ContentId::from_bytes(b"merkle-test-2");

        let coeff1 = vec![1u8, 0, 0];
        let coeff2 = vec![0u8, 1, 0];
        let coeff3 = vec![0u8, 0, 1];

        let pid1 = crate::piece_id_from_coefficients(&coeff1);
        let pid2 = crate::piece_id_from_coefficients(&coeff2);
        let pid3 = crate::piece_id_from_coefficients(&coeff3);

        store.store_piece(&cid1, 0, &pid1, b"d1", &coeff1).unwrap();
        store.store_piece(&cid1, 1, &pid2, b"d2", &coeff2).unwrap();
        store.store_piece(&cid2, 0, &pid3, b"d3", &coeff3).unwrap();

        let tree = StorageMerkleTree::build_from_store(&store).unwrap();
        assert_eq!(tree.len(), 3);

        // Verify all proofs
        let root = tree.root();
        let proof1 = tree.proof_for_piece(&cid1, 0, &pid1).unwrap();
        assert!(StorageMerkleTree::verify_proof(&root, &proof1));
        let proof2 = tree.proof_for_piece(&cid1, 1, &pid2).unwrap();
        assert!(StorageMerkleTree::verify_proof(&root, &proof2));
        let proof3 = tree.proof_for_piece(&cid2, 0, &pid3).unwrap();
        assert!(StorageMerkleTree::verify_proof(&root, &proof3));

        // Build manually with insert and compare roots
        let mut manual = StorageMerkleTree::new();
        manual.insert(&cid2, 0, &pid3);
        manual.insert(&cid1, 1, &pid2);
        manual.insert(&cid1, 0, &pid1);
        assert_eq!(tree.root(), manual.root());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_diff_empty_trees() {
        let t1 = StorageMerkleTree::new();
        let t2 = StorageMerkleTree::new();
        let diff = t1.diff(&t2);
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_diff_additions() {
        let old = StorageMerkleTree::new();
        let mut current = StorageMerkleTree::new();
        current.insert(&cid(1), 0, &pid(1));
        current.insert(&cid(2), 0, &pid(2));
        let diff = current.diff(&old);
        assert_eq!(diff.added.len(), 2);
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_diff_removals() {
        let mut old = StorageMerkleTree::new();
        old.insert(&cid(1), 0, &pid(1));
        old.insert(&cid(2), 0, &pid(2));
        let current = StorageMerkleTree::new();
        let diff = current.diff(&old);
        assert!(diff.added.is_empty());
        assert_eq!(diff.removed.len(), 2);
    }

    #[test]
    fn test_diff_mixed() {
        let mut old = StorageMerkleTree::new();
        old.insert(&cid(1), 0, &pid(1));
        old.insert(&cid(2), 0, &pid(2));

        let mut current = StorageMerkleTree::new();
        current.insert(&cid(2), 0, &pid(2));
        current.insert(&cid(3), 0, &pid(3));

        let diff = current.diff(&old);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.removed.len(), 1);
        assert_eq!(diff.added[0], compute_leaf(&cid(3), 0, &pid(3)));
        assert_eq!(diff.removed[0], compute_leaf(&cid(1), 0, &pid(1)));
    }

    #[test]
    fn test_diff_identical() {
        let mut t1 = StorageMerkleTree::new();
        t1.insert(&cid(1), 0, &pid(1));
        t1.insert(&cid(2), 0, &pid(2));
        let t2 = t1.clone();
        let diff = t1.diff(&t2);
        assert!(diff.added.is_empty());
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_diff_from_leaves() {
        let mut current = StorageMerkleTree::new();
        current.insert(&cid(1), 0, &pid(1));
        current.insert(&cid(2), 0, &pid(2));

        let old_leaves = vec![compute_leaf(&cid(1), 0, &pid(1))];
        let diff = current.diff_from_leaves(&old_leaves);
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0], compute_leaf(&cid(2), 0, &pid(2)));
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn test_has_changed_since() {
        let mut tree = StorageMerkleTree::new();
        let root1 = tree.root();
        assert!(!tree.has_changed_since(&root1));
        tree.insert(&cid(1), 0, &pid(1));
        assert!(tree.has_changed_since(&root1));
        assert!(!tree.has_changed_since(&tree.root()));
    }

    #[test]
    fn test_from_leaves_roundtrip() {
        let mut tree = StorageMerkleTree::new();
        tree.insert(&cid(1), 0, &pid(1));
        tree.insert(&cid(2), 0, &pid(2));
        tree.insert(&cid(3), 1, &pid(3));

        let exported = tree.leaves().to_vec();
        let reconstructed = StorageMerkleTree::from_leaves(exported);
        assert_eq!(tree.root(), reconstructed.root());
        assert_eq!(tree.len(), reconstructed.len());
    }

    #[test]
    fn test_large_tree_performance() {
        // ~10,000 pieces should be fast using batch insert
        let mut tree = StorageMerkleTree::new();
        let mut pieces: Vec<(ContentId, u32, [u8; 32])> = Vec::with_capacity(10_000);
        for i in 0u32..10_000 {
            let mut c = [0u8; 32];
            c[..4].copy_from_slice(&i.to_be_bytes());
            let mut p = [0u8; 32];
            p[..4].copy_from_slice(&i.to_le_bytes());
            pieces.push((ContentId(c), 0, p));
        }
        let refs: Vec<(&ContentId, u32, &[u8; 32])> = pieces
            .iter()
            .map(|(c, s, p)| (c, *s, p))
            .collect();
        tree.insert_batch(&refs);
        assert_eq!(tree.len(), 10_000);

        // Verify a random proof
        let mut c = [0u8; 32];
        c[..4].copy_from_slice(&5000u32.to_be_bytes());
        let mut p = [0u8; 32];
        p[..4].copy_from_slice(&5000u32.to_le_bytes());
        let proof = tree.proof_for_piece(&ContentId(c), 0, &p).unwrap();
        assert!(StorageMerkleTree::verify_proof(&tree.root(), &proof));
    }
}
