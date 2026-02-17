//! E2E integration tests for DataCraft's core content flow.
//!
//! Tests the full content lifecycle: publish → distribute → fetch → delete.
//! Uses library crates directly (no daemon processes).

use std::collections::HashSet;
use std::path::PathBuf;

use craftec_erasure::{check_independence, CodedPiece};
use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, PublishOptions};
use datacraft_store::FsStore;

/// Create a unique temp directory for a test node.
fn node_dir(label: &str) -> PathBuf {
    use rand::RngCore;
    let mut rng = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut rng);
    let dir = std::env::temp_dir()
        .join("datacraft-e2e-flow")
        .join(format!("{}-{}", label, hex::encode(rng)));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Generate deterministic test data of the given size.
fn test_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 7 + 13) % 256) as u8).collect()
}

/// Count total pieces across all segments for a CID in a store.
fn count_pieces(store: &FsStore, cid: &ContentId, segment_count: usize) -> usize {
    (0..segment_count as u32)
        .map(|seg| store.list_pieces(cid, seg).unwrap_or_default().len())
        .sum()
}

/// Simulate round-robin push distribution from publisher to storage nodes.
/// Pushes all pieces, round-robin across nodes, then deletes from publisher.
fn distribute_round_robin(
    publisher_store: &FsStore,
    storage_stores: &[&FsStore],
    cid: &ContentId,
    segment_count: usize,
) {
    let manifest = publisher_store.get_manifest(cid).unwrap();

    // Push manifest to all storage nodes
    for store in storage_stores {
        store.store_manifest(&manifest).unwrap();
    }

    // Round-robin distribute ALL pieces to storage nodes
    let mut node_idx = 0;
    for seg in 0..segment_count as u32 {
        let piece_ids = publisher_store.list_pieces(cid, seg).unwrap();
        for pid in &piece_ids {
            let (data, coeff) = publisher_store.get_piece(cid, seg, pid).unwrap();
            storage_stores[node_idx]
                .store_piece(cid, seg, pid, &data, &coeff)
                .unwrap();
            node_idx = (node_idx + 1) % storage_stores.len();
        }
    }

    // Publisher deletes local pieces after successful push
    for seg in 0..segment_count as u32 {
        let piece_ids = publisher_store.list_pieces(cid, seg).unwrap();
        for pid in &piece_ids {
            publisher_store.delete_piece(cid, seg, pid).unwrap();
        }
    }
}

// ─── Test 1: Publish and Initial Distribution ───

#[test]
fn test_publish_and_initial_distribution() {
    let pub_dir = node_dir("pub");
    let store1_dir = node_dir("store1");
    let store2_dir = node_dir("store2");

    let mut publisher = DataCraftClient::new(&pub_dir).unwrap();
    let store1 = FsStore::new(&store1_dir).unwrap();
    let store2 = FsStore::new(&store2_dir).unwrap();

    // Publish ~1MB test file
    let data = test_data(1_000_000);
    let result = publisher
        .publish_bytes(&data, &PublishOptions::default())
        .unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;

    // Verify publisher created k + parity pieces per segment
    let pub_store = publisher.store();
    for seg in 0..seg_count as u32 {
        let k = result.manifest.k_for_segment(seg as usize);
        let pieces = pub_store.list_pieces(&cid, seg).unwrap();
        // k source + parity pieces (default 20% = initial_parity)
        assert!(
            pieces.len() > k,
            "Segment {}: should have more than k={} pieces, got {}",
            seg, k, pieces.len()
        );
    }

    let total_before = count_pieces(pub_store, &cid, seg_count);
    assert!(total_before > 0);

    // Distribute round-robin to 2 storage nodes
    distribute_round_robin(pub_store, &[&store1, &store2], &cid, seg_count);

    // Verify: publisher has 0 local pieces after distribution
    let pub_pieces_after = count_pieces(pub_store, &cid, seg_count);
    assert_eq!(pub_pieces_after, 0, "Publisher should have 0 pieces after distribution");

    // Verify: ALL pieces distributed to storage nodes
    let s1_count = count_pieces(&store1, &cid, seg_count);
    let s2_count = count_pieces(&store2, &cid, seg_count);
    assert_eq!(
        s1_count + s2_count, total_before,
        "Total pieces on storage nodes should equal original"
    );

    // Verify: approximately equal distribution (within 1 piece)
    let diff = (s1_count as isize - s2_count as isize).unsigned_abs();
    assert!(diff <= 1, "Uneven distribution: s1={}, s2={}", s1_count, s2_count);

    for d in [&pub_dir, &store1_dir, &store2_dir] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ─── Test 2: Equalization ───

#[test]
fn test_equalization() {
    let pub_dir = node_dir("eq-pub");
    let s1_dir = node_dir("eq-s1");
    let s2_dir = node_dir("eq-s2");
    let s3_dir = node_dir("eq-s3");

    let mut publisher = DataCraftClient::new(&pub_dir).unwrap();
    let store1 = FsStore::new(&s1_dir).unwrap();
    let store2 = FsStore::new(&s2_dir).unwrap();
    let store3 = FsStore::new(&s3_dir).unwrap();

    let data = test_data(1_000_000);
    let result = publisher
        .publish_bytes(&data, &PublishOptions::default())
        .unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;

    distribute_round_robin(publisher.store(), &[&store1, &store2], &cid, seg_count);

    let s1_before = count_pieces(&store1, &cid, seg_count);
    let s2_before = count_pieces(&store2, &cid, seg_count);
    let total = s1_before + s2_before;

    // Add 3rd node — equalization: each existing node pushes HALF its pieces
    store3
        .store_manifest(&store1.get_manifest(&cid).unwrap())
        .unwrap();

    for source in [&store1, &store2] {
        for seg in 0..seg_count as u32 {
            let pids = source.list_pieces(&cid, seg).unwrap();
            let half = pids.len() / 2;
            for pid in pids.iter().take(half) {
                let (data, coeff) = source.get_piece(&cid, seg, pid).unwrap();
                store3.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
                // DELETE from source — no duplicates
                source.delete_piece(&cid, seg, pid).unwrap();
            }
        }
    }

    let s1_after = count_pieces(&store1, &cid, seg_count);
    let s2_after = count_pieces(&store2, &cid, seg_count);
    let s3_after = count_pieces(&store3, &cid, seg_count);

    // Total preserved
    assert_eq!(s1_after + s2_after + s3_after, total);

    // Sources lost pieces
    assert!(s1_after < s1_before);
    assert!(s2_after < s2_before);

    // New node is a viable provider (≥2 pieces)
    assert!(s3_after >= 2, "New node needs ≥2 pieces, got {}", s3_after);

    for d in [&pub_dir, &s1_dir, &s2_dir, &s3_dir] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ─── Test 3: Fetch After Distribution ───

#[test]
fn test_fetch_after_distribution() {
    let pub_dir = node_dir("fetch-pub");
    let s1_dir = node_dir("fetch-s1");
    let s2_dir = node_dir("fetch-s2");
    let client_dir = node_dir("fetch-client");

    let mut publisher = DataCraftClient::new(&pub_dir).unwrap();
    let store1 = FsStore::new(&s1_dir).unwrap();
    let store2 = FsStore::new(&s2_dir).unwrap();

    let original = test_data(1_000_000);
    let result = publisher
        .publish_bytes(&original, &PublishOptions::default())
        .unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;
    let manifest = result.manifest.clone();

    distribute_round_robin(publisher.store(), &[&store1, &store2], &cid, seg_count);

    // Fresh client fetches from storage providers
    let client_store = FsStore::new(&client_dir).unwrap();
    client_store.store_manifest(&manifest).unwrap();

    for seg in 0..seg_count as u32 {
        let k = manifest.k_for_segment(seg as usize);
        let mut collected: Vec<CodedPiece> = Vec::new();

        // Request "any piece" from providers, check independence
        for provider in [&store1, &store2] {
            if collected.len() >= k {
                break;
            }
            for pid in provider.list_pieces(&cid, seg).unwrap_or_default() {
                if collected.len() >= k {
                    break;
                }
                let (data, coeff) = provider.get_piece(&cid, seg, &pid).unwrap();

                // Linear independence check
                let mut test_coeffs: Vec<Vec<u8>> =
                    collected.iter().map(|p| p.coefficients.clone()).collect();
                test_coeffs.push(coeff.clone());
                let rank = check_independence(&test_coeffs);

                if rank > collected.len() {
                    client_store
                        .store_piece(&cid, seg, &pid, &data, &coeff)
                        .unwrap();
                    collected.push(CodedPiece { data, coefficients: coeff });
                }
            }
        }

        assert!(collected.len() >= k, "Seg {}: need {} independent, got {}", seg, k, collected.len());
    }

    // Reconstruct and verify
    let client = DataCraftClient::new(&client_dir).unwrap();
    let output = client_dir.join("output.bin");
    client.reconstruct(&cid, &output, None).unwrap();

    let reconstructed = std::fs::read(&output).unwrap();
    assert_eq!(reconstructed, original);

    // SHA-256 matches CID
    let reconstructed_cid = ContentId::from_bytes(&reconstructed);
    assert_eq!(reconstructed_cid, cid);

    for d in [&pub_dir, &s1_dir, &s2_dir, &client_dir] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ─── Test 4: Piece Uniqueness ───

#[test]
fn test_piece_uniqueness_after_distribution() {
    let pub_dir = node_dir("uniq-pub");
    let s1_dir = node_dir("uniq-s1");
    let s2_dir = node_dir("uniq-s2");
    let s3_dir = node_dir("uniq-s3");

    let mut publisher = DataCraftClient::new(&pub_dir).unwrap();
    let store1 = FsStore::new(&s1_dir).unwrap();
    let store2 = FsStore::new(&s2_dir).unwrap();
    let store3 = FsStore::new(&s3_dir).unwrap();

    let data = test_data(1_000_000);
    let result = publisher
        .publish_bytes(&data, &PublishOptions::default())
        .unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;

    let total_original = count_pieces(publisher.store(), &cid, seg_count);

    distribute_round_robin(publisher.store(), &[&store1, &store2, &store3], &cid, seg_count);

    let stores = [&store1, &store2, &store3];
    let total_distributed: usize = stores.iter().map(|s| count_pieces(s, &cid, seg_count)).sum();
    assert_eq!(total_distributed, total_original);

    // No piece appears on more than one node
    for seg in 0..seg_count as u32 {
        let mut seen: HashSet<[u8; 32]> = HashSet::new();
        for (i, store) in stores.iter().enumerate() {
            for pid in store.list_pieces(&cid, seg).unwrap_or_default() {
                assert!(
                    seen.insert(pid),
                    "Duplicate piece in seg {} on node {}: {}",
                    seg, i, hex::encode(&pid[..8])
                );
            }
        }
    }

    for d in [&pub_dir, &s1_dir, &s2_dir, &s3_dir] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ─── Test 5: Content Deletion (Local) ───

#[test]
fn test_content_deletion_local() {
    let s1_dir = node_dir("del-s1");
    let s2_dir = node_dir("del-s2");
    let pub_dir = node_dir("del-pub");

    let mut publisher = DataCraftClient::new(&pub_dir).unwrap();
    let store1 = FsStore::new(&s1_dir).unwrap();
    let store2 = FsStore::new(&s2_dir).unwrap();

    let data = test_data(500_000);
    let result = publisher
        .publish_bytes(&data, &PublishOptions::default())
        .unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;

    distribute_round_robin(publisher.store(), &[&store1, &store2], &cid, seg_count);

    let s2_before = count_pieces(&store2, &cid, seg_count);
    assert!(s2_before > 0);

    // Store1 deletes locally
    store1.delete_content(&cid).unwrap();

    // Pieces removed from disk
    assert_eq!(count_pieces(&store1, &cid, seg_count), 0);

    // Manifest removed
    assert!(!store1.has_manifest(&cid));

    // Content listing reflects deletion
    assert!(!store1.list_content_with_pieces().unwrap().contains(&cid));
    assert!(!store1.list_content().unwrap().contains(&cid));

    // Other node unaffected
    assert_eq!(count_pieces(&store2, &cid, seg_count), s2_before);

    for d in [&pub_dir, &s1_dir, &s2_dir] {
        std::fs::remove_dir_all(d).ok();
    }
}
