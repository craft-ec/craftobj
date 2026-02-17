//! E2E integration tests for DataCraft's core content flow.
//!
//! Tests the full lifecycle: publish → distribute → equalize → fetch,
//! plus partial push retry and piece uniqueness invariants.

use std::collections::HashSet;
use std::path::PathBuf;

use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, PublishOptions};
use datacraft_store::FsStore;
use rand::RngCore;

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("datacraft-e2e-flow")
        .join(format!(
            "{}-{}",
            label,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn random_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut data);
    data
}

/// Helper: count total pieces across all segments for a content in a store.
fn total_piece_count(store: &FsStore, cid: &ContentId, segment_count: usize) -> usize {
    (0..segment_count as u32)
        .map(|seg| store.list_pieces(cid, seg).unwrap().len())
        .sum()
}

// ===========================================================================
// Test 1: Publish and Distribute
// ===========================================================================

#[test]
fn test_publish_and_distribute() {
    let dir_pub = temp_dir("pub");
    let dir_s1 = temp_dir("storage1");
    let dir_s2 = temp_dir("storage2");

    // Publisher encodes a test file
    let mut client = DataCraftClient::new(&dir_pub).unwrap();
    let content = random_data(1_000_000); // 1MB
    let file_path = dir_pub.join("testfile.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let segment_count = result.segment_count;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();

    let total_encoded = total_piece_count(&store_pub, &cid, segment_count);
    assert!(total_encoded > 0, "publisher should have pieces after publish");

    // Push manifest to both storage nodes
    store_s1.store_manifest(&manifest).unwrap();
    store_s2.store_manifest(&manifest).unwrap();

    // Round-robin push ALL pieces to storage nodes, then delete from publisher
    let stores = [&store_s1, &store_s2];
    for seg in 0..segment_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for (i, pid) in pieces.iter().enumerate() {
            let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
            stores[i % 2].store_piece(&cid, seg, pid, &data, &coeff).unwrap();
        }
        // Delete all pieces from publisher
        for pid in &pieces {
            store_pub.delete_piece(&cid, seg, pid).unwrap();
        }
    }

    // Verify: publisher has 0 local pieces
    assert_eq!(total_piece_count(&store_pub, &cid, segment_count), 0);

    // Verify: each storage node received pieces
    let s1_count = total_piece_count(&store_s1, &cid, segment_count);
    let s2_count = total_piece_count(&store_s2, &cid, segment_count);
    assert!(s1_count > 0, "storage1 should have pieces");
    assert!(s2_count > 0, "storage2 should have pieces");

    // Verify: total across storage == total encoded
    assert_eq!(s1_count + s2_count, total_encoded);

    for d in [&dir_pub, &dir_s1, &dir_s2] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ===========================================================================
// Test 2: Equalization
// ===========================================================================

#[test]
fn test_equalization() {
    let dir_s1 = temp_dir("eq-s1");
    let dir_s2 = temp_dir("eq-s2");
    let dir_s3 = temp_dir("eq-s3");
    let dir_pub = temp_dir("eq-pub");

    // Publish content
    let mut client = DataCraftClient::new(&dir_pub).unwrap();
    let content = random_data(500_000);
    let file_path = dir_pub.join("eq_test.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let segment_count = result.segment_count;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();
    let store_s3 = FsStore::new(&dir_s3).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();

    // Distribute pieces between s1 and s2 (round-robin)
    store_s1.store_manifest(&manifest).unwrap();
    store_s2.store_manifest(&manifest).unwrap();
    store_s3.store_manifest(&manifest).unwrap();

    for seg in 0..segment_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for (i, pid) in pieces.iter().enumerate() {
            let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
            if i % 2 == 0 {
                store_s1.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            } else {
                store_s2.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            }
        }
    }

    // s3 starts as non-provider (0 pieces)
    assert_eq!(total_piece_count(&store_s3, &cid, segment_count), 0);

    let s1_before = total_piece_count(&store_s1, &cid, segment_count);
    let s2_before = total_piece_count(&store_s2, &cid, segment_count);

    // Equalization: nodes with >2 pieces push HALF to the non-provider
    // Simulate equalization logic per segment
    for seg in 0..segment_count as u32 {
        for source_store in [&store_s1, &store_s2] {
            let pieces = source_store.list_pieces(&cid, seg).unwrap();
            if pieces.len() > 2 {
                let to_push = pieces.len() / 2;
                for pid in pieces.iter().take(to_push) {
                    let (data, coeff) = source_store.get_piece(&cid, seg, pid).unwrap();
                    store_s3.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
                    source_store.delete_piece(&cid, seg, pid).unwrap();
                }
            }
        }
    }

    let s1_after = total_piece_count(&store_s1, &cid, segment_count);
    let s2_after = total_piece_count(&store_s2, &cid, segment_count);
    let s3_after = total_piece_count(&store_s3, &cid, segment_count);

    // Verify: senders lost pieces
    assert!(s1_after < s1_before || s1_before <= 2, "s1 should have pushed pieces");
    assert!(s2_after < s2_before || s2_before <= 2, "s2 should have pushed pieces");

    // Verify: non-provider now has pieces (is a provider)
    assert!(s3_after >= 2, "s3 should have ≥2 pieces, got {}", s3_after);

    // Verify: total pieces conserved
    let total_before = s1_before + s2_before;
    let total_after = s1_after + s2_after + s3_after;
    assert_eq!(total_before, total_after, "total pieces must be conserved");

    for d in [&dir_pub, &dir_s1, &dir_s2, &dir_s3] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ===========================================================================
// Test 3: Fetch
// ===========================================================================

#[test]
fn test_fetch_and_reconstruct() {
    let dir_pub = temp_dir("fetch-pub");
    let dir_s1 = temp_dir("fetch-s1");
    let dir_s2 = temp_dir("fetch-s2");
    let dir_client = temp_dir("fetch-client");

    // Publish
    let mut client = DataCraftClient::new(&dir_pub).unwrap();
    let content = random_data(500_000);
    let file_path = dir_pub.join("fetch_test.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let segment_count = result.segment_count;

    // Verify CID == SHA-256 of content
    let expected_cid = ContentId::from_bytes(&content);
    assert_eq!(cid, expected_cid, "CID must be SHA-256 of content");

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();

    // Distribute to storage nodes
    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();
    store_s1.store_manifest(&manifest).unwrap();
    store_s2.store_manifest(&manifest).unwrap();

    for seg in 0..segment_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for (i, pid) in pieces.iter().enumerate() {
            let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
            if i % 2 == 0 {
                store_s1.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            } else {
                store_s2.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            }
        }
    }

    // Client fetches: gather k independent pieces per segment from storage nodes
    let store_client = FsStore::new(&dir_client).unwrap();
    store_client.store_manifest(&manifest).unwrap();

    for seg in 0..segment_count as u32 {
        let k = manifest.k_for_segment(seg as usize);
        let mut collected = 0;

        for source in [&store_s1, &store_s2] {
            if collected >= k {
                break;
            }
            for pid in source.list_pieces(&cid, seg).unwrap() {
                if collected >= k {
                    break;
                }
                let (data, coeff) = source.get_piece(&cid, seg, &pid).unwrap();
                store_client.store_piece(&cid, seg, &pid, &data, &coeff).unwrap();
                collected += 1;
            }
        }

        assert!(collected >= k, "segment {}: need {} pieces, got {}", seg, k, collected);
    }

    // Reconstruct
    let client_fetch = DataCraftClient::new(&dir_client).unwrap();
    let output = dir_client.join("output.bin");
    client_fetch.reconstruct(&cid, &output, None).unwrap();

    let decoded = std::fs::read(&output).unwrap();
    assert_eq!(decoded, content, "decoded content must match original");

    // Verify SHA-256(decoded) == CID
    let decoded_cid = ContentId::from_bytes(&decoded);
    assert_eq!(decoded_cid, cid, "SHA-256(decoded) must equal CID");

    for d in [&dir_pub, &dir_s1, &dir_s2, &dir_client] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ===========================================================================
// Test 4: Publisher Partial Push Retry
// ===========================================================================

#[test]
fn test_partial_push_retry() {
    let dir_pub = temp_dir("retry-pub");
    let dir_storage = temp_dir("retry-storage");

    // Publish
    let mut client = DataCraftClient::new(&dir_pub).unwrap();
    let content = random_data(500_000);
    let file_path = dir_pub.join("retry_test.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let segment_count = result.segment_count;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_storage = FsStore::new(&dir_storage).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();
    store_storage.store_manifest(&manifest).unwrap();

    let total_pieces = total_piece_count(&store_pub, &cid, segment_count);

    // Push only ~50% of pieces (simulating partial push before "failure")
    let mut pushed = 0;
    let target = total_pieces / 2;
    'outer: for seg in 0..segment_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for pid in &pieces {
            if pushed >= target {
                break 'outer;
            }
            let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
            store_storage.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            pushed += 1;
        }
    }

    // Simulate "failure" — storage node becomes unreachable.
    // Since not all pieces were pushed, initial_push_done should NOT be marked.
    let storage_received = total_piece_count(&store_storage, &cid, segment_count);
    assert!(storage_received < total_pieces, "only partial push occurred");
    assert!(storage_received > 0, "some pieces were pushed");

    // Publisher retains ALL pieces (never deleted them since push wasn't complete)
    let pub_remaining = total_piece_count(&store_pub, &cid, segment_count);
    assert_eq!(
        pub_remaining, total_pieces,
        "publisher must retain all pieces when push incomplete"
    );

    // Retry: push remaining pieces (simulating next cycle)
    for seg in 0..segment_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for pid in &pieces {
            if !store_storage.has_piece(&cid, seg, pid) {
                let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
                store_storage.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            }
        }
    }

    // Now all pieces are on storage
    let after_retry = total_piece_count(&store_storage, &cid, segment_count);
    assert_eq!(after_retry, total_pieces, "all pieces should be on storage after retry");

    // Publisher can now safely delete (marking initial_push_done)
    for seg in 0..segment_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for pid in &pieces {
            store_pub.delete_piece(&cid, seg, pid).unwrap();
        }
    }
    assert_eq!(total_piece_count(&store_pub, &cid, segment_count), 0);

    for d in [&dir_pub, &dir_storage] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ===========================================================================
// Test 5: Piece Uniqueness
// ===========================================================================

#[test]
fn test_piece_uniqueness_after_distribution() {
    let dir_pub = temp_dir("uniq-pub");
    let dir_s1 = temp_dir("uniq-s1");
    let dir_s2 = temp_dir("uniq-s2");
    let dir_s3 = temp_dir("uniq-s3");

    // Publish
    let mut client = DataCraftClient::new(&dir_pub).unwrap();
    let content = random_data(500_000);
    let file_path = dir_pub.join("uniq_test.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let segment_count = result.segment_count;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();
    let store_s3 = FsStore::new(&dir_s3).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();

    store_s1.store_manifest(&manifest).unwrap();
    store_s2.store_manifest(&manifest).unwrap();
    store_s3.store_manifest(&manifest).unwrap();

    // Round-robin distribute to s1 and s2
    for seg in 0..segment_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for (i, pid) in pieces.iter().enumerate() {
            let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
            if i % 2 == 0 {
                store_s1.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            } else {
                store_s2.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            }
        }
    }

    // Equalize: push half from nodes with >2 pieces to s3
    for seg in 0..segment_count as u32 {
        for source in [&store_s1, &store_s2] {
            let pieces = source.list_pieces(&cid, seg).unwrap();
            if pieces.len() > 2 {
                let to_push = pieces.len() / 2;
                for pid in pieces.iter().take(to_push) {
                    let (data, coeff) = source.get_piece(&cid, seg, pid).unwrap();
                    store_s3.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
                    source.delete_piece(&cid, seg, pid).unwrap();
                }
            }
        }
    }

    // Verify: no piece exists on more than one node
    for seg in 0..segment_count as u32 {
        let s1_pids = store_s1.list_pieces(&cid, seg).unwrap();
        let s2_pids = store_s2.list_pieces(&cid, seg).unwrap();
        let s3_pids = store_s3.list_pieces(&cid, seg).unwrap();

        let s1_set: HashSet<_> = s1_pids.iter().collect();
        let s2_set: HashSet<_> = s2_pids.iter().collect();
        let s3_set: HashSet<_> = s3_pids.iter().collect();

        // No overlaps between any pair
        assert!(
            s1_set.is_disjoint(&s2_set),
            "segment {}: s1 and s2 share pieces", seg
        );
        assert!(
            s1_set.is_disjoint(&s3_set),
            "segment {}: s1 and s3 share pieces", seg
        );
        assert!(
            s2_set.is_disjoint(&s3_set),
            "segment {}: s2 and s3 share pieces", seg
        );

        // Each piece_id appears exactly once across all nodes
        let total = s1_pids.len() + s2_pids.len() + s3_pids.len();
        let mut all: HashSet<&[u8; 32]> = HashSet::new();
        all.extend(s1_pids.iter());
        all.extend(s2_pids.iter());
        all.extend(s3_pids.iter());
        assert_eq!(all.len(), total, "segment {}: duplicate piece IDs found", seg);
    }

    for d in [&dir_pub, &dir_s1, &dir_s2, &dir_s3] {
        std::fs::remove_dir_all(d).ok();
    }
}
