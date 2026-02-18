//! E2E integration tests for CraftObj's core content flow.
//!
//! Tests the full lifecycle: publish → distribute → equalize → fetch,
//! plus edge cases, error paths, encryption, integrity, and GC.

use std::collections::HashSet;
use std::path::PathBuf;

use craftobj_client::CraftObjClient;
use craftobj_core::{ContentId, PublishOptions};
use craftobj_store::{piece_id_from_coefficients, FsStore, GarbageCollector, PinManager};
use rand::RngCore;

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("craftobj-e2e-flow")
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

/// Count total pieces across all segments for a content in a store.
fn total_piece_count(store: &FsStore, cid: &ContentId, segment_count: usize) -> usize {
    (0..segment_count as u32)
        .map(|seg| store.list_pieces(cid, seg).unwrap().len())
        .sum()
}

// ===================================================================
// DISTRIBUTION SCENARIOS
// ===================================================================

#[test]
fn publish_and_distribute_round_robin() {
    let dir_pub = temp_dir("dist-pub");
    let dir_s1 = temp_dir("dist-s1");
    let dir_s2 = temp_dir("dist-s2");

    let mut client = CraftObjClient::new(&dir_pub).unwrap();
    let content = random_data(1_000_000);
    let file_path = dir_pub.join("testfile.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();

    let total_encoded = total_piece_count(&store_pub, &cid, seg_count);
    assert!(total_encoded > 0);

    store_s1.store_manifest(&manifest).unwrap();
    store_s2.store_manifest(&manifest).unwrap();

    // Round-robin push ALL pieces, then delete from publisher
    let stores = [&store_s1, &store_s2];
    for seg in 0..seg_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for (i, pid) in pieces.iter().enumerate() {
            let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
            stores[i % 2].store_piece(&cid, seg, pid, &data, &coeff).unwrap();
        }
        for pid in &pieces {
            store_pub.delete_piece(&cid, seg, pid).unwrap();
        }
    }

    assert_eq!(total_piece_count(&store_pub, &cid, seg_count), 0);
    let s1 = total_piece_count(&store_s1, &cid, seg_count);
    let s2 = total_piece_count(&store_s2, &cid, seg_count);
    assert!(s1 > 0);
    assert!(s2 > 0);
    assert_eq!(s1 + s2, total_encoded);

    for d in [&dir_pub, &dir_s1, &dir_s2] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn push_with_mid_transfer_failure() {
    let dir_pub = temp_dir("midfail-pub");
    let dir_s1 = temp_dir("midfail-s1");
    let dir_s2 = temp_dir("midfail-s2");

    let mut client = CraftObjClient::new(&dir_pub).unwrap();
    let content = random_data(2_000_000);
    let file_path = dir_pub.join("test.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();
    store_s1.store_manifest(&manifest).unwrap();
    store_s2.store_manifest(&manifest).unwrap();

    // Push all pieces to s1. Only push first 2 to s2 (simulating failure after 2).
    let total_pieces = total_piece_count(&store_pub, &cid, seg_count);
    let mut s2_received = 0;
    let s2_limit = 2;
    for seg in 0..seg_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for pid in &pieces {
            let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
            store_s1.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            if s2_received < s2_limit {
                store_s2.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
                s2_received += 1;
            }
        }
    }

    // Publisher still has all pieces (hasn't deleted since s2 push was incomplete)
    let pub_total = total_piece_count(&store_pub, &cid, seg_count);
    assert_eq!(pub_total, total_pieces, "publisher retains all pieces on partial failure");

    // s1 got everything, s2 got partial
    let s1_total = total_piece_count(&store_s1, &cid, seg_count);
    let s2_total = total_piece_count(&store_s2, &cid, seg_count);
    assert_eq!(s1_total, total_pieces);
    assert_eq!(s2_total, s2_limit.min(total_pieces));
    assert!(s2_total < s1_total, "s2 should have fewer due to failure");

    for d in [&dir_pub, &dir_s1, &dir_s2] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn push_with_single_storage_node() {
    let dir_pub = temp_dir("single-pub");
    let dir_s1 = temp_dir("single-s1");

    let mut client = CraftObjClient::new(&dir_pub).unwrap();
    let content = random_data(300_000);
    let file_path = dir_pub.join("test.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();
    store_s1.store_manifest(&manifest).unwrap();

    let total_encoded = total_piece_count(&store_pub, &cid, seg_count);

    // Push ALL pieces to single storage node
    for seg in 0..seg_count as u32 {
        for pid in store_pub.list_pieces(&cid, seg).unwrap() {
            let (data, coeff) = store_pub.get_piece(&cid, seg, &pid).unwrap();
            store_s1.store_piece(&cid, seg, &pid, &data, &coeff).unwrap();
        }
        for pid in store_pub.list_pieces(&cid, seg).unwrap() {
            store_pub.delete_piece(&cid, seg, &pid).unwrap();
        }
    }

    assert_eq!(total_piece_count(&store_pub, &cid, seg_count), 0);
    assert_eq!(total_piece_count(&store_s1, &cid, seg_count), total_encoded);

    // Can still reconstruct from single node
    let client_s1 = CraftObjClient::new(&dir_s1).unwrap();
    let output = dir_s1.join("output.bin");
    client_s1.reconstruct(&cid, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    for d in [&dir_pub, &dir_s1] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn multiple_publishers_different_content() {
    let dir_a = temp_dir("multipub-a");
    let dir_b = temp_dir("multipub-b");
    let dir_store = temp_dir("multipub-store");

    let mut client_a = CraftObjClient::new(&dir_a).unwrap();
    let mut client_b = CraftObjClient::new(&dir_b).unwrap();

    let content_a = random_data(200_000);
    let content_b = random_data(200_000);
    let file_a = dir_a.join("a.bin");
    let file_b = dir_b.join("b.bin");
    std::fs::write(&file_a, &content_a).unwrap();
    std::fs::write(&file_b, &content_b).unwrap();

    let result_a = client_a.publish(&file_a, &PublishOptions::default()).unwrap();
    let result_b = client_b.publish(&file_b, &PublishOptions::default()).unwrap();

    // Different content → different CIDs
    assert_ne!(result_a.content_id, result_b.content_id);

    // Both can push to same storage node
    let store = FsStore::new(&dir_store).unwrap();
    let store_a = FsStore::new(&dir_a).unwrap();
    let store_b = FsStore::new(&dir_b).unwrap();

    let manifest_a = store_a.get_manifest(&result_a.content_id).unwrap();
    let manifest_b = store_b.get_manifest(&result_b.content_id).unwrap();
    store.store_manifest(&manifest_a).unwrap();
    store.store_manifest(&manifest_b).unwrap();

    for seg in 0..result_a.segment_count as u32 {
        for pid in store_a.list_pieces(&result_a.content_id, seg).unwrap() {
            let (data, coeff) = store_a.get_piece(&result_a.content_id, seg, &pid).unwrap();
            store.store_piece(&result_a.content_id, seg, &pid, &data, &coeff).unwrap();
        }
    }
    for seg in 0..result_b.segment_count as u32 {
        for pid in store_b.list_pieces(&result_b.content_id, seg).unwrap() {
            let (data, coeff) = store_b.get_piece(&result_b.content_id, seg, &pid).unwrap();
            store.store_piece(&result_b.content_id, seg, &pid, &data, &coeff).unwrap();
        }
    }

    // Storage has both contents
    let stored = store.list_content().unwrap();
    assert_eq!(stored.len(), 2);

    for d in [&dir_a, &dir_b, &dir_store] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn same_content_published_by_two_clients_dedup() {
    let dir_a = temp_dir("dedup-a");
    let dir_b = temp_dir("dedup-b");

    let content = random_data(100_000);

    let mut client_a = CraftObjClient::new(&dir_a).unwrap();
    let mut client_b = CraftObjClient::new(&dir_b).unwrap();

    let file_a = dir_a.join("same.bin");
    let file_b = dir_b.join("same.bin");
    std::fs::write(&file_a, &content).unwrap();
    std::fs::write(&file_b, &content).unwrap();

    let result_a = client_a.publish(&file_a, &PublishOptions::default()).unwrap();
    let result_b = client_b.publish(&file_b, &PublishOptions::default()).unwrap();

    // Same content → same CID (content-addressed dedup)
    assert_eq!(result_a.content_id, result_b.content_id);
    assert_eq!(result_a.total_size, result_b.total_size);
    assert_eq!(result_a.segment_count, result_b.segment_count);

    for d in [&dir_a, &dir_b] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn storage_node_at_capacity_refuses_via_gc() {
    // Simulate capacity by checking disk usage before accepting
    let dir_pub = temp_dir("cap-pub");
    let dir_store = temp_dir("cap-store");

    let mut client = CraftObjClient::new(&dir_pub).unwrap();
    let content = random_data(200_000);
    let file_path = dir_pub.join("test.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_node = FsStore::new(&dir_store).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();
    store_node.store_manifest(&manifest).unwrap();

    // Push some pieces
    let seg0_pieces = store_pub.list_pieces(&cid, 0).unwrap();
    let (data, coeff) = store_pub.get_piece(&cid, 0, &seg0_pieces[0]).unwrap();
    store_node.store_piece(&cid, 0, &seg0_pieces[0], &data, &coeff).unwrap();

    let usage = store_node.disk_usage().unwrap();
    assert!(usage > 0);

    // Set a very low capacity threshold — GC should remove unpinned content
    let pm = PinManager::new(&dir_store).unwrap();
    // Content is not pinned on the storage node
    assert!(!pm.is_pinned(&cid));

    let removed = GarbageCollector::sweep(&store_node, &pm, 1).unwrap(); // max 1 byte
    assert_eq!(removed, 1, "GC should remove unpinned content over capacity");

    // Content should be gone
    assert!(store_node.get_manifest(&cid).is_err());

    for d in [&dir_pub, &dir_store] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ===================================================================
// FILE SIZE EDGE CASES
// ===================================================================

#[test]
fn tiny_file_less_than_one_piece() {
    let dir = temp_dir("tiny");
    let mut client = CraftObjClient::new(&dir).unwrap();

    // 50 bytes — way smaller than 100KB piece size, so k=1
    let content = b"Hello, this is a tiny file for edge case testing!";
    let file_path = dir.join("tiny.txt");
    std::fs::write(&file_path, content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    assert_eq!(result.segment_count, 1);
    assert_eq!(result.total_size, content.len() as u64);

    let manifest = client.store().get_manifest(&result.content_id).unwrap();
    let k = manifest.k_for_segment(0);
    assert_eq!(k, 1, "tiny file should have k=1");

    // Can reconstruct from a single piece
    let output = dir.join("output.txt");
    client.reconstruct(&result.content_id, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn exact_one_segment_file() {
    let dir = temp_dir("exact-seg");
    let mut client = CraftObjClient::new(&dir).unwrap();

    // 10MB = segment_size → exactly 1 segment, k = 10MB/100KB = 100
    let content = random_data(10_485_760);
    let file_path = dir.join("exact.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    assert_eq!(result.segment_count, 1);

    let manifest = client.store().get_manifest(&result.content_id).unwrap();
    let k = manifest.k_for_segment(0);
    assert_eq!(k, 40, "full segment should have k=40 (10MB/256KiB)");

    let output = dir.join("output.bin");
    client.reconstruct(&result.content_id, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn multi_segment_with_partial_last() {
    let dir = temp_dir("multi-seg");
    let mut client = CraftObjClient::new(&dir).unwrap();

    // 15MB → 1 full segment (10MB) + 1 partial (5MB)
    let content = random_data(15_000_000);
    let file_path = dir.join("multi.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    assert_eq!(result.segment_count, 2);

    let manifest = client.store().get_manifest(&result.content_id).unwrap();
    let k0 = manifest.k_for_segment(0);
    let k1 = manifest.k_for_segment(1);
    assert_eq!(k0, 40, "full segment k=40 (10MB/256KiB)");
    assert!(k1 < 40 && k1 > 0, "partial segment k={} should be < 40", k1);

    let output = dir.join("output.bin");
    client.reconstruct(&result.content_id, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn empty_file_rejected() {
    let dir = temp_dir("empty");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let result = client.publish_bytes(b"", &PublishOptions::default());
    assert!(result.is_err(), "empty data should be rejected");

    std::fs::remove_dir_all(&dir).ok();
}

// ===================================================================
// EQUALIZATION SCENARIOS
// ===================================================================

/// Helper: publish and distribute pieces evenly across given stores.
fn publish_and_distribute(
    content_size: usize,
    stores: &[&FsStore],
) -> (ContentId, usize, Vec<u8>) {
    let dir_pub = temp_dir("eq-helper-pub");
    let mut client = CraftObjClient::new(&dir_pub).unwrap();
    let content = random_data(content_size);
    let file_path = dir_pub.join("test.bin");
    std::fs::write(&file_path, &content).unwrap();

    let result = client.publish(&file_path, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;
    let store_pub = FsStore::new(&dir_pub).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();

    for s in stores {
        s.store_manifest(&manifest).unwrap();
    }

    for seg in 0..seg_count as u32 {
        let pieces = store_pub.list_pieces(&cid, seg).unwrap();
        for (i, pid) in pieces.iter().enumerate() {
            let (data, coeff) = store_pub.get_piece(&cid, seg, pid).unwrap();
            stores[i % stores.len()].store_piece(&cid, seg, pid, &data, &coeff).unwrap();
        }
    }

    std::fs::remove_dir_all(&dir_pub).ok();
    (cid, seg_count, content)
}

#[test]
fn equalization_one_non_provider() {
    let dir_s1 = temp_dir("eq1-s1");
    let dir_s2 = temp_dir("eq1-s2");
    let dir_s3 = temp_dir("eq1-s3");

    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();
    let store_s3 = FsStore::new(&dir_s3).unwrap();

    let (cid, seg_count, _content) = publish_and_distribute(2_000_000, &[&store_s1, &store_s2]);

    // s3 is non-provider
    let manifest = store_s1.get_manifest(&cid).unwrap();
    store_s3.store_manifest(&manifest).unwrap();
    assert_eq!(total_piece_count(&store_s3, &cid, seg_count), 0);

    let total_before = total_piece_count(&store_s1, &cid, seg_count)
        + total_piece_count(&store_s2, &cid, seg_count);

    // Equalize: push half from >2 to s3
    for seg in 0..seg_count as u32 {
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

    let s3_count = total_piece_count(&store_s3, &cid, seg_count);
    assert!(s3_count >= 2, "non-provider should have ≥2 pieces, got {}", s3_count);

    let total_after = total_piece_count(&store_s1, &cid, seg_count)
        + total_piece_count(&store_s2, &cid, seg_count)
        + s3_count;
    assert_eq!(total_before, total_after, "total pieces conserved");

    for d in [&dir_s1, &dir_s2, &dir_s3] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn equalization_three_non_providers() {
    let dir_s1 = temp_dir("eq3-s1");
    let dir_n1 = temp_dir("eq3-n1");
    let dir_n2 = temp_dir("eq3-n2");
    let dir_n3 = temp_dir("eq3-n3");

    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_n1 = FsStore::new(&dir_n1).unwrap();
    let store_n2 = FsStore::new(&dir_n2).unwrap();
    let store_n3 = FsStore::new(&dir_n3).unwrap();

    // All pieces on s1
    let (cid, seg_count, _) = publish_and_distribute(2_000_000, &[&store_s1]);

    let manifest = store_s1.get_manifest(&cid).unwrap();
    store_n1.store_manifest(&manifest).unwrap();
    store_n2.store_manifest(&manifest).unwrap();
    store_n3.store_manifest(&manifest).unwrap();

    let total_before = total_piece_count(&store_s1, &cid, seg_count);

    // Equalize: s1 distributes to non-providers round-robin
    let non_providers = [&store_n1, &store_n2, &store_n3];
    for seg in 0..seg_count as u32 {
        let pieces = store_s1.list_pieces(&cid, seg).unwrap();
        // Keep some for self, distribute rest
        let keep = (pieces.len() / 4).max(1);
        let to_distribute = &pieces[keep..];
        for (i, pid) in to_distribute.iter().enumerate() {
            let (data, coeff) = store_s1.get_piece(&cid, seg, pid).unwrap();
            non_providers[i % 3].store_piece(&cid, seg, pid, &data, &coeff).unwrap();
            store_s1.delete_piece(&cid, seg, pid).unwrap();
        }
    }

    let n1 = total_piece_count(&store_n1, &cid, seg_count);
    let n2 = total_piece_count(&store_n2, &cid, seg_count);
    let n3 = total_piece_count(&store_n3, &cid, seg_count);
    assert!(n1 > 0, "n1 should have pieces");
    assert!(n2 > 0, "n2 should have pieces");
    assert!(n3 > 0, "n3 should have pieces");

    let total_after = total_piece_count(&store_s1, &cid, seg_count) + n1 + n2 + n3;
    assert_eq!(total_before, total_after, "total conserved");

    for d in [&dir_s1, &dir_n1, &dir_n2, &dir_n3] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn equalization_stops_when_all_are_providers() {
    let dir_s1 = temp_dir("eqstop-s1");
    let dir_s2 = temp_dir("eqstop-s2");

    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();

    let (cid, seg_count, _) = publish_and_distribute(2_000_000, &[&store_s1, &store_s2]);

    // Both already have pieces — both are providers
    let s1_before = total_piece_count(&store_s1, &cid, seg_count);
    let s2_before = total_piece_count(&store_s2, &cid, seg_count);
    assert!(s1_before > 0);
    assert!(s2_before > 0);

    // Run equalization logic: nothing should happen since both are providers
    // (no non-providers to push to)
    let mut pushed = 0;
    for seg in 0..seg_count as u32 {
        // Check: all known nodes have pieces → skip equalization
        let s1_has = !store_s1.list_pieces(&cid, seg).unwrap().is_empty();
        let s2_has = !store_s2.list_pieces(&cid, seg).unwrap().is_empty();
        if s1_has && s2_has {
            // All providers, no equalization needed
            continue;
        }
        pushed += 1;
    }
    assert_eq!(pushed, 0, "no equalization should happen when all are providers");

    // Counts unchanged
    assert_eq!(total_piece_count(&store_s1, &cid, seg_count), s1_before);
    assert_eq!(total_piece_count(&store_s2, &cid, seg_count), s2_before);

    for d in [&dir_s1, &dir_s2] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn equalization_multiple_rounds_converge() {
    let dir_s1 = temp_dir("eqconv-s1");
    let dir_s2 = temp_dir("eqconv-s2");

    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();

    // All pieces on s1
    let (cid, seg_count, _) = publish_and_distribute(2_000_000, &[&store_s1]);
    let manifest = store_s1.get_manifest(&cid).unwrap();
    store_s2.store_manifest(&manifest).unwrap();

    let total = total_piece_count(&store_s1, &cid, seg_count);

    // Run multiple equalization rounds (push half each time)
    for _round in 0..5 {
        for seg in 0..seg_count as u32 {
            let pieces = store_s1.list_pieces(&cid, seg).unwrap();
            if pieces.len() > 2 {
                let to_push = pieces.len() / 2;
                for pid in pieces.iter().take(to_push) {
                    let (data, coeff) = store_s1.get_piece(&cid, seg, pid).unwrap();
                    store_s2.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
                    store_s1.delete_piece(&cid, seg, pid).unwrap();
                }
            }
        }
    }

    // Should converge: s1 has ~few, s2 has rest
    let s1_final = total_piece_count(&store_s1, &cid, seg_count);
    let s2_final = total_piece_count(&store_s2, &cid, seg_count);
    assert_eq!(s1_final + s2_final, total, "total conserved after rounds");
    // After enough rounds of halving, s1 should have ≤2 per segment
    for seg in 0..seg_count as u32 {
        let count = store_s1.list_pieces(&cid, seg).unwrap().len();
        assert!(count <= 2, "segment {}: s1 should have ≤2 pieces, got {}", seg, count);
    }

    for d in [&dir_s1, &dir_s2] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn piece_uniqueness_after_equalization() {
    let dir_s1 = temp_dir("uniq-s1");
    let dir_s2 = temp_dir("uniq-s2");
    let dir_s3 = temp_dir("uniq-s3");

    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();
    let store_s3 = FsStore::new(&dir_s3).unwrap();

    let (cid, seg_count, _) = publish_and_distribute(2_000_000, &[&store_s1, &store_s2]);
    let manifest = store_s1.get_manifest(&cid).unwrap();
    store_s3.store_manifest(&manifest).unwrap();

    // Equalize to s3
    for seg in 0..seg_count as u32 {
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

    // Verify no piece on >1 node
    for seg in 0..seg_count as u32 {
        let s1_set: HashSet<_> = store_s1.list_pieces(&cid, seg).unwrap().into_iter().collect();
        let s2_set: HashSet<_> = store_s2.list_pieces(&cid, seg).unwrap().into_iter().collect();
        let s3_set: HashSet<_> = store_s3.list_pieces(&cid, seg).unwrap().into_iter().collect();

        assert!(s1_set.is_disjoint(&s2_set), "seg {}: s1∩s2 overlap", seg);
        assert!(s1_set.is_disjoint(&s3_set), "seg {}: s1∩s3 overlap", seg);
        assert!(s2_set.is_disjoint(&s3_set), "seg {}: s2∩s3 overlap", seg);
    }

    for d in [&dir_s1, &dir_s2, &dir_s3] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ===================================================================
// FETCH SCENARIOS
// ===================================================================

#[test]
fn fetch_from_distributed_network() {
    let dir_s1 = temp_dir("fetch-s1");
    let dir_s2 = temp_dir("fetch-s2");
    let dir_client = temp_dir("fetch-client");

    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();

    let (cid, seg_count, content) = publish_and_distribute(2_000_000, &[&store_s1, &store_s2]);

    // Client fetches k pieces per segment
    let store_client = FsStore::new(&dir_client).unwrap();
    let manifest = store_s1.get_manifest(&cid).unwrap();
    store_client.store_manifest(&manifest).unwrap();

    for seg in 0..seg_count as u32 {
        let k = manifest.k_for_segment(seg as usize);
        let mut collected = 0;
        for source in [&store_s1, &store_s2] {
            if collected >= k { break; }
            for pid in source.list_pieces(&cid, seg).unwrap() {
                if collected >= k { break; }
                let (data, coeff) = source.get_piece(&cid, seg, &pid).unwrap();
                store_client.store_piece(&cid, seg, &pid, &data, &coeff).unwrap();
                collected += 1;
            }
        }
        assert!(collected >= k);
    }

    let client = CraftObjClient::new(&dir_client).unwrap();
    let output = dir_client.join("output.bin");
    client.reconstruct(&cid, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    // SHA-256(decoded) == CID
    assert_eq!(ContentId::from_bytes(&content), cid);

    for d in [&dir_s1, &dir_s2, &dir_client] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn fetch_with_some_providers_offline() {
    let dir_s1 = temp_dir("offline-s1");
    let dir_s2 = temp_dir("offline-s2");
    let dir_client = temp_dir("offline-client");

    let store_s1 = FsStore::new(&dir_s1).unwrap();
    let store_s2 = FsStore::new(&dir_s2).unwrap();

    let (cid, seg_count, content) = publish_and_distribute(200_000, &[&store_s1, &store_s2]);

    // s2 is "offline" — client can only reach s1
    // s1 should have enough pieces (at least k due to parity) to reconstruct
    let manifest = store_s1.get_manifest(&cid).unwrap();
    let store_client = FsStore::new(&dir_client).unwrap();
    store_client.store_manifest(&manifest).unwrap();

    let mut can_reconstruct = true;
    for seg in 0..seg_count as u32 {
        let k = manifest.k_for_segment(seg as usize);
        let available = store_s1.list_pieces(&cid, seg).unwrap();
        if available.len() < k {
            can_reconstruct = false;
            break;
        }
        for pid in available.iter().take(k) {
            let (data, coeff) = store_s1.get_piece(&cid, seg, pid).unwrap();
            store_client.store_piece(&cid, seg, pid, &data, &coeff).unwrap();
        }
    }

    if can_reconstruct {
        let client = CraftObjClient::new(&dir_client).unwrap();
        let output = dir_client.join("output.bin");
        client.reconstruct(&cid, &output, None).unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), content);
    } else {
        // If s1 doesn't have enough for any segment, that's expected with 2-node split
        // The point: we don't crash, we handle gracefully
        let client = CraftObjClient::new(&dir_client).unwrap();
        let output = dir_client.join("output.bin");
        assert!(client.reconstruct(&cid, &output, None).is_err());
    }

    for d in [&dir_s1, &dir_s2, &dir_client] {
        std::fs::remove_dir_all(d).ok();
    }
}

#[test]
fn fetch_tiny_file_k1() {
    let dir = temp_dir("fetch-tiny");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = b"tiny content";
    let file = dir.join("tiny.txt");
    std::fs::write(&file, content).unwrap();

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    let manifest = client.store().get_manifest(&result.content_id).unwrap();
    assert_eq!(manifest.k_for_segment(0), 1);

    let output = dir.join("output.txt");
    client.reconstruct(&result.content_id, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content.as_slice());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn fetch_multi_segment() {
    let dir = temp_dir("fetch-multi");
    let mut client = CraftObjClient::new(&dir).unwrap();

    // 15MB → 2 segments (10+5)
    let content = random_data(15_000_000);
    let file = dir.join("big.bin");
    std::fs::write(&file, &content).unwrap();

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    assert!(result.segment_count >= 2, "expected ≥2 segments, got {}", result.segment_count);

    let output = dir.join("output.bin");
    client.reconstruct(&result.content_id, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn fetch_with_redundant_pieces_discards_extras() {
    let dir = temp_dir("fetch-redundant");
    let mut client = CraftObjClient::new(&dir).unwrap();

    // Use 5MB so k=20 (256KiB pieces), parity 1.2 → 24 pieces total > k
    let content = random_data(5_000_000);
    let file = dir.join("test.bin");
    std::fs::write(&file, &content).unwrap();

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    let cid = result.content_id;

    // Verify we have more pieces than k (due to parity)
    let manifest = client.store().get_manifest(&cid).unwrap();
    for seg in 0..result.segment_count as u32 {
        let k = manifest.k_for_segment(seg as usize);
        let total = client.store().list_pieces(&cid, seg).unwrap().len();
        assert!(total > k, "seg {}: total={} should be > k={} (parity pieces)", seg, total, k);
    }

    // reconstruct() should succeed using only k, ignoring extras
    let output = dir.join("output.bin");
    client.reconstruct(&cid, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    std::fs::remove_dir_all(&dir).ok();
}

// ===================================================================
// ENCRYPTED CONTENT
// ===================================================================

#[test]
fn publish_encrypted_cid_is_hash_of_ciphertext() {
    let dir = temp_dir("enc-cid");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = b"secret data for encrypted CID test";
    let file = dir.join("secret.txt");
    std::fs::write(&file, content).unwrap();

    let result = client.publish(&file, &PublishOptions { encrypted: true }).unwrap();
    assert!(result.encryption_key.is_some());

    // CID should NOT be hash of plaintext
    let plaintext_cid = ContentId::from_bytes(content);
    assert_ne!(result.content_id, plaintext_cid);

    // Reconstruct ciphertext (without decryption key) and verify CID = SHA-256(ciphertext)
    let output_raw = dir.join("raw.bin");
    client.reconstruct(&result.content_id, &output_raw, None).unwrap();
    let ciphertext = std::fs::read(&output_raw).unwrap();
    let ciphertext_cid = ContentId::from_bytes(&ciphertext);
    assert_eq!(result.content_id, ciphertext_cid, "CID must equal SHA-256(ciphertext)");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn publish_encrypted_fetch_decrypt_roundtrip() {
    let dir = temp_dir("enc-rt");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = random_data(200_000);
    let file = dir.join("secret.bin");
    std::fs::write(&file, &content).unwrap();

    let result = client.publish(&file, &PublishOptions { encrypted: true }).unwrap();
    let key = result.encryption_key.as_ref().unwrap();

    let output = dir.join("decrypted.bin");
    client.reconstruct(&result.content_id, &output, Some(key)).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    // Wrong key fails
    let wrong_key = random_data(32);
    let output_wrong = dir.join("wrong.bin");
    assert!(client.reconstruct(&result.content_id, &output_wrong, Some(&wrong_key)).is_err());

    std::fs::remove_dir_all(&dir).ok();
}

// ===================================================================
// LIFECYCLE: PIN / UNPIN / GC / DELETE
// ===================================================================

#[test]
fn unpin_gc_sweep_removes_content() {
    let dir = temp_dir("gc");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = random_data(100_000);
    let file = dir.join("test.bin");
    std::fs::write(&file, &content).unwrap();

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    let cid = result.content_id;

    // Published content is auto-pinned
    assert!(client.is_pinned(&cid));

    // Unpin
    client.unpin(&cid).unwrap();
    assert!(!client.is_pinned(&cid));

    // GC sweep with very low threshold → should remove
    let removed = GarbageCollector::sweep(client.store(), client.pin_manager(), 1).unwrap();
    assert_eq!(removed, 1);

    // Content gone
    assert!(client.store().get_manifest(&cid).is_err());
    assert_eq!(client.store().list_pieces(&cid, 0).unwrap().len(), 0);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn gc_does_not_remove_pinned_content() {
    let dir = temp_dir("gc-pinned");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = random_data(100_000);
    let file = dir.join("test.bin");
    std::fs::write(&file, &content).unwrap();

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    let cid = result.content_id;

    assert!(client.is_pinned(&cid));

    // GC with low threshold — pinned content should survive
    let removed = GarbageCollector::sweep(client.store(), client.pin_manager(), 1).unwrap();
    assert_eq!(removed, 0, "pinned content must not be GC'd");

    // Content still exists
    assert!(client.store().get_manifest(&cid).is_ok());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn delete_content_removes_pieces_and_manifest() {
    let dir = temp_dir("delete");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = random_data(200_000);
    let file = dir.join("test.bin");
    std::fs::write(&file, &content).unwrap();

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    let cid = result.content_id;

    // Verify pieces exist
    let pieces_before = total_piece_count(client.store(), &cid, result.segment_count);
    assert!(pieces_before > 0);

    // Delete
    client.store().delete_content(&cid).unwrap();

    // Pieces and manifest gone
    assert_eq!(total_piece_count(client.store(), &cid, result.segment_count), 0);
    assert!(client.store().get_manifest(&cid).is_err());

    std::fs::remove_dir_all(&dir).ok();
}

// ===================================================================
// INTEGRITY
// ===================================================================

#[test]
fn corrupt_piece_detected_by_verify_integrity() {
    let dir = temp_dir("corrupt");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = random_data(200_000);
    let file = dir.join("test.bin");
    std::fs::write(&file, &content).unwrap();

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    let cid = result.content_id;

    // Corrupt a piece's coefficient file on disk (piece_id won't match SHA-256(coeff))
    let pieces = client.store().list_pieces(&cid, 0).unwrap();
    assert!(!pieces.is_empty());
    let target_pid = pieces[0];
    let pid_hex = hex::encode(target_pid);
    let coeff_path = dir
        .join("pieces")
        .join(cid.to_hex())
        .join("0")
        .join(format!("{pid_hex}.coeff"));
    assert!(coeff_path.exists());

    // Write garbage to coefficient file
    let mut coeff_data = std::fs::read(&coeff_path).unwrap();
    coeff_data[0] ^= 0xFF; // flip a byte
    std::fs::write(&coeff_path, &coeff_data).unwrap();

    // verify_integrity should detect and remove the corrupt piece
    let store = FsStore::new(&dir).unwrap();
    let removed = store.verify_integrity().unwrap();
    assert!(removed >= 1, "should detect corrupt piece, removed={}", removed);

    // The corrupt piece should be gone
    assert!(!store.has_piece(&cid, 0, &target_pid));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn piece_id_mismatch_detected() {
    let dir = temp_dir("pid-mismatch");
    let store = FsStore::new(&dir).unwrap();

    let cid = ContentId([42u8; 32]);
    let real_coeff = vec![1u8, 2, 3, 4];
    let correct_pid = piece_id_from_coefficients(&real_coeff);

    // Store with correct pid
    store.store_piece(&cid, 0, &correct_pid, b"data", &real_coeff).unwrap();
    assert!(store.has_piece(&cid, 0, &correct_pid));

    // Store with WRONG pid (doesn't match coefficient hash)
    let wrong_pid = [0xFFu8; 32];
    store.store_piece(&cid, 0, &wrong_pid, b"data", &real_coeff).unwrap();

    // verify_integrity detects the mismatched one
    let removed = store.verify_integrity().unwrap();
    assert_eq!(removed, 1, "should remove piece with wrong pid");

    // Correct piece still exists
    assert!(store.has_piece(&cid, 0, &correct_pid));
    // Wrong one removed
    assert!(!store.has_piece(&cid, 0, &wrong_pid));

    std::fs::remove_dir_all(&dir).ok();
}

// ===================================================================
// PARTIAL PUSH RETRY
// ===================================================================

#[test]
fn partial_push_retry_full_scenario() {
    let dir_pub = temp_dir("retry-pub");
    let dir_storage = temp_dir("retry-storage");

    let mut client = CraftObjClient::new(&dir_pub).unwrap();
    let content = random_data(500_000);
    let file = dir_pub.join("test.bin");
    std::fs::write(&file, &content).unwrap();

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    let cid = result.content_id;
    let seg_count = result.segment_count;

    let store_pub = FsStore::new(&dir_pub).unwrap();
    let store_storage = FsStore::new(&dir_storage).unwrap();
    let manifest = store_pub.get_manifest(&cid).unwrap();
    store_storage.store_manifest(&manifest).unwrap();

    let total_pieces = total_piece_count(&store_pub, &cid, seg_count);

    // Push only ~50%
    let target = total_pieces / 2;
    let mut pushed = 0;
    'outer: for seg in 0..seg_count as u32 {
        for pid in store_pub.list_pieces(&cid, seg).unwrap() {
            if pushed >= target { break 'outer; }
            let (data, coeff) = store_pub.get_piece(&cid, seg, &pid).unwrap();
            store_storage.store_piece(&cid, seg, &pid, &data, &coeff).unwrap();
            pushed += 1;
        }
    }

    // Publisher retains ALL (didn't delete since incomplete)
    assert_eq!(total_piece_count(&store_pub, &cid, seg_count), total_pieces);
    let storage_partial = total_piece_count(&store_storage, &cid, seg_count);
    assert!(storage_partial > 0 && storage_partial < total_pieces);

    // Retry: push remaining
    for seg in 0..seg_count as u32 {
        for pid in store_pub.list_pieces(&cid, seg).unwrap() {
            if !store_storage.has_piece(&cid, seg, &pid) {
                let (data, coeff) = store_pub.get_piece(&cid, seg, &pid).unwrap();
                store_storage.store_piece(&cid, seg, &pid, &data, &coeff).unwrap();
            }
        }
    }

    assert_eq!(total_piece_count(&store_storage, &cid, seg_count), total_pieces);

    // Now publisher can safely delete
    for seg in 0..seg_count as u32 {
        for pid in store_pub.list_pieces(&cid, seg).unwrap() {
            store_pub.delete_piece(&cid, seg, &pid).unwrap();
        }
    }
    assert_eq!(total_piece_count(&store_pub, &cid, seg_count), 0);

    // Storage can reconstruct
    let client_storage = CraftObjClient::new(&dir_storage).unwrap();
    let output = dir_storage.join("output.bin");
    client_storage.reconstruct(&cid, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    for d in [&dir_pub, &dir_storage] {
        std::fs::remove_dir_all(d).ok();
    }
}

// ===================================================================
// ADDITIONAL EDGE CASES
// ===================================================================

#[test]
fn reconstruct_nonexistent_content_fails() {
    let dir = temp_dir("noexist");
    let client = CraftObjClient::new(&dir).unwrap();

    let fake_cid = ContentId([0xAB; 32]);
    let output = dir.join("output.bin");
    assert!(client.reconstruct(&fake_cid, &output, None).is_err());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn publish_same_content_twice_idempotent() {
    let dir = temp_dir("idempotent");
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = random_data(50_000);
    let file = dir.join("test.bin");
    std::fs::write(&file, &content).unwrap();

    let r1 = client.publish(&file, &PublishOptions::default()).unwrap();
    let r2 = client.publish(&file, &PublishOptions::default()).unwrap();

    assert_eq!(r1.content_id, r2.content_id);
    assert_eq!(r1.total_size, r2.total_size);

    // Should still reconstruct fine
    let output = dir.join("output.bin");
    client.reconstruct(&r1.content_id, &output, None).unwrap();
    assert_eq!(std::fs::read(&output).unwrap(), content);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn disk_usage_increases_with_content() {
    let dir = temp_dir("diskusage");
    let store = FsStore::new(&dir).unwrap();
    let before = store.disk_usage().unwrap();

    let mut client = CraftObjClient::new(&dir).unwrap();
    let content = random_data(100_000);
    let file = dir.join("test.bin");
    std::fs::write(&file, &content).unwrap();
    client.publish(&file, &PublishOptions::default()).unwrap();

    let store2 = FsStore::new(&dir).unwrap();
    let after = store2.disk_usage().unwrap();
    assert!(after > before, "disk usage should increase after publish");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn list_and_status_reflect_multiple_contents() {
    let dir = temp_dir("liststatus");
    let mut client = CraftObjClient::new(&dir).unwrap();

    for i in 0..3 {
        let content = random_data(50_000 + i * 10_000);
        let file = dir.join(format!("file_{}.bin", i));
        std::fs::write(&file, &content).unwrap();
        client.publish(&file, &PublishOptions::default()).unwrap();
    }

    let items = client.list().unwrap();
    assert_eq!(items.len(), 3);
    for item in &items {
        assert!(item.pinned);
    }

    let status = client.status().unwrap();
    assert_eq!(status.content_count, 3);
    assert_eq!(status.pinned_count, 3);
    assert!(status.piece_count > 0);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn gc_sweep_removes_only_unpinned_when_over_threshold() {
    let dir = temp_dir("gc-selective");
    let mut client = CraftObjClient::new(&dir).unwrap();

    // Publish 3 files
    let mut cids = Vec::new();
    for i in 0..3 {
        let content = random_data(50_000);
        let file = dir.join(format!("file_{}.bin", i));
        std::fs::write(&file, &content).unwrap();
        let r = client.publish(&file, &PublishOptions::default()).unwrap();
        cids.push(r.content_id);
    }

    // Unpin only the middle one
    client.unpin(&cids[1]).unwrap();
    assert!(client.is_pinned(&cids[0]));
    assert!(!client.is_pinned(&cids[1]));
    assert!(client.is_pinned(&cids[2]));

    // GC with low threshold
    let removed = GarbageCollector::sweep(client.store(), client.pin_manager(), 1).unwrap();
    assert_eq!(removed, 1, "should remove only the 1 unpinned content");

    // Pinned ones survive
    assert!(client.store().get_manifest(&cids[0]).is_ok());
    assert!(client.store().get_manifest(&cids[1]).is_err());
    assert!(client.store().get_manifest(&cids[2]).is_ok());

    std::fs::remove_dir_all(&dir).ok();
}
