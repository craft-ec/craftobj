//! E2E test: push distribution flow.
//!
//! Simulates the daemon push distribution logic:
//! Node A publishes → pushes subset of pieces (2 per node) to Node B → Node B reconstructs.

use std::path::PathBuf;

use craftobj_client::CraftObjClient;
use craftobj_core::PublishOptions;
use craftobj_store::FsStore;

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("craftobj-e2e-push").join(format!(
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

/// Push distribution: Node A publishes, pushes manifest + 2 pieces per segment to Node B,
/// Node B reconstructs successfully from the pushed subset.
#[tokio::test]
async fn test_push_distribute_and_reconstruct() {
    let dir_a = temp_dir("push-a");
    let dir_b = temp_dir("push-b");

    // Node A publishes
    let mut client_a = CraftObjClient::new(&dir_a).unwrap();
    let input_path = dir_a.join("input.bin");
    let original = b"Push distribution test content. Needs to be long enough for RLNC encoding to produce multiple pieces per segment.";
    std::fs::write(&input_path, original).unwrap();

    let result = client_a.publish(&input_path, &PublishOptions::default()).unwrap();
    let content_id = result.content_id;

    let store_a = FsStore::new(&dir_a).unwrap();
    let store_b = FsStore::new(&dir_b).unwrap();
    let manifest = store_a.get_record(&content_id).unwrap();

    // Push manifest to Node B (simulates PushRecord command)
    store_b.store_record(&manifest).unwrap();

    // Push only 2 pieces per segment (simulates round-robin with 2-per-peer cap)
    let max_per_segment = 2;
    for seg_idx in 0..manifest.segment_count() as u32 {
        let k = manifest.k_for_segment(seg_idx as usize);
        let piece_ids = store_a.list_pieces(&content_id, seg_idx).unwrap();

        // Push min(2, available) pieces — should be >= k for small content
        let to_push = piece_ids.iter().take(max_per_segment.min(k));
        for pid in to_push {
            let (data, coeff) = store_a.get_piece(&content_id, seg_idx, pid).unwrap();
            store_b.store_piece(&content_id, seg_idx, pid, &data, &coeff).unwrap();
        }

        // Verify Node B has enough pieces to reconstruct this segment
        let b_pieces = store_b.list_pieces(&content_id, seg_idx).unwrap();
        assert!(
            b_pieces.len() >= k,
            "Segment {}: need {} pieces, got {}",
            seg_idx, k, b_pieces.len()
        );
    }

    // Node B reconstructs
    let client_b = CraftObjClient::new(&dir_b).unwrap();
    let output_path = dir_b.join("output.bin");
    client_b.reconstruct(&content_id, &output_path, None).unwrap();

    let reconstructed = std::fs::read(&output_path).unwrap();
    assert_eq!(reconstructed, original.as_slice());

    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
}

/// Three-node scenario: A publishes, pushes different pieces to B and C,
/// then D (or B) can reconstruct by combining pieces from both.
#[tokio::test]
async fn test_three_node_distribute_and_reconstruct() {
    let dir_a = temp_dir("3n-a");
    let dir_b = temp_dir("3n-b");
    let dir_c = temp_dir("3n-c");

    let mut client_a = CraftObjClient::new(&dir_a).unwrap();
    let input_path = dir_a.join("input.bin");
    let original = b"Three-node distribution test. Each storage node gets different pieces.";
    std::fs::write(&input_path, original).unwrap();

    let result = client_a.publish(&input_path, &PublishOptions::default()).unwrap();
    let content_id = result.content_id;

    let store_a = FsStore::new(&dir_a).unwrap();
    let manifest = store_a.get_record(&content_id).unwrap();

    let store_b = FsStore::new(&dir_b).unwrap();
    let store_c = FsStore::new(&dir_c).unwrap();

    // Push manifest to both
    store_b.store_record(&manifest).unwrap();
    store_c.store_record(&manifest).unwrap();

    // Round-robin pieces: even-indexed to B, odd-indexed to C
    for seg_idx in 0..manifest.segment_count() as u32 {
        let piece_ids = store_a.list_pieces(&content_id, seg_idx).unwrap();
        for (i, pid) in piece_ids.iter().enumerate() {
            let (data, coeff) = store_a.get_piece(&content_id, seg_idx, pid).unwrap();
            if i % 2 == 0 {
                store_b.store_piece(&content_id, seg_idx, pid, &data, &coeff).unwrap();
            } else {
                store_c.store_piece(&content_id, seg_idx, pid, &data, &coeff).unwrap();
            }
        }
    }

    // Simulate a fetcher (Node D) that gathers pieces from both B and C
    let dir_d = temp_dir("3n-d");
    let store_d = FsStore::new(&dir_d).unwrap();
    store_d.store_record(&manifest).unwrap();

    for seg_idx in 0..manifest.segment_count() as u32 {
        let k = manifest.k_for_segment(seg_idx as usize);
        let mut collected = 0;

        // Gather from B first, then C
        for source in [&store_b, &store_c] {
            if collected >= k {
                break;
            }
            let pids = source.list_pieces(&content_id, seg_idx).unwrap_or_default();
            for pid in pids {
                if collected >= k {
                    break;
                }
                let (data, coeff) = source.get_piece(&content_id, seg_idx, &pid).unwrap();
                store_d.store_piece(&content_id, seg_idx, &pid, &data, &coeff).unwrap();
                collected += 1;
            }
        }

        assert!(collected >= k, "Segment {}: need {} pieces, collected {}", seg_idx, k, collected);
    }

    let client_d = CraftObjClient::new(&dir_d).unwrap();
    let output_path = dir_d.join("output.bin");
    client_d.reconstruct(&content_id, &output_path, None).unwrap();

    let reconstructed = std::fs::read(&output_path).unwrap();
    assert_eq!(reconstructed, original.as_slice());

    for d in [&dir_a, &dir_b, &dir_c, &dir_d] {
        std::fs::remove_dir_all(d).ok();
    }
}
