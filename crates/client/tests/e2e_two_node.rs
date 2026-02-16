//! E2E integration test: two in-process nodes.
//!
//! Node A publishes a file, Node B fetches pieces via the wire protocol,
//! then reconstructs and verifies the content matches.

use std::path::PathBuf;

use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, PublishOptions};
use datacraft_store::{piece_id_from_coefficients, FsStore};
use datacraft_transfer::{
    decode_piece_request, handle_piece_request, request_piece, PIECE_REQUEST_SIZE,
};
use tokio::io::AsyncReadExt;

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("datacraft-e2e").join(format!(
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

/// Full pipeline: Node A publishes → pieces transferred over wire protocol → Node B reconstructs.
#[tokio::test]
async fn test_two_node_publish_fetch_plaintext() {
    let dir_a = temp_dir("node-a");
    let dir_b = temp_dir("node-b");

    // --- Node A: publish a file ---
    let mut client_a = DataCraftClient::new(&dir_a).unwrap();
    let input_path = dir_a.join("input.bin");
    let original_content = b"Hello from Node A! This is test content for the two-node E2E test. \
        It needs to be long enough to exercise encoding with small piece sizes.";
    std::fs::write(&input_path, original_content).unwrap();

    let publish_result = client_a
        .publish(&input_path, &PublishOptions::default())
        .unwrap();
    let content_id = publish_result.content_id;

    assert_eq!(publish_result.total_size, original_content.len() as u64);
    assert!(publish_result.encryption_key.is_none());

    let store_a = FsStore::new(&dir_a).unwrap();
    let manifest = store_a.get_manifest(&content_id).unwrap();

    // --- Transfer pieces from Node A → Node B via wire protocol ---
    let store_b = FsStore::new(&dir_b).unwrap();

    for seg_idx in 0..manifest.segment_count as u32 {
        let piece_ids = store_a.list_pieces(&content_id, seg_idx).unwrap();
        for pid in &piece_ids {
            let (mut client_end, mut server_end) = tokio::io::duplex(1_048_576);

            let server_store = FsStore::new(&dir_a).unwrap();
            let pid_copy = *pid;
            let server_handle = tokio::spawn(async move {
                let mut header = [0u8; PIECE_REQUEST_SIZE];
                server_end.read_exact(&mut header).await.unwrap();
                let (req_cid, seg, req_pid) = decode_piece_request(&header).unwrap();
                handle_piece_request(&mut server_end, &server_store, &req_cid, seg, &req_pid)
                    .await
                    .unwrap();
            });

            let (coeff, data) =
                request_piece(&mut client_end, &content_id, seg_idx, &pid_copy)
                    .await
                    .unwrap();

            server_handle.await.unwrap();

            let received_pid = piece_id_from_coefficients(&coeff);
            store_b
                .store_piece(&content_id, seg_idx, &received_pid, &data, &coeff)
                .unwrap();
        }
    }

    // Node B also needs the manifest
    store_b.store_manifest(&manifest).unwrap();

    // --- Node B: reconstruct the file ---
    let client_b = DataCraftClient::new(&dir_b).unwrap();
    let output_path = dir_b.join("output.bin");
    client_b
        .reconstruct(&content_id, &output_path, None)
        .unwrap();

    let reconstructed = std::fs::read(&output_path).unwrap();
    assert_eq!(reconstructed, original_content);

    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
}

/// Same test but with encrypted content.
#[tokio::test]
async fn test_two_node_publish_fetch_encrypted() {
    let dir_a = temp_dir("node-a-enc");
    let dir_b = temp_dir("node-b-enc");

    let mut client_a = DataCraftClient::new(&dir_a).unwrap();
    let input_path = dir_a.join("secret.txt");
    let original_content = b"This is encrypted content that should survive the full pipeline.";
    std::fs::write(&input_path, original_content).unwrap();

    let publish_result = client_a
        .publish(
            &input_path,
            &PublishOptions { encrypted: true },
        )
        .unwrap();

    let content_id = publish_result.content_id;
    let encryption_key = publish_result.encryption_key.clone().unwrap();

    let plaintext_cid = ContentId::from_bytes(original_content);
    assert_ne!(content_id, plaintext_cid);

    let store_a = FsStore::new(&dir_a).unwrap();
    let manifest = store_a.get_manifest(&content_id).unwrap();

    let store_b = FsStore::new(&dir_b).unwrap();

    for seg_idx in 0..manifest.segment_count as u32 {
        let piece_ids = store_a.list_pieces(&content_id, seg_idx).unwrap();
        for pid in &piece_ids {
            let (mut client_end, mut server_end) = tokio::io::duplex(1_048_576);

            let server_store = FsStore::new(&dir_a).unwrap();
            let pid_copy = *pid;
            let server_handle = tokio::spawn(async move {
                let mut header = [0u8; PIECE_REQUEST_SIZE];
                server_end.read_exact(&mut header).await.unwrap();
                let (req_cid, seg, req_pid) = decode_piece_request(&header).unwrap();
                handle_piece_request(&mut server_end, &server_store, &req_cid, seg, &req_pid)
                    .await
                    .unwrap();
            });

            let (coeff, data) =
                request_piece(&mut client_end, &content_id, seg_idx, &pid_copy)
                    .await
                    .unwrap();

            server_handle.await.unwrap();

            let received_pid = piece_id_from_coefficients(&coeff);
            store_b
                .store_piece(&content_id, seg_idx, &received_pid, &data, &coeff)
                .unwrap();
        }
    }

    store_b.store_manifest(&manifest).unwrap();

    let client_b = DataCraftClient::new(&dir_b).unwrap();
    let output_path = dir_b.join("decrypted.txt");
    client_b
        .reconstruct(&content_id, &output_path, Some(&encryption_key))
        .unwrap();

    let reconstructed = std::fs::read(&output_path).unwrap();
    assert_eq!(reconstructed, original_content);

    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
}

/// Test "any piece" request: Node B requests random pieces without specifying IDs.
#[tokio::test]
async fn test_two_node_any_piece_request() {
    let dir_a = temp_dir("node-a-any");
    let dir_b = temp_dir("node-b-any");

    let mut client_a = DataCraftClient::new(&dir_a).unwrap();
    let input_path = dir_a.join("data.bin");
    let original_content = b"Content for any-piece request test across two nodes.";
    std::fs::write(&input_path, original_content).unwrap();

    let publish_result = client_a
        .publish(&input_path, &PublishOptions::default())
        .unwrap();
    let content_id = publish_result.content_id;

    let store_a = FsStore::new(&dir_a).unwrap();
    let manifest = store_a.get_manifest(&content_id).unwrap();

    let store_b = FsStore::new(&dir_b).unwrap();

    // For each segment, request "any piece" enough times to get k independent pieces
    for seg_idx in 0..manifest.segment_count as u32 {
        let k = manifest.k_for_segment(seg_idx as usize);
        let total_available = store_a.list_pieces(&content_id, seg_idx).unwrap().len();
        // Request more than k to ensure we get enough independent pieces
        for _ in 0..total_available {
            let (mut client_end, mut server_end) = tokio::io::duplex(1_048_576);
            let zeroed = [0u8; 32]; // "any piece"

            let server_store = FsStore::new(&dir_a).unwrap();
            let server_handle = tokio::spawn(async move {
                let mut header = [0u8; PIECE_REQUEST_SIZE];
                server_end.read_exact(&mut header).await.unwrap();
                let (req_cid, seg, req_pid) = decode_piece_request(&header).unwrap();
                handle_piece_request(&mut server_end, &server_store, &req_cid, seg, &req_pid)
                    .await
                    .unwrap();
            });

            let (coeff, data) =
                request_piece(&mut client_end, &content_id, seg_idx, &zeroed)
                    .await
                    .unwrap();

            server_handle.await.unwrap();

            let received_pid = piece_id_from_coefficients(&coeff);
            // Store (may overwrite duplicates, that's fine)
            store_b
                .store_piece(&content_id, seg_idx, &received_pid, &data, &coeff)
                .unwrap();
        }

        // Should have at least k pieces
        let stored = store_b.list_pieces(&content_id, seg_idx).unwrap();
        assert!(stored.len() >= k, "expected at least {} pieces, got {}", k, stored.len());
    }

    store_b.store_manifest(&manifest).unwrap();

    let client_b = DataCraftClient::new(&dir_b).unwrap();
    let output_path = dir_b.join("recovered.bin");
    client_b
        .reconstruct(&content_id, &output_path, None)
        .unwrap();

    let reconstructed = std::fs::read(&output_path).unwrap();
    assert_eq!(reconstructed, original_content);

    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
}
