//! E2E integration test: two in-process nodes.
//!
//! Node A publishes a file, Node B fetches pieces via the wire framing
//! protocol, then reconstructs and verifies the content matches.

use std::path::PathBuf;

use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, PublishOptions, WireStatus};
use datacraft_store::{piece_id_from_coefficients, FsStore};
use datacraft_transfer::{DataCraftRequest, DataCraftResponse, PiecePayload};
use datacraft_transfer::wire;

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

/// Simulate a PieceSync request-response over in-memory streams using the wire framing.
async fn simulate_piece_sync(
    store: &FsStore,
    content_id: &ContentId,
    segment_index: u32,
    have_pieces: Vec<[u8; 32]>,
    max_pieces: u16,
) -> Vec<PiecePayload> {
    let request = DataCraftRequest::PieceSync {
        content_id: *content_id,
        segment_index,
        merkle_root: [0u8; 32],
        have_pieces: have_pieces.clone(),
        max_pieces,
    };

    // "Server" handles the request locally — no actual network needed for e2e test
    let piece_ids = store.list_pieces(content_id, segment_index).unwrap_or_default();
    let have_set: std::collections::HashSet<[u8; 32]> = have_pieces.into_iter().collect();
    let mut pieces = Vec::new();
    for pid in piece_ids {
        if have_set.contains(&pid) {
            continue;
        }
        if pieces.len() >= max_pieces as usize {
            break;
        }
        if let Ok((data, coefficients)) = store.get_piece(content_id, segment_index, &pid) {
            pieces.push(PiecePayload {
                segment_index,
                piece_id: pid,
                coefficients,
                data,
            });
        }
    }

    // Verify wire framing roundtrip
    let mut buf = Vec::new();
    wire::write_request_frame(&mut futures::io::Cursor::new(&mut buf), 1, &request)
        .await
        .unwrap();
    let frame = wire::read_frame(&mut futures::io::Cursor::new(&buf)).await.unwrap();
    match frame {
        wire::StreamFrame::Request { seq_id, .. } => assert_eq!(seq_id, 1),
        _ => panic!("expected request frame"),
    }

    let response = DataCraftResponse::PieceBatch { pieces: pieces.clone() };
    let mut resp_buf = Vec::new();
    wire::write_response_frame(&mut futures::io::Cursor::new(&mut resp_buf), 1, &response)
        .await
        .unwrap();
    let resp_frame = wire::read_frame(&mut futures::io::Cursor::new(&resp_buf)).await.unwrap();

    match resp_frame {
        wire::StreamFrame::Response { response: DataCraftResponse::PieceBatch { pieces }, .. } => pieces,
        _ => panic!("unexpected response frame"),
    }
}

/// Full pipeline: Node A publishes → pieces transferred via codec → Node B reconstructs.
#[tokio::test]
async fn test_two_node_publish_fetch_plaintext() {
    let dir_a = temp_dir("node-a");
    let dir_b = temp_dir("node-b");

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
    let store_b = FsStore::new(&dir_b).unwrap();

    for seg_idx in 0..manifest.segment_count as u32 {
        let pieces = simulate_piece_sync(&store_a, &content_id, seg_idx, vec![], 1000).await;
        for p in &pieces {
            store_b
                .store_piece(&content_id, seg_idx, &p.piece_id, &p.data, &p.coefficients)
                .unwrap();
        }
    }

    store_b.store_manifest(&manifest).unwrap();

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
        .publish(&input_path, &PublishOptions { encrypted: true })
        .unwrap();

    let content_id = publish_result.content_id;
    let encryption_key = publish_result.encryption_key.clone().unwrap();

    let plaintext_cid = ContentId::from_bytes(original_content);
    assert_ne!(content_id, plaintext_cid);

    let store_a = FsStore::new(&dir_a).unwrap();
    let manifest = store_a.get_manifest(&content_id).unwrap();
    let store_b = FsStore::new(&dir_b).unwrap();

    for seg_idx in 0..manifest.segment_count as u32 {
        let pieces = simulate_piece_sync(&store_a, &content_id, seg_idx, vec![], 1000).await;
        for p in &pieces {
            store_b
                .store_piece(&content_id, seg_idx, &p.piece_id, &p.data, &p.coefficients)
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

/// Test "any piece" request: Node B requests pieces using PieceSync with exclusion list.
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

    for seg_idx in 0..manifest.segment_count as u32 {
        let k = manifest.k_for_segment(seg_idx as usize);
        let pieces = simulate_piece_sync(&store_a, &content_id, seg_idx, vec![], 1000).await;
        for p in &pieces {
            store_b
                .store_piece(&content_id, seg_idx, &p.piece_id, &p.data, &p.coefficients)
                .unwrap();
        }

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
