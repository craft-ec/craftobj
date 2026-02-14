//! E2E integration test: two in-process nodes.
//!
//! Node A publishes a file, Node B fetches shards via the wire protocol,
//! then reconstructs and verifies the content matches.

use std::path::PathBuf;

use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, PublishOptions};
use datacraft_store::FsStore;
use datacraft_transfer::{
    decode_shard_request, handle_shard_request, request_shard, REQUEST_HEADER_SIZE,
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

/// Full pipeline: Node A publishes → shards transferred over wire protocol → Node B reconstructs.
#[tokio::test]
async fn test_two_node_publish_fetch_plaintext() {
    let dir_a = temp_dir("node-a");
    let dir_b = temp_dir("node-b");

    // --- Node A: publish a file ---
    let mut client_a = DataCraftClient::new(&dir_a).unwrap();
    let input_path = dir_a.join("input.bin");
    let original_content = b"Hello from Node A! This is test content for the two-node E2E test. \
        It needs to be long enough to exercise chunking with small chunk sizes.";
    std::fs::write(&input_path, original_content).unwrap();

    let publish_result = client_a
        .publish(&input_path, &PublishOptions::default())
        .unwrap();
    let content_id = publish_result.content_id;

    // Verify Node A has the content
    assert_eq!(publish_result.total_size, original_content.len() as u64);
    assert!(publish_result.encryption_key.is_none());

    let store_a = FsStore::new(&dir_a).unwrap();
    let manifest = store_a.get_manifest(&content_id).unwrap();
    let total_shards = manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards;

    // --- Transfer shards from Node A → Node B via wire protocol ---
    let store_b = FsStore::new(&dir_b).unwrap();

    for chunk_idx in 0..manifest.chunk_count {
        for shard_idx in 0..total_shards as u8 {
            let (mut client_end, mut server_end) = tokio::io::duplex(65536);

            // Spawn server (Node A) to handle shard request
            let server_store = FsStore::new(&dir_a).unwrap();
            let server_handle = tokio::spawn(async move {
                let mut header = [0u8; REQUEST_HEADER_SIZE];
                server_end.read_exact(&mut header).await.unwrap();
                let (req_cid, chunk, shard) = decode_shard_request(&header).unwrap();
                handle_shard_request(&mut server_end, &server_store, &req_cid, chunk, shard)
                    .await
                    .unwrap();
            });

            // Client (Node B) requests the shard
            let shard_data =
                request_shard(&mut client_end, &content_id, chunk_idx, shard_idx)
                    .await
                    .unwrap();

            server_handle.await.unwrap();

            // Node B stores the shard locally
            store_b
                .put_shard(&content_id, chunk_idx, shard_idx, &shard_data)
                .unwrap();
        }
    }

    // Node B also needs the manifest (in real life fetched via DHT)
    store_b.put_manifest(&manifest).unwrap();

    // --- Node B: reconstruct the file ---
    let client_b = DataCraftClient::new(&dir_b).unwrap();
    let output_path = dir_b.join("output.bin");
    client_b
        .reconstruct(&content_id, &output_path, None)
        .unwrap();

    let reconstructed = std::fs::read(&output_path).unwrap();
    assert_eq!(reconstructed, original_content);

    // Cleanup
    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
}

/// Same test but with encrypted content: Node A encrypts, Node B decrypts with key.
#[tokio::test]
async fn test_two_node_publish_fetch_encrypted() {
    let dir_a = temp_dir("node-a-enc");
    let dir_b = temp_dir("node-b-enc");

    // --- Node A: publish encrypted ---
    let mut client_a = DataCraftClient::new(&dir_a).unwrap();
    let input_path = dir_a.join("secret.txt");
    let original_content = b"This is encrypted content that should survive the full pipeline.";
    std::fs::write(&input_path, original_content).unwrap();

    let publish_result = client_a
        .publish(
            &input_path,
            &PublishOptions {
                encrypted: true,
                erasure_config: None,
            },
        )
        .unwrap();

    let content_id = publish_result.content_id;
    let encryption_key = publish_result.encryption_key.clone().unwrap();

    // CID is hash of ciphertext, not plaintext
    let plaintext_cid = ContentId::from_bytes(original_content);
    assert_ne!(content_id, plaintext_cid);

    // --- Transfer all shards via wire protocol ---
    let store_a = FsStore::new(&dir_a).unwrap();
    let manifest = store_a.get_manifest(&content_id).unwrap();
    assert!(manifest.encrypted);
    let total_shards = manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards;

    let store_b = FsStore::new(&dir_b).unwrap();

    for chunk_idx in 0..manifest.chunk_count {
        for shard_idx in 0..total_shards as u8 {
            let (mut client_end, mut server_end) = tokio::io::duplex(65536);

            let server_store = FsStore::new(&dir_a).unwrap();
            let server_handle = tokio::spawn(async move {
                let mut header = [0u8; REQUEST_HEADER_SIZE];
                server_end.read_exact(&mut header).await.unwrap();
                let (req_cid, chunk, shard) = decode_shard_request(&header).unwrap();
                handle_shard_request(&mut server_end, &server_store, &req_cid, chunk, shard)
                    .await
                    .unwrap();
            });

            let shard_data =
                request_shard(&mut client_end, &content_id, chunk_idx, shard_idx)
                    .await
                    .unwrap();

            server_handle.await.unwrap();
            store_b
                .put_shard(&content_id, chunk_idx, shard_idx, &shard_data)
                .unwrap();
        }
    }

    store_b.put_manifest(&manifest).unwrap();

    // --- Node B: reconstruct with encryption key ---
    let client_b = DataCraftClient::new(&dir_b).unwrap();
    let output_path = dir_b.join("decrypted.txt");
    client_b
        .reconstruct(&content_id, &output_path, Some(&encryption_key))
        .unwrap();

    let reconstructed = std::fs::read(&output_path).unwrap();
    assert_eq!(reconstructed, original_content);

    // Cleanup
    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
}

/// Test partial shard transfer: Node B only gets data_shards (not parity),
/// verifying erasure coding recovery works across the transfer boundary.
#[tokio::test]
async fn test_two_node_partial_shards_erasure_recovery() {
    let dir_a = temp_dir("node-a-partial");
    let dir_b = temp_dir("node-b-partial");

    // --- Node A: publish ---
    let mut client_a = DataCraftClient::new(&dir_a).unwrap();
    let input_path = dir_a.join("data.bin");
    let original_content = b"Content for partial shard recovery test across two nodes.";
    std::fs::write(&input_path, original_content).unwrap();

    let publish_result = client_a
        .publish(&input_path, &PublishOptions::default())
        .unwrap();
    let content_id = publish_result.content_id;

    let store_a = FsStore::new(&dir_a).unwrap();
    let manifest = store_a.get_manifest(&content_id).unwrap();
    let data_shards = manifest.erasure_config.data_shards;

    // --- Transfer ONLY data_shards (skip parity) via wire protocol ---
    let store_b = FsStore::new(&dir_b).unwrap();

    for chunk_idx in 0..manifest.chunk_count {
        // Only transfer the first data_shards shards, skip parity
        for shard_idx in 0..data_shards as u8 {
            let (mut client_end, mut server_end) = tokio::io::duplex(65536);

            let server_store = FsStore::new(&dir_a).unwrap();
            let server_handle = tokio::spawn(async move {
                let mut header = [0u8; REQUEST_HEADER_SIZE];
                server_end.read_exact(&mut header).await.unwrap();
                let (req_cid, chunk, shard) = decode_shard_request(&header).unwrap();
                handle_shard_request(&mut server_end, &server_store, &req_cid, chunk, shard)
                    .await
                    .unwrap();
            });

            let shard_data =
                request_shard(&mut client_end, &content_id, chunk_idx, shard_idx)
                    .await
                    .unwrap();

            server_handle.await.unwrap();
            store_b
                .put_shard(&content_id, chunk_idx, shard_idx, &shard_data)
                .unwrap();
        }
    }

    store_b.put_manifest(&manifest).unwrap();

    // --- Node B: reconstruct with only data shards (no parity) ---
    let client_b = DataCraftClient::new(&dir_b).unwrap();
    let output_path = dir_b.join("recovered.bin");
    client_b
        .reconstruct(&content_id, &output_path, None)
        .unwrap();

    let reconstructed = std::fs::read(&output_path).unwrap();
    assert_eq!(reconstructed, original_content);

    // Cleanup
    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
}
