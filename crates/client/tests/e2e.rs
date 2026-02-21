//! End-to-end integration tests for CraftObj.
//!
//! Covers the kernel content lifecycle: publish, fetch, pin/unpin.
//! COM-layer tests (PRE, access control, payment channels) removed.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use craftobj_client::CraftObjClient;
use craftobj_core::{ContentId, PublishOptions};
use rand::RngCore;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tmp_dir() -> PathBuf {
    let mut rng_bytes = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut rng_bytes);
    let dir = std::env::temp_dir()
        .join("craftobj-integration")
        .join(format!(
            "{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            hex::encode(rng_bytes)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_test_file(dir: &PathBuf, name: &str, content: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    path
}

// ===========================================================================
// 1. Publish → Fetch (plaintext)
// ===========================================================================

#[test]
fn publish_fetch_plaintext() {
    let dir = tmp_dir();
    let mut client = CraftObjClient::new(&dir).unwrap();

    let content = b"Hello CraftObj - plaintext lifecycle test with enough data to chunk.";
    let file = write_test_file(&dir, "plain.txt", content);

    // Publish
    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    assert_eq!(result.total_size, content.len() as u64);
    assert!(result.encryption_key.is_none());

    // Fetch (reconstruct) by CID
    let out = dir.join("plain_out.txt");
    client
        .reconstruct(&result.content_id, &out, None)
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), content);

    // Verify CID is deterministic
    let cid2 = ContentId::from_bytes(content);
    assert_eq!(result.content_id, cid2);

    // Verify pinned
    assert!(client.is_pinned(&result.content_id));

    // List shows it
    let items = client.list().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].content_id, result.content_id);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn publish_fetch_large_content_multichunk() {
    let dir = tmp_dir();
    let mut client = CraftObjClient::new(&dir).unwrap();

    // 200KB — will produce multiple chunks at 64KB default
    let content: Vec<u8> = (0..200_000).map(|i| (i % 256) as u8).collect();
    let file = write_test_file(&dir, "large.bin", &content);

    let result = client.publish(&file, &PublishOptions::default()).unwrap();
    assert!(result.segment_count >= 1);

    let out = dir.join("large_out.bin");
    client
        .reconstruct(&result.content_id, &out, None)
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), content);

    std::fs::remove_dir_all(&dir).ok();
}

// NOTE: PRE/access control, revoke-and-rotate, payment channel, and
// storage receipt signing tests removed — these are COM layer concerns.
