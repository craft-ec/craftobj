//! End-to-end integration tests for CraftOBJ.
//!
//! Covers the full content lifecycle: publish, fetch, encrypt, access control (PRE),
//! revocation with key rotation, payment channels, and storage receipts.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use craftobj_client::CraftOBJClient;
use craftobj_core::{
    ContentId, PublishOptions, StorageReceipt,
    payment_channel::{
        PaymentChannel, PaymentChannelError, PaymentVoucher, sign_voucher, verify_voucher,
    },
    signing::{sign_storage_receipt, verify_storage_receipt},
};
use ed25519_dalek::SigningKey;
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

fn keygen() -> SigningKey {
    SigningKey::generate(&mut rand::thread_rng())
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
    let mut client = CraftOBJClient::new(&dir).unwrap();

    let content = b"Hello CraftOBJ - plaintext lifecycle test with enough data to chunk.";
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
    let mut client = CraftOBJClient::new(&dir).unwrap();

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

// ===========================================================================
// 2. Publish encrypted → Grant → Fetch → Decrypt (PRE flow)
// ===========================================================================

#[test]
fn publish_encrypted_grant_fetch_decrypt() {
    let dir = tmp_dir();
    let mut client = CraftOBJClient::new(&dir).unwrap();

    let creator = keygen();
    let recipient = keygen();
    let content = b"Encrypted content for PRE access control lifecycle test.";
    let file = write_test_file(&dir, "secret.txt", content);

    // 1. Publish with PRE
    let (result, _encrypted_ck) = client
        .publish_with_pre(&file, &PublishOptions::default(), &creator)
        .unwrap();
    let content_key = result.encryption_key.as_ref().unwrap();

    // 2. Grant access to recipient
    let (_re_key_entry, re_encrypted) = client
        .grant_access(&creator, &recipient.verifying_key(), content_key)
        .unwrap();

    // 3. Recipient decrypts via PRE
    let out = dir.join("decrypted.txt");
    client
        .reconstruct_with_pre(
            &result.content_id,
            &out,
            &re_encrypted,
            &recipient,
            &creator.verifying_key(),
        )
        .unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), content);

    // 4. Without key → ciphertext (not plaintext)
    let out_raw = dir.join("raw.bin");
    client
        .reconstruct(&result.content_id, &out_raw, None)
        .unwrap();
    let raw = std::fs::read(&out_raw).unwrap();
    assert_ne!(raw, content);
    assert!(raw.len() > content.len()); // nonce + tag overhead

    // 5. Wrong recipient fails
    let wrong = keygen();
    let out_wrong = dir.join("wrong.txt");
    assert!(client
        .reconstruct_with_pre(
            &result.content_id,
            &out_wrong,
            &re_encrypted,
            &wrong,
            &creator.verifying_key(),
        )
        .is_err());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn pre_multiple_recipients_independent() {
    let dir = tmp_dir();
    let mut client = CraftOBJClient::new(&dir).unwrap();

    let creator = keygen();
    let recipients: Vec<SigningKey> = (0..5).map(|_| keygen()).collect();
    let content = b"Shared content for multiple recipients.";
    let file = write_test_file(&dir, "shared.txt", content);

    let (result, _) = client
        .publish_with_pre(&file, &PublishOptions::default(), &creator)
        .unwrap();
    let ck = result.encryption_key.as_ref().unwrap();

    // Grant each recipient independently
    let re_encrypted_keys: Vec<_> = recipients
        .iter()
        .map(|r| {
            client
                .grant_access(&creator, &r.verifying_key(), ck)
                .unwrap()
                .1
        })
        .collect();

    // Each can decrypt
    for (i, (recipient, re_enc)) in recipients.iter().zip(&re_encrypted_keys).enumerate() {
        let out = dir.join(format!("recipient_{}.txt", i));
        client
            .reconstruct_with_pre(
                &result.content_id,
                &out,
                re_enc,
                recipient,
                &creator.verifying_key(),
            )
            .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), content);
    }

    std::fs::remove_dir_all(&dir).ok();
}

// ===========================================================================
// 3. Revoke → Verify denied
// ===========================================================================

#[test]
fn revoke_and_rotate_denies_revoked_user() {
    let dir = tmp_dir();
    let mut client = CraftOBJClient::new(&dir).unwrap();

    let creator = keygen();
    let user_a = keygen();
    let user_b = keygen(); // will be revoked
    let user_c = keygen();
    let content = b"Content that will have access revoked for user B.";
    let file = write_test_file(&dir, "revocable.txt", content);

    // Publish encrypted with PRE
    let (result, _) = client
        .publish_with_pre(&file, &PublishOptions::default(), &creator)
        .unwrap();
    let content_key = result.encryption_key.as_ref().unwrap().clone();

    let all_users = vec![
        user_a.verifying_key(),
        user_b.verifying_key(),
        user_c.verifying_key(),
    ];

    // Grant all 3
    let mut re_keys = Vec::new();
    for user in &all_users {
        let (_, re_enc) = client.grant_access(&creator, user, &content_key).unwrap();
        re_keys.push(re_enc);
    }

    // Verify all 3 can decrypt original
    for (i, (user, re_enc)) in [&user_a, &user_b, &user_c]
        .iter()
        .zip(&re_keys)
        .enumerate()
    {
        let out = dir.join(format!("pre_revoke_{}.txt", i));
        client
            .reconstruct_with_pre(
                &result.content_id,
                &out,
                re_enc,
                user,
                &creator.verifying_key(),
            )
            .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), content);
    }

    // Revoke user_b → rotate
    let revocation = client
        .revoke_and_rotate(
            &result.content_id,
            &content_key,
            &creator,
            &user_b.verifying_key(),
            &all_users,
        )
        .unwrap();

    // New CID is different
    assert_ne!(result.content_id, revocation.new_content_id);
    // 2 remaining users got re-grants
    assert_eq!(revocation.re_grants.len(), 2);

    // Remaining users (A and C) can decrypt NEW content
    for (entry, re_enc) in &revocation.re_grants {
        let recipient = if entry.recipient_did == user_a.verifying_key().to_bytes() {
            &user_a
        } else {
            assert_eq!(entry.recipient_did, user_c.verifying_key().to_bytes());
            &user_c
        };
        let out = dir.join(format!(
            "post_revoke_{}.txt",
            hex::encode(&entry.recipient_did[..4])
        ));
        client
            .reconstruct_with_pre(
                &revocation.new_content_id,
                &out,
                re_enc,
                recipient,
                &creator.verifying_key(),
            )
            .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), content);
    }

    // user_b CANNOT decrypt new CID with old re-encrypted key
    let out_b = dir.join("post_revoke_b.txt");
    assert!(client
        .reconstruct_with_pre(
            &revocation.new_content_id,
            &out_b,
            &re_keys[1], // user_b's old key
            &user_b,
            &creator.verifying_key(),
        )
        .is_err());

    std::fs::remove_dir_all(&dir).ok();
}

// ===========================================================================
// 4. Payment channel lifecycle
// ===========================================================================

#[test]
fn payment_channel_full_lifecycle() {
    let sender_key = keygen();
    let sender_pub = sender_key.verifying_key();
    let receiver_pub = keygen().verifying_key();

    let channel_id = [42u8; 32];
    let locked = 1_000_000u64; // 1M USDC lamports

    // 1. Open channel
    let mut channel = PaymentChannel::new(
        channel_id,
        sender_pub.to_bytes(),
        receiver_pub.to_bytes(),
        locked,
    );
    assert_eq!(channel.remaining(), locked);
    assert_eq!(channel.nonce, 0);
    assert_eq!(channel.spent, 0);

    // 2. Issue vouchers with incrementing nonce, cumulative amount
    let amounts = [100_000u64, 300_000, 600_000, 900_000];
    for (i, &amount) in amounts.iter().enumerate() {
        let nonce = (i + 1) as u64;
        let mut voucher = PaymentVoucher {
            channel_id,
            cumulative_amount: amount,
            nonce,
            signature: vec![],
        };
        sign_voucher(&mut voucher, &sender_key);

        // Verify signature
        assert!(verify_voucher(&voucher, &sender_pub));

        // Apply to channel
        channel.apply_voucher(&voucher).unwrap();
        assert_eq!(channel.spent, amount);
        assert_eq!(channel.nonce, nonce);
        assert_eq!(channel.remaining(), locked - amount);
    }

    // 3. Final state before close
    assert_eq!(channel.spent, 900_000);
    assert_eq!(channel.remaining(), 100_000);

    // 4. Verify invalid vouchers are rejected

    // Stale nonce
    let mut stale = PaymentVoucher {
        channel_id,
        cumulative_amount: 950_000,
        nonce: 2, // already past nonce=4
        signature: vec![],
    };
    sign_voucher(&mut stale, &sender_key);
    assert_eq!(
        channel.apply_voucher(&stale),
        Err(PaymentChannelError::StaleNonce {
            voucher_nonce: 2,
            current_nonce: 4,
        })
    );

    // Overspend
    let mut overspend = PaymentVoucher {
        channel_id,
        cumulative_amount: 2_000_000,
        nonce: 5,
        signature: vec![],
    };
    sign_voucher(&mut overspend, &sender_key);
    assert_eq!(
        channel.apply_voucher(&overspend),
        Err(PaymentChannelError::Overspend {
            amount: 2_000_000,
            locked: 1_000_000,
        })
    );

    // Decreasing amount
    let mut decreasing = PaymentVoucher {
        channel_id,
        cumulative_amount: 500_000, // < 900k
        nonce: 5,
        signature: vec![],
    };
    sign_voucher(&mut decreasing, &sender_key);
    assert_eq!(
        channel.apply_voucher(&decreasing),
        Err(PaymentChannelError::DecreasingAmount {
            new_amount: 500_000,
            current_spent: 900_000,
        })
    );

    // Wrong channel
    let mut wrong_ch = PaymentVoucher {
        channel_id: [99u8; 32],
        cumulative_amount: 950_000,
        nonce: 5,
        signature: vec![],
    };
    sign_voucher(&mut wrong_ch, &sender_key);
    assert_eq!(
        channel.apply_voucher(&wrong_ch),
        Err(PaymentChannelError::ChannelMismatch)
    );

    // Wrong signer
    let wrong_key = keygen();
    let mut wrong_sig = PaymentVoucher {
        channel_id,
        cumulative_amount: 950_000,
        nonce: 5,
        signature: vec![],
    };
    sign_voucher(&mut wrong_sig, &wrong_key);
    assert!(!verify_voucher(&wrong_sig, &sender_pub));

    // 5. Close with final voucher
    let mut final_voucher = PaymentVoucher {
        channel_id,
        cumulative_amount: 1_000_000,
        nonce: 5,
        signature: vec![],
    };
    sign_voucher(&mut final_voucher, &sender_key);
    channel.apply_voucher(&final_voucher).unwrap();
    assert_eq!(channel.remaining(), 0);
    assert_eq!(channel.spent, locked);

    // Serialization roundtrip
    let json = serde_json::to_string(&channel).unwrap();
    let restored: PaymentChannel = serde_json::from_str(&json).unwrap();
    assert_eq!(restored.spent, channel.spent);
    assert_eq!(restored.channel_id, channel.channel_id);
}

#[test]
fn payment_voucher_tamper_detection() {
    let key = keygen();
    let pubkey = key.verifying_key();

    let mut voucher = PaymentVoucher {
        channel_id: [1u8; 32],
        cumulative_amount: 50_000,
        nonce: 1,
        signature: vec![],
    };
    sign_voucher(&mut voucher, &key);
    assert!(verify_voucher(&voucher, &pubkey));

    // Tamper amount
    voucher.cumulative_amount = 999_999;
    assert!(!verify_voucher(&voucher, &pubkey));
}

// ===========================================================================
// 5. StorageReceipt generation and verification
// ===========================================================================

fn make_storage_receipt(
    cid: ContentId,
    storage_node: [u8; 32],
    challenger: [u8; 32],
    segment: u32,
    nonce: [u8; 32],
) -> StorageReceipt {
    StorageReceipt {
        content_id: cid,
        storage_node,
        challenger,
        segment_index: segment,
        piece_id: {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(b"piece_coeff_placeholder");
            h.update(segment.to_le_bytes());
            let r = h.finalize();
            let mut out = [0u8; 32];
            out.copy_from_slice(&r);
            out
        },
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        nonce,
        proof_hash: {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(b"piece_data_placeholder");
            h.update(nonce);
            let r = h.finalize();
            let mut out = [0u8; 32];
            out.copy_from_slice(&r);
            out
        },
        signature: vec![],
    }
}

#[test]
fn storage_receipt_sign_verify_roundtrip() {
    let challenger_key = keygen();
    let challenger_pub = challenger_key.verifying_key();
    let cid = ContentId([77u8; 32]);

    // Generate and sign a receipt
    let mut receipt = make_storage_receipt(
        cid,
        [1u8; 32],
        challenger_pub.to_bytes(),
        0,
        [9u8; 32],
    );
    sign_storage_receipt(&mut receipt, &challenger_key);

    // Verify
    assert_eq!(receipt.signature.len(), 64);
    assert!(verify_storage_receipt(&receipt, &challenger_pub));

    // Wrong key fails
    let wrong_key = keygen();
    assert!(!verify_storage_receipt(&receipt, &wrong_key.verifying_key()));

    // Tamper detection
    let mut tampered = receipt.clone();
    tampered.segment_index = 99;
    assert!(!verify_storage_receipt(&tampered, &challenger_pub));

    let mut tampered2 = receipt.clone();
    tampered2.timestamp += 1;
    assert!(!verify_storage_receipt(&tampered2, &challenger_pub));
}

#[test]
fn storage_receipt_batch_pdp_simulation() {
    let challenger_key = keygen();
    let challenger_pub = challenger_key.verifying_key();
    let cid = ContentId([88u8; 32]);

    // Simulate a PDP challenge round: challenger verifies 5 shards from a storage node
    let storage_node = [0xAA; 32];
    let mut receipts = Vec::new();

    for shard in 0..5u32 {
        let mut nonce = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut nonce);

        let mut receipt = make_storage_receipt(
            cid,
            storage_node,
            challenger_pub.to_bytes(),
            shard,
            nonce,
        );
        sign_storage_receipt(&mut receipt, &challenger_key);
        receipts.push(receipt);
    }

    // All receipts valid
    assert_eq!(receipts.len(), 5);
    for r in &receipts {
        assert!(verify_storage_receipt(r, &challenger_pub));
        assert_eq!(r.content_id, cid);
        assert_eq!(r.storage_node, storage_node);
        assert_eq!(r.challenger, challenger_pub.to_bytes());
    }

    // Each receipt has unique shard index
    let shard_indices: Vec<u32> = receipts.iter().map(|r| r.segment_index).collect();
    assert_eq!(shard_indices, vec![0, 1, 2, 3, 4]);

    // Signable data is deterministic
    for r in &receipts {
        assert_eq!(r.signable_data(), r.signable_data());
    }

    // Weight
    for r in &receipts {
        assert_eq!(r.weight(), 1);
    }
}

#[test]
fn storage_receipt_serialization() {
    let key = keygen();
    let cid = ContentId([99u8; 32]);
    let mut receipt = make_storage_receipt(cid, [1u8; 32], key.verifying_key().to_bytes(), 3, [5u8; 32]);
    sign_storage_receipt(&mut receipt, &key);

    // bincode roundtrip
    let bytes = bincode::serialize(&receipt).unwrap();
    let restored: StorageReceipt = bincode::deserialize(&bytes).unwrap();
    assert_eq!(restored.content_id, receipt.content_id);
    assert_eq!(restored.segment_index, receipt.segment_index);
    assert!(verify_storage_receipt(&restored, &key.verifying_key()));

    // JSON roundtrip
    let json = serde_json::to_string(&receipt).unwrap();
    let restored2: StorageReceipt = serde_json::from_str(&json).unwrap();
    assert!(verify_storage_receipt(&restored2, &key.verifying_key()));
}
