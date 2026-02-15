//! Persistent payment channel storage.
//!
//! One JSON file per channel in a `channels/` directory.
//! Similar pattern to `receipt_store.rs` but file-per-record for
//! easy inspection and atomic updates.

use std::path::{Path, PathBuf};

use datacraft_core::payment_channel::{
    ChannelId, PaymentChannel, PaymentChannelError, PaymentVoucher, verify_voucher,
};
use ed25519_dalek::VerifyingKey;
use serde::{Deserialize, Serialize};

/// On-disk channel record (wraps PaymentChannel with status).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelRecord {
    pub channel: PaymentChannel,
    pub status: ChannelStatus,
    /// Latest voucher (kept for on-chain redemption).
    pub latest_voucher: Option<PaymentVoucher>,
}

/// Channel lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelStatus {
    Open,
    Closed,
}

/// Persistent store for payment channels.
pub struct ChannelStore {
    dir: PathBuf,
}

/// Errors specific to the channel store.
#[derive(Debug)]
pub enum ChannelStoreError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Channel(PaymentChannelError),
    NotFound(String),
    AlreadyExists(String),
    AlreadyClosed(String),
}

impl std::fmt::Display for ChannelStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Json(e) => write!(f, "JSON error: {}", e),
            Self::Channel(e) => write!(f, "channel error: {}", e),
            Self::NotFound(id) => write!(f, "channel not found: {}", id),
            Self::AlreadyExists(id) => write!(f, "channel already exists: {}", id),
            Self::AlreadyClosed(id) => write!(f, "channel already closed: {}", id),
        }
    }
}

impl std::error::Error for ChannelStoreError {}

impl From<std::io::Error> for ChannelStoreError {
    fn from(e: std::io::Error) -> Self { Self::Io(e) }
}
impl From<serde_json::Error> for ChannelStoreError {
    fn from(e: serde_json::Error) -> Self { Self::Json(e) }
}
impl From<PaymentChannelError> for ChannelStoreError {
    fn from(e: PaymentChannelError) -> Self { Self::Channel(e) }
}

type Result<T> = std::result::Result<T, ChannelStoreError>;

impl ChannelStore {
    /// Create or open a channel store at the given directory.
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn channel_path(&self, id: &ChannelId) -> PathBuf {
        self.dir.join(format!("{}.json", hex::encode(id)))
    }

    fn read_record(&self, id: &ChannelId) -> Result<ChannelRecord> {
        let path = self.channel_path(id);
        if !path.exists() {
            return Err(ChannelStoreError::NotFound(hex::encode(id)));
        }
        let data = std::fs::read(&path)?;
        Ok(serde_json::from_slice(&data)?)
    }

    fn write_record(&self, record: &ChannelRecord) -> Result<()> {
        let path = self.channel_path(&record.channel.channel_id);
        let data = serde_json::to_vec_pretty(record)?;
        std::fs::write(&path, data)?;
        Ok(())
    }

    /// Persist a new payment channel.
    pub fn open_channel(&self, channel: PaymentChannel) -> Result<()> {
        let path = self.channel_path(&channel.channel_id);
        if path.exists() {
            return Err(ChannelStoreError::AlreadyExists(hex::encode(channel.channel_id)));
        }
        let record = ChannelRecord {
            channel,
            status: ChannelStatus::Open,
            latest_voucher: None,
        };
        self.write_record(&record)
    }

    /// Validate and apply a voucher to a channel.
    ///
    /// Validates: correct channel_id, valid signature, nonce > previous,
    /// amount ≤ locked, amount ≥ previous (cumulative).
    pub fn apply_voucher(&self, channel_id: &ChannelId, voucher: PaymentVoucher) -> Result<()> {
        let mut record = self.read_record(channel_id)?;
        if record.status == ChannelStatus::Closed {
            return Err(ChannelStoreError::AlreadyClosed(hex::encode(channel_id)));
        }

        // Verify signature against sender pubkey
        let sender_key = VerifyingKey::from_bytes(&record.channel.sender)
            .map_err(|_| PaymentChannelError::InvalidSignature)?;
        if !verify_voucher(&voucher, &sender_key) {
            return Err(PaymentChannelError::InvalidSignature.into());
        }

        // Apply voucher (validates channel_id match, nonce, amounts)
        record.channel.apply_voucher(&voucher)?;
        record.latest_voucher = Some(voucher);
        self.write_record(&record)
    }

    /// Close a channel and return final state for on-chain settlement.
    pub fn close_channel(&self, channel_id: &ChannelId) -> Result<PaymentChannel> {
        let mut record = self.read_record(channel_id)?;
        if record.status == ChannelStatus::Closed {
            return Err(ChannelStoreError::AlreadyClosed(hex::encode(channel_id)));
        }
        record.status = ChannelStatus::Closed;
        self.write_record(&record)?;
        Ok(record.channel)
    }

    /// Get a channel by ID.
    pub fn get_channel(&self, channel_id: &ChannelId) -> Option<PaymentChannel> {
        self.read_record(channel_id).ok().map(|r| r.channel)
    }

    /// Get a full channel record by ID.
    pub fn get_record(&self, channel_id: &ChannelId) -> Option<ChannelRecord> {
        self.read_record(channel_id).ok()
    }

    /// List all channels.
    pub fn list_channels(&self) -> Vec<PaymentChannel> {
        self.list_records().into_iter().map(|r| r.channel).collect()
    }

    /// List all channel records.
    fn list_records(&self) -> Vec<ChannelRecord> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else { return vec![] };
        let mut records = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("json") {
                if let Ok(data) = std::fs::read(&path) {
                    if let Ok(record) = serde_json::from_slice::<ChannelRecord>(&data) {
                        records.push(record);
                    }
                }
            }
        }
        records
    }

    /// List channels where the given pubkey is sender or receiver.
    pub fn list_channels_by_peer(&self, peer_pubkey: &[u8; 32]) -> Vec<PaymentChannel> {
        self.list_records()
            .into_iter()
            .filter(|r| &r.channel.sender == peer_pubkey || &r.channel.receiver == peer_pubkey)
            .map(|r| r.channel)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use datacraft_core::payment_channel::sign_voucher;
    use rand::rngs::OsRng;

    fn temp_dir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "channel-store-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn make_keypair() -> SigningKey {
        SigningKey::generate(&mut OsRng)
    }

    fn make_channel(sender_key: &SigningKey) -> PaymentChannel {
        let sender = sender_key.verifying_key().to_bytes();
        PaymentChannel::new([1u8; 32], sender, [3u8; 32], 1_000_000)
    }

    fn make_voucher(channel_id: ChannelId, amount: u64, nonce: u64, key: &SigningKey) -> PaymentVoucher {
        let mut v = PaymentVoucher {
            channel_id,
            cumulative_amount: amount,
            nonce,
            signature: vec![],
        };
        sign_voucher(&mut v, key);
        v
    }

    #[test]
    fn test_open_and_get() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let ch = make_channel(&key);
        let id = ch.channel_id;

        store.open_channel(ch.clone()).unwrap();
        let got = store.get_channel(&id).unwrap();
        assert_eq!(got.locked_amount, 1_000_000);
        assert_eq!(got.sender, ch.sender);

        // Cleanup
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_open_duplicate_rejected() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let ch = make_channel(&key);

        store.open_channel(ch.clone()).unwrap();
        assert!(store.open_channel(ch).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_voucher_sequence() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let ch = make_channel(&key);
        let id = ch.channel_id;

        store.open_channel(ch).unwrap();

        let v1 = make_voucher(id, 100_000, 1, &key);
        store.apply_voucher(&id, v1).unwrap();

        let got = store.get_channel(&id).unwrap();
        assert_eq!(got.spent, 100_000);
        assert_eq!(got.nonce, 1);

        let v2 = make_voucher(id, 300_000, 2, &key);
        store.apply_voucher(&id, v2).unwrap();

        let got = store.get_channel(&id).unwrap();
        assert_eq!(got.spent, 300_000);
        assert_eq!(got.nonce, 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_close_channel() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let ch = make_channel(&key);
        let id = ch.channel_id;

        store.open_channel(ch).unwrap();
        let v = make_voucher(id, 500_000, 1, &key);
        store.apply_voucher(&id, v).unwrap();

        let final_state = store.close_channel(&id).unwrap();
        assert_eq!(final_state.spent, 500_000);

        // Double close fails
        assert!(store.close_channel(&id).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_voucher_on_closed_channel() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let ch = make_channel(&key);
        let id = ch.channel_id;

        store.open_channel(ch).unwrap();
        store.close_channel(&id).unwrap();

        let v = make_voucher(id, 100, 1, &key);
        assert!(store.apply_voucher(&id, v).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_persistence_across_restarts() {
        let dir = temp_dir();
        let key = make_keypair();
        let id;

        {
            let store = ChannelStore::new(&dir).unwrap();
            let ch = make_channel(&key);
            id = ch.channel_id;
            store.open_channel(ch).unwrap();
            let v = make_voucher(id, 200_000, 1, &key);
            store.apply_voucher(&id, v).unwrap();
        }

        // Reopen
        {
            let store = ChannelStore::new(&dir).unwrap();
            let got = store.get_channel(&id).unwrap();
            assert_eq!(got.spent, 200_000);
            assert_eq!(got.nonce, 1);
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_invalid_voucher_wrong_signature() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let wrong_key = make_keypair();
        let ch = make_channel(&key);
        let id = ch.channel_id;

        store.open_channel(ch).unwrap();

        // Sign with wrong key
        let v = make_voucher(id, 100_000, 1, &wrong_key);
        assert!(store.apply_voucher(&id, v).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_invalid_voucher_stale_nonce() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let ch = make_channel(&key);
        let id = ch.channel_id;

        store.open_channel(ch).unwrap();
        store.apply_voucher(&id, make_voucher(id, 100_000, 5, &key)).unwrap();

        // Stale nonce
        let v = make_voucher(id, 200_000, 3, &key);
        assert!(store.apply_voucher(&id, v).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_invalid_voucher_overspend() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let ch = make_channel(&key);
        let id = ch.channel_id;

        store.open_channel(ch).unwrap();
        let v = make_voucher(id, 2_000_000, 1, &key); // > 1M locked
        assert!(store.apply_voucher(&id, v).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_invalid_voucher_decreasing_amount() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        let key = make_keypair();
        let ch = make_channel(&key);
        let id = ch.channel_id;

        store.open_channel(ch).unwrap();
        store.apply_voucher(&id, make_voucher(id, 500_000, 1, &key)).unwrap();

        let v = make_voucher(id, 300_000, 2, &key); // less than current
        assert!(store.apply_voucher(&id, v).is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_channels() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();

        let key1 = make_keypair();
        let key2 = make_keypair();
        let mut ch1 = make_channel(&key1);
        ch1.channel_id = [10u8; 32];
        let mut ch2 = make_channel(&key2);
        ch2.channel_id = [20u8; 32];

        store.open_channel(ch1).unwrap();
        store.open_channel(ch2).unwrap();

        assert_eq!(store.list_channels().len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_list_channels_by_peer() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();

        let key1 = make_keypair();
        let key2 = make_keypair();

        let mut ch1 = make_channel(&key1);
        ch1.channel_id = [10u8; 32];
        let mut ch2 = make_channel(&key2);
        ch2.channel_id = [20u8; 32];

        store.open_channel(ch1.clone()).unwrap();
        store.open_channel(ch2).unwrap();

        let by_sender = store.list_channels_by_peer(&ch1.sender);
        assert_eq!(by_sender.len(), 1);
        assert_eq!(by_sender[0].channel_id, [10u8; 32]);

        // By receiver (both have same receiver [3u8; 32])
        let by_receiver = store.list_channels_by_peer(&[3u8; 32]);
        assert_eq!(by_receiver.len(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_get_nonexistent_channel() {
        let dir = temp_dir();
        let store = ChannelStore::new(&dir).unwrap();
        assert!(store.get_channel(&[99u8; 32]).is_none());
        std::fs::remove_dir_all(&dir).ok();
    }
}
