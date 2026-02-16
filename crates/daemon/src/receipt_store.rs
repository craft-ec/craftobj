//! Persistent receipt storage
//!
//! Append-only binary file format: `[len:4 LE][bincode receipt_entry]`
//! Dedup by SHA-256 hash of key fields (using piece_id instead of shard_index).

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use datacraft_core::{ContentId, StorageReceipt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Receipt entry — only StorageReceipts remain (TransferReceipts removed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReceiptEntry {
    Storage(StorageReceipt),
}

/// Persistent, append-only receipt store with in-memory indices.
pub struct PersistentReceiptStore {
    #[allow(dead_code)]
    path: PathBuf,
    file: BufWriter<File>,
    storage_receipts: Vec<StorageReceipt>,
    entries: Vec<ReceiptEntry>,
    by_cid: HashMap<ContentId, Vec<usize>>,
    by_node: HashMap<[u8; 32], Vec<usize>>,
    seen: HashSet<[u8; 32]>,
}

impl PersistentReceiptStore {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let (entries, seen) = if path.exists() {
            Self::load_entries(&path)?
        } else {
            (Vec::new(), HashSet::new())
        };

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let mut store = PersistentReceiptStore {
            path,
            file: BufWriter::new(file),
            storage_receipts: Vec::new(),
            entries: Vec::new(),
            by_cid: HashMap::new(),
            by_node: HashMap::new(),
            seen,
        };

        for entry in entries {
            store.index_entry(entry);
        }

        Ok(store)
    }

    fn load_entries(path: &PathBuf) -> io::Result<(Vec<ReceiptEntry>, HashSet<[u8; 32]>)> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut seen = HashSet::new();

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            reader.read_exact(&mut payload)?;

            match bincode::deserialize::<ReceiptEntry>(&payload) {
                Ok(entry) => {
                    seen.insert(Self::dedup_hash(&entry));
                    entries.push(entry);
                }
                Err(e) => {
                    tracing::warn!("Skipping corrupt receipt entry: {}", e);
                }
            }
        }

        Ok((entries, seen))
    }

    /// Compute dedup hash — uses piece_id for identity.
    fn dedup_hash(entry: &ReceiptEntry) -> [u8; 32] {
        let mut hasher = Sha256::new();
        match entry {
            ReceiptEntry::Storage(r) => {
                hasher.update(r.content_id.0);
                hasher.update(r.segment_index.to_le_bytes());
                hasher.update(r.piece_id);
                hasher.update(r.storage_node);
                hasher.update(r.challenger);
                hasher.update(r.timestamp.to_le_bytes());
            }
        }
        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }

    fn index_entry(&mut self, entry: ReceiptEntry) {
        let idx = self.entries.len();
        match &entry {
            ReceiptEntry::Storage(r) => {
                self.storage_receipts.push(r.clone());
                self.by_cid.entry(r.content_id).or_default().push(idx);
                self.by_node.entry(r.storage_node).or_default().push(idx);
                self.by_node.entry(r.challenger).or_default().push(idx);
            }
        }
        self.entries.push(entry);
    }

    fn append_to_disk(&mut self, entry: &ReceiptEntry) -> io::Result<()> {
        let payload = bincode::serialize(entry)
            .map_err(io::Error::other)?;
        let len = (payload.len() as u32).to_le_bytes();
        self.file.write_all(&len)?;
        self.file.write_all(&payload)?;
        self.file.flush()?;
        Ok(())
    }

    pub fn add_storage(&mut self, receipt: StorageReceipt) -> io::Result<bool> {
        let entry = ReceiptEntry::Storage(receipt);
        let hash = Self::dedup_hash(&entry);
        if !self.seen.insert(hash) {
            return Ok(false);
        }
        self.append_to_disk(&entry)?;
        self.index_entry(entry);
        Ok(true)
    }

    pub fn query_by_cid(&self, cid: &ContentId) -> Vec<&ReceiptEntry> {
        self.by_cid
            .get(cid)
            .map(|indices| indices.iter().map(|&i| &self.entries[i]).collect())
            .unwrap_or_default()
    }

    pub fn query_by_node(&self, node: &[u8; 32]) -> Vec<&ReceiptEntry> {
        self.by_node
            .get(node)
            .map(|indices| indices.iter().map(|&i| &self.entries[i]).collect())
            .unwrap_or_default()
    }

    pub fn query_by_time_range(&self, from: u64, to: u64) -> Vec<&ReceiptEntry> {
        self.entries
            .iter()
            .filter(|e| {
                let ts = match e {
                    ReceiptEntry::Storage(r) => r.timestamp,
                };
                ts >= from && ts <= to
            })
            .collect()
    }

    pub fn storage_receipt_count(&self) -> usize {
        self.storage_receipts.len()
    }

    pub fn all_storage_receipts(&self) -> &[StorageReceipt] {
        &self.storage_receipts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_storage_receipt(cid: ContentId, seg: u32, piece_id: [u8; 32], ts: u64) -> StorageReceipt {
        StorageReceipt {
            content_id: cid,
            storage_node: [1u8; 32],
            challenger: [2u8; 32],
            segment_index: seg,
            piece_id,
            timestamp: ts,
            nonce: [3u8; 32],
            proof_hash: [4u8; 32],
            signature: vec![],
        }
    }

    fn temp_path() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "receipt-store-test-{}-{}.bin",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn write_read_roundtrip() {
        let path = temp_path();
        let cid = ContentId([10u8; 32]);

        {
            let mut store = PersistentReceiptStore::new(path.clone()).unwrap();
            assert!(store.add_storage(make_storage_receipt(cid, 0, [5u8; 32], 100)).unwrap());
            assert_eq!(store.storage_receipt_count(), 1);
        }

        {
            let store = PersistentReceiptStore::new(path.clone()).unwrap();
            assert_eq!(store.storage_receipt_count(), 1);
            assert_eq!(store.all_storage_receipts()[0].content_id, cid);
            assert_eq!(store.all_storage_receipts()[0].piece_id, [5u8; 32]);
        }

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn dedup_rejection() {
        let path = temp_path();
        let cid = ContentId([20u8; 32]);
        let receipt = make_storage_receipt(cid, 0, [5u8; 32], 100);

        let mut store = PersistentReceiptStore::new(path.clone()).unwrap();
        assert!(store.add_storage(receipt.clone()).unwrap());
        assert!(!store.add_storage(receipt).unwrap());
        assert_eq!(store.storage_receipt_count(), 1);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn query_by_cid() {
        let path = temp_path();
        let cid1 = ContentId([40u8; 32]);
        let cid2 = ContentId([41u8; 32]);

        let mut store = PersistentReceiptStore::new(path.clone()).unwrap();
        store.add_storage(make_storage_receipt(cid1, 0, [5u8; 32], 100)).unwrap();
        store.add_storage(make_storage_receipt(cid1, 1, [6u8; 32], 200)).unwrap();
        store.add_storage(make_storage_receipt(cid2, 0, [7u8; 32], 300)).unwrap();

        assert_eq!(store.query_by_cid(&cid1).len(), 2);
        assert_eq!(store.query_by_cid(&cid2).len(), 1);

        std::fs::remove_file(&path).ok();
    }
}
