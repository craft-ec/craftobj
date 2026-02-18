//! In-memory cache of content removal notices.
//!
//! Populated from DHT lookups.
//! Storage nodes check this before serving shards to avoid DHT lookup every time.

use std::collections::HashMap;

use datacraft_core::{ContentId, RemovalNotice};

/// In-memory cache of removed CIDs and their removal notices.
#[derive(Debug, Default)]
pub struct RemovalCache {
    /// Map of removed CID → RemovalNotice.
    removals: HashMap<ContentId, RemovalNotice>,
}

impl RemovalCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a removal notice into the cache.
    ///
    /// Only inserts if the notice signature is valid.
    pub fn insert(&mut self, notice: RemovalNotice) -> bool {
        if !notice.verify() {
            return false;
        }
        self.removals.insert(notice.cid, notice);
        true
    }

    /// Check if a CID has been removed.
    pub fn is_removed(&self, cid: &ContentId) -> bool {
        self.removals.contains_key(cid)
    }

    /// Get the removal notice for a CID, if any.
    pub fn get(&self, cid: &ContentId) -> Option<&RemovalNotice> {
        self.removals.get(cid)
    }

    /// Number of cached removal notices.
    pub fn len(&self) -> usize {
        self.removals.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.removals.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    #[test]
    fn test_removal_cache_insert_and_check() {
        let mut cache = RemovalCache::new();
        let keypair = SigningKey::generate(&mut rand::thread_rng());
        let cid = ContentId::from_bytes(b"cached removal");

        let notice = RemovalNotice::sign(&keypair, cid, None);
        assert!(cache.insert(notice));
        assert!(cache.is_removed(&cid));
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_removal_cache_invalid_notice_rejected() {
        let mut cache = RemovalCache::new();
        let notice = RemovalNotice {
            cid: ContentId::from_bytes(b"bad"),
            creator: "did:craftec:0000".into(),
            timestamp: 0,
            reason: None,
            signature: vec![1, 2, 3], // invalid
        };
        assert!(!cache.insert(notice));
        assert!(cache.is_empty());
    }

    #[test]
    fn test_removal_cache_not_removed() {
        let cache = RemovalCache::new();
        let cid = ContentId::from_bytes(b"not removed");
        assert!(!cache.is_removed(&cid));
        assert!(cache.get(&cid).is_none());
    }
}
