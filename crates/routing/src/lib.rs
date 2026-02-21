//! CraftObj Routing
//!
//! DHT-based content provider discovery built on craftec-network's Kademlia.
//!
//! DHT keys:
//! - `/craftobj/providers/<cid_hex>` — provider records
//!
//! NOTE: DHT manifest records removed — piece headers are self-describing and
//! carry all metadata (content_id, total_size, segment_count, k, vtags_cid).
//! The local ContentManifest (FsStore) is auto-created from the first received
//! piece header.

use std::time::Duration;

use craftobj_core::{ContentId, PROVIDERS_DHT_PREFIX};
use craftec_network::CraftBehaviour;
use libp2p::kad;
use sha2::{Sha256, Digest};
use tracing::debug;

/// TTL for content provider records (10 minutes).
pub const PROVIDER_RECORD_TTL: Duration = Duration::from_secs(600);

/// Build DHT key for content providers.
pub fn providers_dht_key(content_id: &ContentId) -> Vec<u8> {
    format!("{}{}", PROVIDERS_DHT_PREFIX, content_id.to_hex()).into_bytes()
}

/// Build a deterministic DHT provider key for a specific CID + segment.
///
/// This is used for per-segment provider records so that fetchers can discover
/// which nodes hold pieces for a given segment.
///
/// Key = SHA-256("craftobj/provider/" || content_id_bytes || segment_index_be)
pub fn provider_key(content_id: &ContentId, segment_index: u32) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(b"craftobj/provider/");
    hasher.update(content_id.0);
    hasher.update(segment_index.to_be_bytes());
    hasher.finalize().to_vec()
}

/// Content router — wraps CraftBehaviour's generic DHT methods
/// with CraftObj-specific key schemes.
pub struct ContentRouter;

impl ContentRouter {
    /// Announce this node as a provider for the given CID.
    pub fn announce(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
    ) -> std::result::Result<kad::QueryId, kad::store::Error> {
        let key = providers_dht_key(content_id);
        debug!("Announcing as provider for {}", content_id);
        behaviour.start_providing(&key)
    }

    /// Query DHT for providers of the given CID.
    pub fn resolve(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
    ) -> kad::QueryId {
        let key = providers_dht_key(content_id);
        debug!("Resolving providers for {}", content_id);
        behaviour.get_providers(&key)
    }

    // NOTE: publish_manifest, get_record, put_access_list, get_access_list,
    // put_re_key, get_re_key, remove_re_key, put_removal_notice,
    // get_removal_notice, publish_verification_record, get_verification_record
    // all removed — manifest is derived from piece headers, rest is COM layer.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_providers_dht_key() {
        let cid = ContentId::from_bytes(b"test");
        let key = providers_dht_key(&cid);
        let key_str = String::from_utf8_lossy(&key);
        assert!(key_str.starts_with("/craftobj/providers/"));
        assert!(key_str.len() > 20);
    }

    #[test]
    fn test_provider_key_deterministic() {
        let cid = ContentId::from_bytes(b"test");
        let key1 = provider_key(&cid, 0);
        let key2 = provider_key(&cid, 0);
        assert_eq!(key1, key2);
        // Different segment → different key
        let key3 = provider_key(&cid, 1);
        assert_ne!(key1, key3);
    }
}
