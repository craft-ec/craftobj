//! DataCraft Routing
//!
//! DHT-based content provider discovery built on craftec-network's Kademlia.
//!
//! DHT keys:
//! - `/datacraft/providers/<cid_hex>` — provider records
//! - `/datacraft/manifest/<cid_hex>` — serialized ChunkManifest

use std::time::Duration;

use datacraft_core::{
    ChunkManifest, ContentId, DataCraftError, Result,
    MANIFEST_DHT_PREFIX, PROVIDERS_DHT_PREFIX,
};
use craftec_network::CraftBehaviour;
use libp2p::{kad, PeerId};
use tracing::debug;

/// TTL for content provider records (10 minutes).
pub const PROVIDER_RECORD_TTL: Duration = Duration::from_secs(600);

/// TTL for manifest records (30 minutes).
pub const MANIFEST_RECORD_TTL: Duration = Duration::from_secs(1800);

/// Build DHT key for content providers.
pub fn providers_dht_key(content_id: &ContentId) -> Vec<u8> {
    format!("{}{}", PROVIDERS_DHT_PREFIX, content_id.to_hex()).into_bytes()
}

/// Build DHT key for a content manifest.
pub fn manifest_dht_key(content_id: &ContentId) -> Vec<u8> {
    format!("{}{}", MANIFEST_DHT_PREFIX, content_id.to_hex()).into_bytes()
}

/// Content router — wraps CraftBehaviour's generic DHT methods
/// with DataCraft-specific key schemes.
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

    /// Publish a manifest to the DHT as a record.
    pub fn publish_manifest(
        behaviour: &mut CraftBehaviour,
        manifest: &ChunkManifest,
        local_peer_id: &PeerId,
    ) -> Result<kad::QueryId> {
        let key = manifest_dht_key(&manifest.content_id);
        let value = serde_json::to_vec(manifest)
            .map_err(|e| DataCraftError::ManifestError(e.to_string()))?;
        debug!(
            "Publishing manifest for {} ({} bytes)",
            manifest.content_id,
            value.len()
        );
        behaviour
            .put_record(&key, value, local_peer_id, Some(MANIFEST_RECORD_TTL))
            .map_err(|e| DataCraftError::NetworkError(format!("put_record: {:?}", e)))
    }

    /// Fetch a manifest from the DHT.
    pub fn get_manifest(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
    ) -> kad::QueryId {
        let key = manifest_dht_key(content_id);
        debug!("Fetching manifest for {}", content_id);
        behaviour.get_record(&key)
    }
}

/// Parse a manifest from a DHT record value.
pub fn parse_manifest_record(value: &[u8]) -> Result<ChunkManifest> {
    serde_json::from_slice(value)
        .map_err(|e| DataCraftError::ManifestError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_providers_dht_key() {
        let cid = ContentId::from_bytes(b"test");
        let key = providers_dht_key(&cid);
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.starts_with("/datacraft/providers/"));
        assert_eq!(key_str.len(), "/datacraft/providers/".len() + 64);
    }

    #[test]
    fn test_manifest_dht_key() {
        let cid = ContentId::from_bytes(b"test");
        let key = manifest_dht_key(&cid);
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.starts_with("/datacraft/manifest/"));
    }

    #[test]
    fn test_parse_manifest_record() {
        use datacraft_core::default_erasure_config;

        let manifest = ChunkManifest {
            content_id: ContentId::from_bytes(b"test"),
            total_size: 1024,
            chunk_count: 1,
            chunk_size: 65536,
            erasure_config: default_erasure_config(),
            encrypted: false,
            chunk_sizes: vec![1024],
        };
        let value = serde_json::to_vec(&manifest).unwrap();
        let parsed = parse_manifest_record(&value).unwrap();
        assert_eq!(parsed.content_id, manifest.content_id);
        assert_eq!(parsed.total_size, 1024);
    }
}
