//! DataCraft Routing
//!
//! DHT-based content provider discovery built on craftec-network's Kademlia.
//!
//! DHT keys:
//! - `/datacraft/providers/<cid_hex>` — provider records
//! - `/datacraft/manifest/<cid_hex>` — serialized ContentManifest

use std::time::Duration;

use datacraft_core::{
    ContentManifest, ContentId, DataCraftError, RemovalNotice, Result,
    MANIFEST_DHT_PREFIX, PROVIDERS_DHT_PREFIX, ACCESS_DHT_PREFIX, REKEY_DHT_PREFIX,
    REMOVAL_DHT_PREFIX,
    access::AccessList,
    pre::ReKeyEntry,
};
use craftec_network::CraftBehaviour;
use libp2p::{kad, PeerId};
use tracing::debug;

/// TTL for content provider records (10 minutes).
pub const PROVIDER_RECORD_TTL: Duration = Duration::from_secs(600);

/// TTL for manifest records (30 minutes).
pub const MANIFEST_RECORD_TTL: Duration = Duration::from_secs(1800);

/// TTL for access metadata records (1 hour).
pub const ACCESS_RECORD_TTL: Duration = Duration::from_secs(3600);

/// TTL for removal notice records (24 hours — long-lived to ensure propagation).
pub const REMOVAL_RECORD_TTL: Duration = Duration::from_secs(86400);

/// Build DHT key for content providers.
pub fn providers_dht_key(content_id: &ContentId) -> Vec<u8> {
    format!("{}{}", PROVIDERS_DHT_PREFIX, content_id.to_hex()).into_bytes()
}

/// Build DHT key for a content manifest.
pub fn manifest_dht_key(content_id: &ContentId) -> Vec<u8> {
    format!("{}{}", MANIFEST_DHT_PREFIX, content_id.to_hex()).into_bytes()
}

/// Build DHT key for an access list.
pub fn access_dht_key(content_id: &ContentId) -> Vec<u8> {
    format!("{}{}", ACCESS_DHT_PREFIX, content_id.to_hex()).into_bytes()
}

/// Build DHT key for a re-encryption key entry.
pub fn rekey_dht_key(content_id: &ContentId, recipient_did: &[u8; 32]) -> Vec<u8> {
    format!("{}{}/{}", REKEY_DHT_PREFIX, content_id.to_hex(), hex::encode(recipient_did)).into_bytes()
}

/// Build DHT key for a removal notice.
pub fn removal_dht_key(content_id: &ContentId) -> Vec<u8> {
    format!("{}{}", REMOVAL_DHT_PREFIX, content_id.to_hex()).into_bytes()
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
        manifest: &ContentManifest,
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

    /// Publish an access list to the DHT.
    pub fn put_access_list(
        behaviour: &mut CraftBehaviour,
        access_list: &AccessList,
        local_peer_id: &libp2p::PeerId,
    ) -> Result<kad::QueryId> {
        let key = access_dht_key(&access_list.content_id);
        let value = bincode::serialize(access_list)
            .map_err(|e| DataCraftError::EncryptionError(format!("serialize access list: {e}")))?;
        debug!(
            "Publishing access list for {} ({} bytes, {} entries)",
            access_list.content_id,
            value.len(),
            access_list.entries.len(),
        );
        behaviour
            .put_record(&key, value, local_peer_id, Some(ACCESS_RECORD_TTL))
            .map_err(|e| DataCraftError::NetworkError(format!("put_record: {:?}", e)))
    }

    /// Fetch an access list from the DHT.
    pub fn get_access_list(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
    ) -> kad::QueryId {
        let key = access_dht_key(content_id);
        debug!("Fetching access list for {}", content_id);
        behaviour.get_record(&key)
    }

    /// Publish a re-encryption key entry to the DHT.
    pub fn put_re_key(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
        entry: &ReKeyEntry,
        local_peer_id: &libp2p::PeerId,
    ) -> Result<kad::QueryId> {
        let key = rekey_dht_key(content_id, &entry.recipient_did);
        let value = bincode::serialize(entry)
            .map_err(|e| DataCraftError::EncryptionError(format!("serialize re-key: {e}")))?;
        debug!(
            "Publishing re-key for {} → {} ({} bytes)",
            content_id,
            hex::encode(entry.recipient_did),
            value.len(),
        );
        behaviour
            .put_record(&key, value, local_peer_id, Some(ACCESS_RECORD_TTL))
            .map_err(|e| DataCraftError::NetworkError(format!("put_record: {:?}", e)))
    }

    /// Fetch a re-encryption key entry from the DHT.
    pub fn get_re_key(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
        recipient_did: &[u8; 32],
    ) -> kad::QueryId {
        let key = rekey_dht_key(content_id, recipient_did);
        debug!(
            "Fetching re-key for {} → {}",
            content_id,
            hex::encode(recipient_did),
        );
        behaviour.get_record(&key)
    }

    /// Store a removal notice in the DHT.
    pub fn put_removal_notice(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
        notice: &RemovalNotice,
        local_peer_id: &libp2p::PeerId,
    ) -> Result<kad::QueryId> {
        let key = removal_dht_key(content_id);
        let value = bincode::serialize(notice)
            .map_err(|e| DataCraftError::StorageError(format!("serialize removal notice: {e}")))?;
        debug!(
            "Publishing removal notice for {} ({} bytes)",
            content_id,
            value.len(),
        );
        behaviour
            .put_record(&key, value, local_peer_id, Some(REMOVAL_RECORD_TTL))
            .map_err(|e| DataCraftError::NetworkError(format!("put_record: {:?}", e)))
    }

    /// Fetch a removal notice from the DHT.
    pub fn get_removal_notice(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
    ) -> kad::QueryId {
        let key = removal_dht_key(content_id);
        debug!("Checking removal notice for {}", content_id);
        behaviour.get_record(&key)
    }

    /// Remove a re-encryption key entry from the DHT.
    ///
    /// Note: Kademlia doesn't natively support record removal. We overwrite
    /// the record with an empty value to effectively "tombstone" it.
    pub fn remove_re_key(
        behaviour: &mut CraftBehaviour,
        content_id: &ContentId,
        recipient_did: &[u8; 32],
        local_peer_id: &libp2p::PeerId,
    ) -> Result<kad::QueryId> {
        let key = rekey_dht_key(content_id, recipient_did);
        debug!(
            "Removing re-key for {} → {}",
            content_id,
            hex::encode(recipient_did),
        );
        // Overwrite with empty value (tombstone)
        behaviour
            .put_record(&key, vec![], local_peer_id, Some(Duration::from_secs(1)))
            .map_err(|e| DataCraftError::NetworkError(format!("put_record: {:?}", e)))
    }
}

/// Parse a manifest from a DHT record value.
pub fn parse_manifest_record(value: &[u8]) -> Result<ContentManifest> {
    serde_json::from_slice(value)
        .map_err(|e| DataCraftError::ManifestError(e.to_string()))
}

/// Parse an access list from a DHT record value.
pub fn parse_access_list_record(value: &[u8]) -> Result<AccessList> {
    bincode::deserialize(value)
        .map_err(|e| DataCraftError::EncryptionError(format!("deserialize access list: {e}")))
}

/// Parse a removal notice from a DHT record value.
pub fn parse_removal_record(value: &[u8]) -> Result<RemovalNotice> {
    if value.is_empty() {
        return Err(DataCraftError::StorageError("removal record is empty".into()));
    }
    bincode::deserialize(value)
        .map_err(|e| DataCraftError::StorageError(format!("deserialize removal notice: {e}")))
}

/// Parse a re-key entry from a DHT record value.
pub fn parse_rekey_record(value: &[u8]) -> Result<ReKeyEntry> {
    if value.is_empty() {
        return Err(DataCraftError::EncryptionError("re-key record is tombstoned".into()));
    }
    bincode::deserialize(value)
        .map_err(|e| DataCraftError::EncryptionError(format!("deserialize re-key: {e}")))
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
    fn test_access_dht_key() {
        let cid = ContentId::from_bytes(b"test");
        let key = access_dht_key(&cid);
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.starts_with("/datacraft/access/"));
        assert_eq!(key_str.len(), "/datacraft/access/".len() + 64);
    }

    #[test]
    fn test_rekey_dht_key() {
        let cid = ContentId::from_bytes(b"test");
        let recipient = [0xABu8; 32];
        let key = rekey_dht_key(&cid, &recipient);
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.starts_with("/datacraft/rekey/"));
        let parts: Vec<&str> = key_str.strip_prefix("/datacraft/rekey/").unwrap().split('/').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].len(), 64);
        assert_eq!(parts[1].len(), 64);
    }

    #[test]
    fn test_parse_access_list_record() {
        use datacraft_core::access::create_access_list;
        use ed25519_dalek::SigningKey;

        let creator = SigningKey::generate(&mut rand::thread_rng());
        let content_id = ContentId::from_bytes(b"access test");
        let content_key = b"0123456789abcdef0123456789abcdef";

        let list = create_access_list(&content_id, &creator, content_key, vec![]).unwrap();
        let bytes = bincode::serialize(&list).unwrap();
        let parsed = parse_access_list_record(&bytes).unwrap();
        assert_eq!(parsed.content_id, list.content_id);
        assert_eq!(parsed.creator_did, list.creator_did);
        parsed.verify().unwrap();
    }

    #[test]
    fn test_parse_rekey_record() {
        use datacraft_core::pre::{generate_re_key, ReKeyEntry};
        use ed25519_dalek::SigningKey;

        let creator = SigningKey::generate(&mut rand::thread_rng());
        let recipient = SigningKey::generate(&mut rand::thread_rng());
        let re_key = generate_re_key(&creator, &recipient.verifying_key()).unwrap();

        let entry = ReKeyEntry {
            recipient_did: recipient.verifying_key().to_bytes(),
            re_key,
        };
        let bytes = bincode::serialize(&entry).unwrap();
        let parsed = parse_rekey_record(&bytes).unwrap();
        assert_eq!(parsed.recipient_did, entry.recipient_did);
        assert_eq!(parsed.re_key.transform_key, entry.re_key.transform_key);
    }

    #[test]
    fn test_removal_dht_key() {
        let cid = ContentId::from_bytes(b"removal test");
        let key = removal_dht_key(&cid);
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.starts_with("/datacraft/removal/"));
        assert_eq!(key_str.len(), "/datacraft/removal/".len() + 64);
    }

    #[test]
    fn test_parse_removal_record() {
        use ed25519_dalek::SigningKey;

        let keypair = SigningKey::generate(&mut rand::thread_rng());
        let cid = ContentId::from_bytes(b"removal parse test");
        let notice = datacraft_core::RemovalNotice::sign(&keypair, cid, Some("test".into()));

        let bytes = bincode::serialize(&notice).unwrap();
        let parsed = parse_removal_record(&bytes).unwrap();
        assert_eq!(parsed.cid, cid);
        assert!(parsed.verify());
    }

    #[test]
    fn test_parse_removal_record_empty() {
        assert!(parse_removal_record(&[]).is_err());
    }

    #[test]
    fn test_parse_rekey_record_tombstone() {
        let result = parse_rekey_record(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_manifest_record() {
        let cid = ContentId::from_bytes(b"test");
        let manifest = ContentManifest {
            content_id: cid,
            content_hash: cid.0,
            segment_size: 10_485_760,
            piece_size: 102_400,
            segment_count: 1,
            total_size: 1024,
            creator: String::new(),
            signature: vec![],
        };
        let value = serde_json::to_vec(&manifest).unwrap();
        let parsed = parse_manifest_record(&value).unwrap();
        assert_eq!(parsed.content_id, manifest.content_id);
        assert_eq!(parsed.total_size, 1024);
    }
}
