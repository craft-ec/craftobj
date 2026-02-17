//! IPC request handler
//!
//! Implements craftec_ipc::IpcHandler for DataCraft daemon.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use craftec_ipc::server::IpcHandler;
use datacraft_client::DataCraftClient;
use datacraft_core::PublishOptions;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};

use crate::channel_store::ChannelStore;
use datacraft_transfer;
use crate::commands::DataCraftCommand;
use crate::config::DaemonConfig;
use crate::content_tracker::ContentTracker;
use crate::events::{DaemonEvent, EventSender};
use crate::protocol::DataCraftProtocol;
use crate::receipt_store::PersistentReceiptStore;
use crate::settlement::SolanaClient;

use datacraft_core::DataCraftCapability;
use ed25519_dalek;

use crate::peer_scorer::PeerScorer;

/// Shared peer scorer type alias.
type SharedPeerScorer = Arc<Mutex<PeerScorer>>;

/// DataCraft IPC handler wrapping a DataCraftClient and protocol.
pub struct DataCraftHandler {
    client: Arc<Mutex<DataCraftClient>>,
    _protocol: Option<Arc<DataCraftProtocol>>,
    command_tx: Option<mpsc::UnboundedSender<DataCraftCommand>>,
    peer_scorer: Option<SharedPeerScorer>,
    receipt_store: Option<Arc<Mutex<PersistentReceiptStore>>>,
    channel_store: Option<Arc<Mutex<ChannelStore>>>,
    settlement_client: Option<Arc<Mutex<SolanaClient>>>,
    content_tracker: Option<Arc<Mutex<ContentTracker>>>,
    own_capabilities: Vec<DataCraftCapability>,
    daemon_config: Option<Arc<Mutex<DaemonConfig>>>,
    data_dir: Option<std::path::PathBuf>,
    event_sender: Option<EventSender>,
    /// Node signing key for manifest signing on publish.
    node_signing_key: Option<ed25519_dalek::SigningKey>,
    /// Eviction manager for recording access on fetch.
    eviction_manager: Option<Arc<Mutex<crate::eviction::EvictionManager>>>,
    /// Storage Merkle tree for incremental updates on store operations.
    merkle_tree: Option<Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>>,
    /// Local peer ID for filtering self from provider lists.
    pub local_peer_id: Option<libp2p::PeerId>,
    /// Challenger manager for PDP — register CIDs after publish/store.
    challenger: Option<Arc<Mutex<crate::challenger::ChallengerManager>>>,
    /// Start time for uptime calculation.
    start_time: Instant,
}

impl DataCraftHandler {
    pub fn new(
        client: Arc<Mutex<DataCraftClient>>,
        protocol: Arc<DataCraftProtocol>,
        command_tx: mpsc::UnboundedSender<DataCraftCommand>,
        peer_scorer: SharedPeerScorer,
        receipt_store: Arc<Mutex<PersistentReceiptStore>>,
        channel_store: Arc<Mutex<ChannelStore>>,
    ) -> Self {
        Self { 
            client, 
            _protocol: Some(protocol),
            command_tx: Some(command_tx),
            peer_scorer: Some(peer_scorer),
            receipt_store: Some(receipt_store),
            channel_store: Some(channel_store),
            settlement_client: None,
            content_tracker: None,
            own_capabilities: Vec::new(),
            daemon_config: None,
            data_dir: None,
            event_sender: None,
            node_signing_key: None,
            eviction_manager: None,
            merkle_tree: None,
            challenger: None,
            local_peer_id: None,
            start_time: Instant::now(),
        }
    }

    pub fn set_local_peer_id(&mut self, peer_id: libp2p::PeerId) {
        self.local_peer_id = Some(peer_id);
    }

    pub fn set_merkle_tree(&mut self, tree: Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>) {
        self.merkle_tree = Some(tree);
    }

    pub fn set_challenger(&mut self, challenger: Arc<Mutex<crate::challenger::ChallengerManager>>) {
        self.challenger = Some(challenger);
    }

    pub fn set_eviction_manager(&mut self, mgr: Arc<Mutex<crate::eviction::EvictionManager>>) {
        self.eviction_manager = Some(mgr);
    }

    pub fn set_node_signing_key(&mut self, key: ed25519_dalek::SigningKey) {
        self.node_signing_key = Some(key);
    }

    /// Set the event sender for broadcasting daemon events to WS clients.
    pub fn set_event_sender(&mut self, sender: EventSender) {
        self.event_sender = Some(sender);
    }

    /// Set the daemon config for get/set-config IPC commands.
    pub fn set_daemon_config(&mut self, config: Arc<Mutex<DaemonConfig>>, data_dir: std::path::PathBuf) {
        self.daemon_config = Some(config);
        self.data_dir = Some(data_dir);
    }

    /// Set the settlement client for on-chain operations.
    pub fn set_settlement_client(&mut self, client: Arc<Mutex<SolanaClient>>) {
        self.settlement_client = Some(client);
    }

    /// Set the content tracker.
    pub fn set_content_tracker(&mut self, tracker: Arc<Mutex<ContentTracker>>) {
        self.content_tracker = Some(tracker);
    }

    /// Set the node's own capabilities (for reporting via `node.capabilities` RPC).
    pub fn set_own_capabilities(&mut self, caps: Vec<DataCraftCapability>) {
        self.own_capabilities = caps;
    }

    /// Create handler without protocol (for testing).
    pub fn new_without_protocol(client: Arc<Mutex<DataCraftClient>>) -> Self {
        Self { 
            client, 
            _protocol: None,
            command_tx: None,
            peer_scorer: None,
            receipt_store: None,
            channel_store: None,
            settlement_client: None,
            content_tracker: None,
            own_capabilities: Vec::new(),
            daemon_config: None,
            data_dir: None,
            event_sender: None,
            node_signing_key: None,
            eviction_manager: None,
            merkle_tree: None,
            challenger: None,
            local_peer_id: None,
            start_time: Instant::now(),
        }
    }

    async fn handle_publish(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let path = params
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or("missing 'path' param")?;
        let encrypted = params
            .get("encrypted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let options = PublishOptions {
            encrypted,
        };

        let file_name = std::path::Path::new(path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        // Run the blocking publish work (fs::read + erasure encode + shard storage)
        // on a dedicated thread to avoid freezing the tokio runtime / WebSocket.
        let client = self.client.clone();
        let path_buf = PathBuf::from(path);
        let signing_key = self.node_signing_key.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut client = client.blocking_lock();
            let result = if let Some(ref key) = signing_key {
                client
                    .publish_signed(&path_buf, &options, key)
                    .map_err(|e| e.to_string())?
            } else {
                client
                    .publish(&path_buf, &options)
                    .map_err(|e| e.to_string())?
            };
            Ok::<_, String>(result)
        })
        .await
        .map_err(|e| format!("publish task panicked: {}", e))??;
        let manifest = result.manifest.clone();
        let manifest_k = manifest.k();

        // Track in content tracker
        if let Some(ref tracker) = self.content_tracker {
            let mut t = tracker.lock().await;
            t.track_published(result.content_id, &manifest, file_name, encrypted);
        }

        // Register CID with challenger for PDP tracking
        if let Some(ref challenger) = self.challenger {
            let mut mgr = challenger.lock().await;
            mgr.register_cid(result.content_id, None);
        }

        // Announce to DHT after successful publish (Milestone 1)
        if let Some(ref command_tx) = self.command_tx {
            debug!("Announcing {} to DHT", result.content_id);
            
            let (reply_tx, reply_rx) = oneshot::channel();
            let command = DataCraftCommand::AnnounceProvider {
                content_id: result.content_id,
                manifest,
                reply_tx,
            };
            
            if let Err(e) = command_tx.send(command) {
                warn!("Failed to send announce command: {}", e);
            } else {
                // Wait for the announcement to complete
                match reply_rx.await {
                    Ok(Ok(())) => {
                        debug!("Successfully announced {} to DHT", result.content_id);
                        if let Some(ref tracker) = self.content_tracker {
                            let mut t = tracker.lock().await;
                            t.mark_announced(&result.content_id);
                        }
                    }
                    Ok(Err(e)) => {
                        warn!("Failed to announce {} to DHT: {}", result.content_id, e);
                    }
                    Err(e) => {
                        warn!("DHT announcement reply channel closed: {}", e);
                    }
                }
            }
        } else {
            debug!("Published {} (no DHT announcement - running without network)", result.content_id);
        }

        if let Some(ref tx) = self.event_sender {
            let _ = tx.send(DaemonEvent::ContentPublished {
                content_id: result.content_id.to_hex(),
                size: result.total_size,
                segments: result.segment_count,
                pieces_per_segment: manifest_k,
            });
        }

        // Emit content status so UI shows the full picture
        if let Some(ref tracker) = self.content_tracker {
            if let Some(ref tx) = self.event_sender {
                let t = tracker.lock().await;
                if let Some((state, summary)) = t.status_summary(&result.content_id) {
                    let _ = tx.send(DaemonEvent::ContentStatus {
                        content_id: result.content_id.to_hex(),
                        name: state.name,
                        size: state.size,
                        stage: state.stage.to_string(),
                        local_pieces: state.local_pieces,
                        remote_pieces: 0,
                        total_pieces: state.segment_count * state.k,
                        provider_count: state.provider_count,
                        summary,
                    });
                }
            }
        }

        // Trigger immediate distribution to push shards to storage peers
        if let Some(ref command_tx) = self.command_tx {
            let _ = command_tx.send(DataCraftCommand::TriggerDistribution);
        }

        let mut response = serde_json::json!({
            "cid": result.content_id.to_hex(),
            "size": result.total_size,
            "segments": result.segment_count as u64,
        });
        if let Some(key) = &result.encryption_key {
            response["key"] = Value::String(hex::encode(key));
        }
        Ok(response)
    }

    async fn handle_fetch(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params
            .get("cid")
            .and_then(|v| v.as_str())
            .ok_or("missing 'cid' param")?;
        let output = params
            .get("output")
            .and_then(|v| v.as_str())
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let mut p = std::env::temp_dir();
                p.push(format!("datacraft-{}", &cid_hex[..8.min(cid_hex.len())]));
                p
            });
        let key = params
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| hex::decode(s).unwrap_or_default());

        let cid =
            datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;

        // Try local-first fetch path: use locally cached manifest + peer scorer providers
        // This works even with few nodes where DHT routing tables are sparse
        let mut p2p_fetched = false;
        if let Some(ref command_tx) = self.command_tx {
            // Step 1: Try to get manifest locally (storage nodes have it from manifest push)
            let local_manifest = {
                let client = self.client.lock().await;
                client.store().get_manifest(&cid).ok()
            };

            if let Some(manifest) = local_manifest {
                info!("Found manifest locally for {}", cid);

                // Step 2: Get providers from peer scorer (peers that announced having pieces for this CID)
                let cid_hex_key = cid.to_hex();
                let mut providers = Vec::new();
                if let Some(ref scorer) = self.peer_scorer {
                    let s = scorer.lock().await;
                    for (peer_id, score) in s.iter() {
                        if let Some(&count) = score.piece_counts.get(&cid_hex_key) {
                            if count > 0 {
                                // Skip self — sending PieceSync to ourselves deadlocks
                                if self.local_peer_id.as_ref() == Some(peer_id) {
                                    info!("Skipping self ({}) as provider for {}", peer_id, cid);
                                    continue;
                                }
                                info!("Provider {} has {} pieces for {}", peer_id, count, cid);
                                providers.push(*peer_id);
                            }
                        }
                    }
                }

                if !providers.is_empty() {
                    info!("Found {} remote providers for {} via peer scorer", providers.len(), cid);
                    // Step 3: Fetch missing pieces from these providers
                    match self.fetch_missing_pieces_from_peers(&cid, &manifest, &providers, command_tx).await {
                        Ok(()) => {
                            info!("Successfully fetched pieces via local-first P2P path");
                            p2p_fetched = true;
                        }
                        Err(e) => {
                            info!("Local-first P2P fetch failed: {}, trying DHT fallback", e);
                        }
                    }
                } else {
                    info!("No local providers found for {} in peer scorer", cid);
                }
            }

            // DHT fallback: try DHT resolution if local-first path didn't succeed
            if !p2p_fetched {
                info!("Attempting DHT resolution for {}", cid);

                let (reply_tx, reply_rx) = oneshot::channel();
                let command = DataCraftCommand::ResolveProviders {
                    content_id: cid,
                    reply_tx,
                };

                if command_tx.send(command).is_ok() {
                    match reply_rx.await {
                        Ok(Ok(providers)) if !providers.is_empty() => {
                            info!("Found {} providers for {} via DHT", providers.len(), cid);

                            let (manifest_tx, manifest_rx) = oneshot::channel();
                            let command = DataCraftCommand::GetManifest {
                                content_id: cid,
                                reply_tx: manifest_tx,
                            };

                            if command_tx.send(command).is_ok() {
                                match manifest_rx.await {
                                    Ok(Ok(manifest)) => {
                                        debug!("Retrieved manifest for {} from DHT", cid);
                                        if let Err(e) = self.fetch_missing_pieces_from_peers(&cid, &manifest, &providers, command_tx).await {
                                            debug!("DHT P2P piece transfer failed: {}, falling back to local reconstruction", e);
                                        } else {
                                            info!("Successfully fetched pieces via DHT P2P path");
                                        }
                                    }
                                    Ok(Err(e)) => debug!("Failed to get manifest from DHT: {}", e),
                                    Err(e) => debug!("Manifest request channel error: {}", e),
                                }
                            }
                        }
                        Ok(Ok(_)) => debug!("No providers found for {} via DHT", cid),
                        Ok(Err(e)) => debug!("Provider resolution failed: {}", e),
                        Err(e) => debug!("Provider resolution channel error: {}", e),
                    }
                }
            }
        }

        // Fall back to local reconstruction (blocking I/O — run off the runtime)
        info!("Using local reconstruction for {}", cid);
        let client = self.client.clone();
        let output_clone = output.clone();
        tokio::task::spawn_blocking(move || {
            let client = client.blocking_lock();
            client
                .reconstruct(&cid, &output_clone, key.as_deref())
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("reconstruct task panicked: {}", e))??;

        // Record access for eviction LRU tracking
        if let Some(ref em) = self.eviction_manager {
            let mut mgr = em.lock().await;
            mgr.record_access(&cid);
        }

        Ok(serde_json::json!({
            "path": output.to_string_lossy(),
        }))
    }

    async fn handle_pin(&self, params: Option<Value>) -> Result<Value, String> {
        let cid = extract_cid(params)?;
        let mut client = self.client.lock().await;
        client.pin(&cid).map_err(|e| e.to_string())?;
        Ok(serde_json::json!({}))
    }

    async fn handle_unpin(&self, params: Option<Value>) -> Result<Value, String> {
        let cid = extract_cid(params)?;
        let mut client = self.client.lock().await;
        client.unpin(&cid).map_err(|e| e.to_string())?;
        Ok(serde_json::json!({}))
    }

    async fn handle_list(&self) -> Result<Value, String> {
        let client = self.client.lock().await;
        let items = client.list().map_err(|e| e.to_string())?;
        drop(client);

        // Merge with tracker data if available
        if let Some(ref tracker) = self.content_tracker {
            let t = tracker.lock().await;
            let mut result = Vec::new();
            for item in &items {
                // Get creator from manifest
                let creator = {
                    let c = self.client.lock().await;
                    c.store().get_manifest(&item.content_id)
                        .map(|m| m.creator.clone())
                        .unwrap_or_default()
                };
                let mut obj = serde_json::json!({
                    "content_id": item.content_id.to_hex(),
                    "total_size": item.total_size,
                    "segment_count": item.segment_count,
                    "pinned": item.pinned,
                    "creator": creator,
                });
                if let Some(state) = t.get(&item.content_id) {
                    obj["name"] = serde_json::json!(state.name);
                    obj["encrypted"] = serde_json::json!(state.encrypted);
                    obj["stage"] = serde_json::json!(state.stage.to_string());
                    obj["total_pieces"] = serde_json::json!(state.segment_count * state.k);
                    obj["local_pieces"] = serde_json::json!(state.local_pieces);
                    obj["remote_pieces"] = serde_json::json!(0);
                    obj["provider_count"] = serde_json::json!(state.provider_count);
                    obj["last_announced"] = serde_json::json!(state.last_announced);
                    obj["role"] = serde_json::json!(match state.role {
                        crate::content_tracker::ContentRole::Publisher => "publisher",
                        crate::content_tracker::ContentRole::StorageProvider => "storage_provider",
                    });
                }
                result.push(obj);
            }
            return Ok(serde_json::json!(result));
        }

        serde_json::to_value(items).map_err(|e| e.to_string())
    }

    async fn handle_status(&self) -> Result<Value, String> {
        let client = self.client.lock().await;
        let status = client.status().map_err(|e| e.to_string())?;
        serde_json::to_value(status).map_err(|e| e.to_string())
    }

    async fn handle_receipts_count(&self) -> Result<Value, String> {
        let store = self.receipt_store.as_ref().ok_or("receipt store not available")?;
        let store = store.lock().await;
        Ok(serde_json::json!({
            "storage": store.storage_receipt_count(),
        }))
    }

    async fn handle_receipts_query(&self, params: Option<Value>) -> Result<Value, String> {
        let store = self.receipt_store.as_ref().ok_or("receipt store not available")?;
        let store = store.lock().await;
        let params = params.unwrap_or(serde_json::json!({}));

        let entries: Vec<&crate::receipt_store::ReceiptEntry> = if let Some(cid_hex) = params.get("cid").and_then(|v| v.as_str()) {
            let cid = datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;
            store.query_by_cid(&cid)
        } else if let Some(node_hex) = params.get("node").and_then(|v| v.as_str()) {
            let bytes = hex::decode(node_hex).map_err(|e| e.to_string())?;
            if bytes.len() != 32 {
                return Err("node must be 32 bytes hex".into());
            }
            let mut node = [0u8; 32];
            node.copy_from_slice(&bytes);
            store.query_by_node(&node)
        } else if params.get("from").is_some() || params.get("to").is_some() {
            let from = params.get("from").and_then(|v| v.as_u64()).unwrap_or(0);
            let to = params.get("to").and_then(|v| v.as_u64()).unwrap_or(u64::MAX);
            store.query_by_time_range(from, to)
        } else {
            store.query_by_time_range(0, u64::MAX)
        };

        let results: Vec<Value> = entries.iter().map(|e| match e {
            crate::receipt_store::ReceiptEntry::Storage(r) => serde_json::json!({
                "type": "storage",
                "cid": r.content_id.to_hex(),
                "segment_index": r.segment_index,
                "piece_id": hex::encode(r.piece_id),
                "storage_node": hex::encode(r.storage_node),
                "challenger": hex::encode(r.challenger),
                "timestamp": r.timestamp,
            }),
        }).collect();

        Ok(serde_json::json!({ "receipts": results }))
    }

    async fn handle_storage_receipt_list(&self, params: Option<Value>) -> Result<Value, String> {
        let store = self.receipt_store.as_ref().ok_or("receipt store not available")?;
        let store = store.lock().await;
        let params = params.unwrap_or(serde_json::json!({}));

        let limit = params.get("limit").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
        let offset = params.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

        // Optional filters
        let receipts: Vec<&datacraft_core::StorageReceipt> =
            if let Some(cid_hex) = params.get("cid").and_then(|v| v.as_str()) {
                let cid = datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;
                store.query_by_cid(&cid).into_iter().filter_map(|e| match e {
                    crate::receipt_store::ReceiptEntry::Storage(r) => Some(r),
                    _ => None,
                }).collect()
            } else if let Some(node_hex) = params.get("node").and_then(|v| v.as_str()) {
                let node = parse_pubkey(node_hex)?;
                store.query_by_node(&node).into_iter().filter_map(|e| match e {
                    crate::receipt_store::ReceiptEntry::Storage(r) => Some(r),
                    _ => None,
                }).collect()
            } else {
                store.all_storage_receipts().iter().collect()
            };

        let total = receipts.len();
        let items: Vec<Value> = receipts.into_iter()
            .skip(offset)
            .take(limit)
            .map(|r| serde_json::json!({
                "cid": r.content_id.to_hex(),
                "storage_node": hex::encode(r.storage_node),
                "challenger": hex::encode(r.challenger),
                "segment_index": r.segment_index,
                "piece_id": hex::encode(r.piece_id),
                "timestamp": r.timestamp,
                "nonce": hex::encode(r.nonce),
                "proof_hash": hex::encode(r.proof_hash),
                "signed": !r.signature.is_empty(),
            }))
            .collect();

        Ok(serde_json::json!({
            "receipts": items,
            "total": total,
            "offset": offset,
            "limit": limit,
        }))
    }

    async fn handle_node_capabilities(&self) -> Result<Value, String> {
        let cap_strings: Vec<String> = self.own_capabilities.iter().map(|c| c.to_string()).collect();
        Ok(serde_json::json!({ "capabilities": cap_strings }))
    }

    async fn handle_peers(&self) -> Result<Value, String> {
        let scorer = match &self.peer_scorer {
            Some(ps) => ps.lock().await,
            None => return Ok(serde_json::json!({})),
        };
        let mut result = serde_json::Map::new();
        for (peer_id, peer_score) in scorer.iter() {
            let cap_strings: Vec<String> = peer_score.capabilities.iter().map(|c| c.to_string()).collect();
            result.insert(
                peer_id.to_string(),
                serde_json::json!({
                    "capabilities": cap_strings,
                    "score": peer_score.score(),
                    "avg_latency_ms": peer_score.avg_latency_ms,
                    "storage_committed_bytes": peer_score.storage_committed_bytes,
                    "storage_used_bytes": peer_score.storage_used_bytes,
                }),
            );
        }
        Ok(Value::Object(result))
    }

    async fn handle_network_storage(&self) -> Result<Value, String> {
        let scorer = match &self.peer_scorer {
            Some(ps) => ps.lock().await,
            None => return Ok(serde_json::json!({
                "total_committed": 0,
                "total_used": 0,
                "total_available": 0,
                "storage_node_count": 0
            })),
        };
        let summary = scorer.network_storage_summary();
        serde_json::to_value(summary).map_err(|e| e.to_string())
    }

    /// Extend a CID by generating new coded pieces via RLNC recombination.
    async fn handle_extend(&self, params: Option<Value>) -> Result<Value, String> {
        let cid = extract_cid(params)?;

        let manifest = {
            let client = self.client.lock().await;
            client.store().get_manifest(&cid).map_err(|e| e.to_string())?
        };

        // Generate new pieces via recombination for each segment
        let result = {
            let client = self.client.lock().await;
            crate::health::heal_content(client.store(), &manifest, 1)
        };

        if result.pieces_generated == 0 {
            return Err(format!("failed to generate new pieces: {:?}", result.errors));
        }

        // Announce as provider
        if let Some(ref command_tx) = self.command_tx {
            let (tx, rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::AnnounceProvider {
                content_id: cid,
                manifest,
                reply_tx: tx,
            }).map_err(|e| e.to_string())?;
            let _ = rx.await;
        }

        debug!("Extended {} with {} new pieces", cid, result.pieces_generated);

        Ok(serde_json::json!({
            "cid": cid.to_hex(),
            "pieces_generated": result.pieces_generated,
        }))
    }

    /// Fetch missing pieces from remote peers using parallel P2P transfer.
    ///
    /// Design: opens concurrent piece requests (up to min(needed, providers, 20)),
    /// checks linear independence of coefficient vectors before storing,
    /// and discards dependent pieces.
    async fn fetch_missing_pieces_from_peers(
        &self,
        content_id: &datacraft_core::ContentId,
        manifest: &datacraft_core::ContentManifest,
        providers: &[libp2p::PeerId],
        command_tx: &tokio::sync::mpsc::UnboundedSender<DataCraftCommand>,
    ) -> Result<(), String> {
        use std::collections::HashMap;
        use tokio::task::JoinSet;

        const MAX_CONCURRENT: usize = 20;

        info!("Fetching pieces for {} from {} providers", content_id, providers.len());

        let ranked_providers = if let Some(ref scorer) = self.peer_scorer {
            let mut s = scorer.lock().await;
            s.rank_peers(providers)
        } else {
            providers.to_vec()
        };

        // For each segment, collect existing coefficient vectors and fetch missing pieces in parallel
        for seg_idx in 0..manifest.segment_count as u32 {
            let k = manifest.k_for_segment(seg_idx as usize);

            // Load existing pieces' coefficient vectors for independence checking
            let (_local_pieces, mut coeff_matrix) = {
                let client = self.client.lock().await;
                let piece_ids = client.store().list_pieces(content_id, seg_idx).unwrap_or_default();
                let mut coeffs = Vec::with_capacity(piece_ids.len());
                for pid in &piece_ids {
                    if let Ok((_data, coeff)) = client.store().get_piece(content_id, seg_idx, pid) {
                        coeffs.push(coeff);
                    }
                }
                (piece_ids.len(), coeffs)
            };

            let mut current_rank = if coeff_matrix.is_empty() {
                0
            } else {
                craftec_erasure::check_independence(&coeff_matrix)
            };

            if current_rank >= k {
                continue; // Already have k independent pieces
            }

            let needed = k - current_rank;
            info!("Segment {} needs {} more independent pieces (have rank {}/{})", seg_idx, needed, current_rank, k);

            // Spawn parallel piece requests using JoinSet
            let concurrency = needed.min(ranked_providers.len()).min(MAX_CONCURRENT);
            let mut join_set: JoinSet<(
                libp2p::PeerId,
                std::result::Result<(Vec<u8>, Vec<u8>), String>,
                std::time::Duration,
            )> = JoinSet::new();

            let mut provider_iter = ranked_providers.iter().copied().cycle();
            let mut requests_launched = 0usize;
            let mut total_fetched = 0usize;
            // Track per-provider consecutive failures to avoid hammering bad peers
            let mut provider_failures: HashMap<libp2p::PeerId, u32> = HashMap::new();

            // Launch initial batch
            for _ in 0..concurrency {
                let provider = provider_iter.next().unwrap();
                let cmd_tx = command_tx.clone();
                let cid = *content_id;
                join_set.spawn(async move {
                    let start = std::time::Instant::now();
                    let (reply_tx, reply_rx) = oneshot::channel::<Result<datacraft_transfer::DataCraftResponse, String>>();
                    let command = DataCraftCommand::PieceSync {
                        peer_id: provider,
                        content_id: cid,
                        segment_index: seg_idx,
                        merkle_root: [0u8; 32],
                        have_pieces: vec![],
                        max_pieces: 1,
                        reply_tx,
                    };
                    if cmd_tx.send(command).is_err() {
                        return (provider, Err::<(Vec<u8>, Vec<u8>), String>("command channel closed".into()), start.elapsed());
                    }
                    match reply_rx.await {
                        Ok(Ok(datacraft_transfer::DataCraftResponse::PieceBatch { pieces })) => {
                            if let Some(piece) = pieces.into_iter().next() {
                                (provider, Ok((piece.coefficients, piece.data)), start.elapsed())
                            } else {
                                (provider, Err("no pieces returned".into()), start.elapsed())
                            }
                        }
                        Ok(Ok(_)) => (provider, Err("unexpected response type".into()), start.elapsed()),
                        Ok(Err(e)) => (provider, Err(e), start.elapsed()),
                        Err(e) => (provider, Err(e.to_string()), start.elapsed()),
                    }
                });
                requests_launched += 1;
            }

            // Process results as they complete, spawn replacements
            while let Some(result) = join_set.join_next().await {
                let (provider, piece_result, latency) = match result {
                    Ok(r) => r,
                    Err(_) => continue, // task panicked
                };

                match piece_result {
                    Ok((coefficients, piece_data)) => {
                        // Check linear independence before storing
                        let mut test_matrix = coeff_matrix.clone();
                        test_matrix.push(coefficients.clone());
                        let new_rank = craftec_erasure::check_independence(&test_matrix);

                        if new_rank > current_rank {
                            // Independent piece — store it
                            let piece_id = datacraft_store::piece_id_from_coefficients(&coefficients);

                            if let Some(ref scorer) = self.peer_scorer {
                                scorer.lock().await.record_success(&provider, latency);
                            }

                            let stored = {
                                let client = self.client.lock().await;
                                client.store().store_piece(content_id, seg_idx, &piece_id, &piece_data, &coefficients)
                            };

                            match stored {
                                Ok(()) => {
                                    if let Some(ref mt) = self.merkle_tree {
                                        mt.lock().await.insert(content_id, seg_idx, &piece_id);
                                    }
                                    coeff_matrix.push(coefficients);
                                    current_rank = new_rank;
                                    total_fetched += 1;

                                    if current_rank >= k {
                                        info!("Segment {} complete: rank {}/{}", seg_idx, current_rank, k);
                                        break; // segment done
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to store piece: {}", e);
                                }
                            }
                        } else {
                            // Dependent piece — discard, record success anyway (peer was fine)
                            info!("Discarded dependent piece for segment {} from {}", seg_idx, provider);
                            if let Some(ref scorer) = self.peer_scorer {
                                scorer.lock().await.record_success(&provider, latency);
                            }
                        }
                    }
                    Err(ref e) => {
                        info!("Failed to get piece from {}: {}", provider, e);
                        let failures = provider_failures.entry(provider).or_insert(0);
                        *failures += 1;
                        if let Some(ref scorer) = self.peer_scorer {
                            scorer.lock().await.record_failure(&provider);
                        }
                    }
                }

                // Spawn a replacement request if we still need more pieces
                if current_rank < k {
                    // Pick next provider, skip those with too many failures
                    let mut attempts = 0;
                    while attempts < ranked_providers.len() {
                        let next_provider = provider_iter.next().unwrap();
                        let fail_count = provider_failures.get(&next_provider).copied().unwrap_or(0);
                        if fail_count < 3 {
                            let cmd_tx = command_tx.clone();
                            let cid = *content_id;
                            join_set.spawn(async move {
                                let start = std::time::Instant::now();
                                let (reply_tx, reply_rx) = oneshot::channel::<Result<datacraft_transfer::DataCraftResponse, String>>();
                                let command = DataCraftCommand::PieceSync {
                                    peer_id: next_provider,
                                    content_id: cid,
                                    segment_index: seg_idx,
                                    merkle_root: [0u8; 32],
                                    have_pieces: vec![],
                                    max_pieces: 1,
                                    reply_tx,
                                };
                                if cmd_tx.send(command).is_err() {
                                    return (next_provider, Err::<(Vec<u8>, Vec<u8>), String>("command channel closed".into()), start.elapsed());
                                }
                                match reply_rx.await {
                                    Ok(Ok(datacraft_transfer::DataCraftResponse::PieceBatch { pieces })) => {
                                        if let Some(piece) = pieces.into_iter().next() {
                                            (next_provider, Ok((piece.coefficients, piece.data)), start.elapsed())
                                        } else {
                                            (next_provider, Err("no pieces returned".into()), start.elapsed())
                                        }
                                    }
                                    Ok(Ok(_)) => (next_provider, Err("unexpected response type".into()), start.elapsed()),
                                    Ok(Err(e)) => (next_provider, Err(e), start.elapsed()),
                                    Err(e) => (next_provider, Err(e.to_string()), start.elapsed()),
                                }
                            });
                            requests_launched += 1;
                            break;
                        }
                        attempts += 1;
                    }
                }
            }

            if current_rank < k {
                warn!(
                    "Segment {} incomplete: got {}/{} independent pieces after {} requests",
                    seg_idx, current_rank, k, requests_launched
                );
            }
        }

        Ok(())
    }

    // -- Access control IPC handlers --

    async fn handle_access_grant(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params.get("cid").and_then(|v| v.as_str()).ok_or("missing 'cid'")?;
        let creator_secret_hex = params.get("creator_secret").and_then(|v| v.as_str()).ok_or("missing 'creator_secret'")?;
        let recipient_pubkey_hex = params.get("recipient_pubkey").and_then(|v| v.as_str()).ok_or("missing 'recipient_pubkey'")?;
        let content_key_hex = params.get("content_key").and_then(|v| v.as_str()).ok_or("missing 'content_key'")?;

        let content_id = datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;
        let creator_bytes = hex::decode(creator_secret_hex).map_err(|e| e.to_string())?;
        if creator_bytes.len() != 32 { return Err("creator_secret must be 32 bytes hex".into()); }
        let creator_key = ed25519_dalek::SigningKey::from_bytes(
            creator_bytes.as_slice().try_into().unwrap()
        );
        let recipient_bytes = parse_pubkey(recipient_pubkey_hex)?;
        let recipient_pubkey = ed25519_dalek::VerifyingKey::from_bytes(&recipient_bytes)
            .map_err(|e| format!("invalid recipient pubkey: {e}"))?;
        let content_key = hex::decode(content_key_hex).map_err(|e| e.to_string())?;

        // Generate re-key entry
        let re_key = datacraft_core::pre::generate_re_key(&creator_key, &recipient_pubkey)
            .map_err(|e| e.to_string())?;
        let entry = datacraft_core::pre::ReKeyEntry {
            recipient_did: recipient_bytes,
            re_key,
        };

        // Also generate the re-encrypted key for the recipient
        let re_encrypted = datacraft_core::pre::re_encrypt_with_content_key(&content_key, &entry.re_key)
            .map_err(|e| e.to_string())?;

        // Store re-key in DHT
        if let Some(ref command_tx) = self.command_tx {
            let (reply_tx, reply_rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::PutReKey {
                content_id,
                entry: entry.clone(),
                reply_tx,
            }).map_err(|e| e.to_string())?;
            reply_rx.await.map_err(|e| e.to_string())??;
        }

        if let Some(ref tx) = self.event_sender {
            let _ = tx.send(DaemonEvent::AccessGranted {
                content_id: cid_hex.to_string(),
                recipient: recipient_pubkey_hex.to_string(),
            });
        }

        Ok(serde_json::json!({
            "cid": cid_hex,
            "recipient": recipient_pubkey_hex,
            "re_encrypted_key": {
                "ephemeral_public": hex::encode(re_encrypted.ephemeral_public),
                "nonce": hex::encode(re_encrypted.nonce),
                "ciphertext": hex::encode(&re_encrypted.ciphertext),
            },
        }))
    }

    async fn handle_access_revoke(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params.get("cid").and_then(|v| v.as_str()).ok_or("missing 'cid'")?;
        let recipient_pubkey_hex = params.get("recipient_pubkey").and_then(|v| v.as_str()).ok_or("missing 'recipient_pubkey'")?;

        let content_id = datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;
        let recipient_did = parse_pubkey(recipient_pubkey_hex)?;

        if let Some(ref command_tx) = self.command_tx {
            let (reply_tx, reply_rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::RemoveReKey {
                content_id,
                recipient_did,
                reply_tx,
            }).map_err(|e| e.to_string())?;
            reply_rx.await.map_err(|e| e.to_string())??;
        }

        if let Some(ref tx) = self.event_sender {
            let _ = tx.send(DaemonEvent::AccessRevoked {
                content_id: cid_hex.to_string(),
                recipient: recipient_pubkey_hex.to_string(),
            });
        }

        Ok(serde_json::json!({
            "cid": cid_hex,
            "recipient": recipient_pubkey_hex,
            "revoked": true,
        }))
    }

    async fn handle_access_list(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params.get("cid").and_then(|v| v.as_str()).ok_or("missing 'cid'")?;

        let content_id = datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;

        if let Some(ref command_tx) = self.command_tx {
            let (reply_tx, reply_rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::GetAccessList {
                content_id,
                reply_tx,
            }).map_err(|e| e.to_string())?;

            match reply_rx.await {
                Ok(Ok(access_list)) => {
                    let dids: Vec<String> = access_list.entries.iter()
                        .map(|e| hex::encode(e.recipient_did))
                        .collect();
                    Ok(serde_json::json!({
                        "cid": cid_hex,
                        "creator": hex::encode(access_list.creator_did),
                        "authorized": dids,
                    }))
                }
                Ok(Err(e)) => Err(format!("DHT lookup failed: {e}")),
                Err(e) => Err(format!("channel error: {e}")),
            }
        } else {
            Err("no network available".into())
        }
    }

    /// Revoke access with key rotation: revoke user, rotate content key, re-grant remaining users.
    async fn handle_access_revoke_rotate(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params.get("cid").and_then(|v| v.as_str()).ok_or("missing 'cid'")?;
        let creator_secret_hex = params.get("creator_secret").and_then(|v| v.as_str()).ok_or("missing 'creator_secret'")?;
        let recipient_pubkey_hex = params.get("recipient_pubkey").and_then(|v| v.as_str()).ok_or("missing 'recipient_pubkey'")?;
        let content_key_hex = params.get("content_key").and_then(|v| v.as_str()).ok_or("missing 'content_key'")?;

        // Parse authorized users list
        let authorized_arr = params.get("authorized").and_then(|v| v.as_array()).ok_or("missing 'authorized' array")?;
        let mut all_authorized = Vec::new();
        for val in authorized_arr {
            let hex_str = val.as_str().ok_or("authorized entry must be hex string")?;
            let bytes = parse_pubkey(hex_str)?;
            let vk = ed25519_dalek::VerifyingKey::from_bytes(&bytes)
                .map_err(|e| format!("invalid authorized pubkey: {e}"))?;
            all_authorized.push(vk);
        }

        let content_id = datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;
        let creator_bytes = hex::decode(creator_secret_hex).map_err(|e| e.to_string())?;
        if creator_bytes.len() != 32 { return Err("creator_secret must be 32 bytes hex".into()); }
        let creator_key = ed25519_dalek::SigningKey::from_bytes(
            creator_bytes.as_slice().try_into().unwrap()
        );
        let revoked_bytes = parse_pubkey(recipient_pubkey_hex)?;
        let revoked_pubkey = ed25519_dalek::VerifyingKey::from_bytes(&revoked_bytes)
            .map_err(|e| format!("invalid recipient pubkey: {e}"))?;
        let content_key = hex::decode(content_key_hex).map_err(|e| e.to_string())?;

        // 1. Revoke: tombstone the old re-key
        if let Some(ref command_tx) = self.command_tx {
            let (reply_tx, reply_rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::RemoveReKey {
                content_id,
                recipient_did: revoked_bytes,
                reply_tx,
            }).map_err(|e| e.to_string())?;
            reply_rx.await.map_err(|e| e.to_string())??;
        }

        // 2. Rotate key + re-encrypt content + re-grant remaining users
        let revocation = {
            let mut client = self.client.lock().await;
            client.revoke_and_rotate(
                &content_id,
                &content_key,
                &creator_key,
                &revoked_pubkey,
                &all_authorized,
            ).map_err(|e| e.to_string())?
        };

        // 3. Store new re-keys in DHT and announce new CID
        if let Some(ref command_tx) = self.command_tx {
            // Store re-keys for remaining users
            for (entry, _re_enc) in &revocation.re_grants {
                let (reply_tx, reply_rx) = oneshot::channel();
                command_tx.send(DataCraftCommand::PutReKey {
                    content_id: revocation.new_content_id,
                    entry: entry.clone(),
                    reply_tx,
                }).map_err(|e| e.to_string())?;
                reply_rx.await.map_err(|e| e.to_string())??;
            }

            // Announce new CID as provider
            let manifest = {
                let client = self.client.lock().await;
                client.store().get_manifest(&revocation.new_content_id)
                    .map_err(|e| e.to_string())?
            };
            let (reply_tx, reply_rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::AnnounceProvider {
                content_id: revocation.new_content_id,
                manifest,
                reply_tx,
            }).map_err(|e| e.to_string())?;
            let _ = reply_rx.await;
        }

        // Build response with re-grant info
        let re_grants_json: Vec<Value> = revocation.re_grants.iter().map(|(entry, re_enc)| {
            serde_json::json!({
                "recipient": hex::encode(entry.recipient_did),
                "re_encrypted_key": {
                    "ephemeral_public": hex::encode(re_enc.ephemeral_public),
                    "nonce": hex::encode(re_enc.nonce),
                    "ciphertext": hex::encode(&re_enc.ciphertext),
                },
            })
        }).collect();

        Ok(serde_json::json!({
            "old_cid": cid_hex,
            "new_cid": revocation.new_content_id.to_hex(),
            "new_key": hex::encode(&revocation.new_encryption_key),
            "new_size": revocation.new_total_size,
            "new_segments": revocation.new_segment_count as u64,
            "revoked": recipient_pubkey_hex,
            "re_grants": re_grants_json,
        }))
    }

    // -- Content removal IPC handler --

    async fn handle_data_providers(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params.get("cid").and_then(|v| v.as_str()).ok_or("missing 'cid'")?;
        let cid_bytes = hex::decode(cid_hex).map_err(|e| format!("invalid hex: {e}"))?;
        if cid_bytes.len() != 32 { return Err("CID must be 32 bytes".into()); }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&cid_bytes);
        let cid = datacraft_core::ContentId(arr);

        let command_tx = self.command_tx.as_ref().ok_or("no command channel")?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        command_tx.send(DataCraftCommand::ResolveProviders { content_id: cid, reply_tx: tx })
            .map_err(|e| format!("send error: {e}"))?;
        let providers = rx.await.map_err(|e| format!("recv error: {e}"))??;
        Ok(serde_json::json!({
            "cid": cid_hex,
            "provider_count": providers.len(),
            "providers": providers.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
        }))
    }

    /// Delete content from local store only (no network propagation).
    /// For client-side content the user published or fetched — safe to remove.
    async fn handle_data_delete_local(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params.get("cid").and_then(|v| v.as_str()).ok_or("missing 'cid'")?;
        let content_id = datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;

        // Delete pieces + manifest from local store
        {
            let client = self.client.lock().await;
            client.store().delete_content(&content_id).map_err(|e| e.to_string())?;
        }

        // Remove from content tracker
        if let Some(ref tracker) = self.content_tracker {
            let mut t = tracker.lock().await;
            t.remove(&content_id);
        }

        info!("Deleted local content {}", content_id);
        Ok(serde_json::json!({ "deleted": true }))
    }

    async fn handle_data_remove(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params.get("cid").and_then(|v| v.as_str()).ok_or("missing 'cid'")?;
        let creator_secret_hex = params.get("creator_secret").and_then(|v| v.as_str())
            .ok_or("missing 'creator_secret'")?;
        let reason = params.get("reason").and_then(|v| v.as_str()).map(String::from);

        let content_id = datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;
        let creator_bytes = hex::decode(creator_secret_hex).map_err(|e| e.to_string())?;
        if creator_bytes.len() != 32 { return Err("creator_secret must be 32 bytes hex".into()); }
        let creator_key = ed25519_dalek::SigningKey::from_bytes(
            creator_bytes.as_slice().try_into().unwrap()
        );

        // Verify creator matches manifest (if we have it locally)
        {
            let client = self.client.lock().await;
            if let Ok(manifest) = client.store().get_manifest(&content_id) {
                if !manifest.creator.is_empty() {
                    let expected_did = datacraft_core::did_from_pubkey(&creator_key.verifying_key());
                    if manifest.creator != expected_did {
                        return Err("creator key does not match manifest creator".into());
                    }
                }
            }
        }

        // Create removal notice via client
        let notice = {
            let mut client = self.client.lock().await;
            client.remove_content(&creator_key, &content_id, reason)
                .map_err(|e| e.to_string())?
        };

        // Publish to DHT + gossipsub
        if let Some(ref command_tx) = self.command_tx {
            let (reply_tx, reply_rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::PublishRemoval {
                content_id,
                notice: notice.clone(),
                reply_tx,
            }).map_err(|e| e.to_string())?;

            match reply_rx.await {
                Ok(Ok(())) => {
                    debug!("Successfully published removal notice for {}", content_id);
                }
                Ok(Err(e)) => {
                    warn!("Failed to publish removal notice: {}", e);
                }
                Err(e) => {
                    warn!("Removal notice channel closed: {}", e);
                }
            }
        }

        if let Some(ref tx) = self.event_sender {
            let _ = tx.send(DaemonEvent::RemovalPublished {
                content_id: cid_hex.to_string(),
            });
        }

        Ok(serde_json::json!({
            "cid": cid_hex,
            "removed": true,
            "creator": notice.creator,
            "timestamp": notice.timestamp,
        }))
    }

    // -- Payment channel IPC handlers --

    async fn handle_channel_open(&self, params: Option<Value>) -> Result<Value, String> {
        let store = self.channel_store.as_ref().ok_or("channel store not available")?;
        let params = params.ok_or("missing params")?;
        let sender_hex = params.get("sender").and_then(|v| v.as_str()).ok_or("missing 'sender'")?;
        let receiver_hex = params.get("receiver").and_then(|v| v.as_str()).ok_or("missing 'receiver'")?;
        let amount = params.get("amount").and_then(|v| v.as_u64()).ok_or("missing 'amount'")?;

        let sender = parse_pubkey(sender_hex)?;
        let receiver = parse_pubkey(receiver_hex)?;

        // Generate channel ID from hash of (sender, receiver, timestamp)
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut id_data = Vec::new();
        id_data.extend_from_slice(&sender);
        id_data.extend_from_slice(&receiver);
        id_data.extend_from_slice(&timestamp.to_le_bytes());
        let channel_id: [u8; 32] = {
            use sha2::{Digest, Sha256};
            let hash = Sha256::digest(&id_data);
            let mut id = [0u8; 32];
            id.copy_from_slice(&hash);
            id
        };

        let channel = datacraft_core::payment_channel::PaymentChannel::new(
            channel_id, sender, receiver, amount,
        );

        // Persist via ChannelStore
        let cs = store.lock().await;
        cs.open_channel(channel.clone()).map_err(|e| e.to_string())?;

        debug!("Opened payment channel {}", hex::encode(channel_id));

        if let Some(ref tx) = self.event_sender {
            let _ = tx.send(DaemonEvent::ChannelOpened {
                channel_id: hex::encode(channel.channel_id),
                receiver: hex::encode(channel.receiver),
                amount: channel.locked_amount,
            });
        }

        Ok(serde_json::json!({
            "channel_id": hex::encode(channel.channel_id),
            "sender": hex::encode(channel.sender),
            "receiver": hex::encode(channel.receiver),
            "locked_amount": channel.locked_amount,
        }))
    }

    async fn handle_channel_voucher(&self, params: Option<Value>) -> Result<Value, String> {
        let store = self.channel_store.as_ref().ok_or("channel store not available")?;
        let params = params.ok_or("missing params")?;
        let channel_id_hex = params.get("channel_id").and_then(|v| v.as_str()).ok_or("missing 'channel_id'")?;
        let amount = params.get("amount").and_then(|v| v.as_u64()).ok_or("missing 'amount'")?;
        let nonce = params.get("nonce").and_then(|v| v.as_u64()).ok_or("missing 'nonce'")?;
        let signature_hex = params.get("signature").and_then(|v| v.as_str()).unwrap_or("");

        let channel_id = parse_pubkey(channel_id_hex)?;
        let signature = if signature_hex.is_empty() {
            vec![]
        } else {
            hex::decode(signature_hex).map_err(|e| e.to_string())?
        };

        let voucher = datacraft_core::payment_channel::PaymentVoucher {
            channel_id,
            cumulative_amount: amount,
            nonce,
            signature,
        };

        // Validate + persist via ChannelStore (includes sig verification)
        let cs = store.lock().await;
        cs.apply_voucher(&channel_id, voucher.clone()).map_err(|e| e.to_string())?;

        debug!("Applied voucher for channel {} amount={} nonce={}", hex::encode(channel_id), amount, nonce);

        Ok(serde_json::json!({
            "channel_id": hex::encode(channel_id),
            "cumulative_amount": amount,
            "nonce": nonce,
            "valid": true,
        }))
    }

    async fn handle_channel_close(&self, params: Option<Value>) -> Result<Value, String> {
        let store = self.channel_store.as_ref().ok_or("channel store not available")?;
        let params = params.ok_or("missing params")?;
        let channel_id_hex = params.get("channel_id").and_then(|v| v.as_str()).ok_or("missing 'channel_id'")?;
        let channel_id = parse_pubkey(channel_id_hex)?;

        let cs = store.lock().await;
        let final_state = cs.close_channel(&channel_id).map_err(|e| e.to_string())?;

        debug!("Closed payment channel {}", channel_id_hex);

        if let Some(ref tx) = self.event_sender {
            let _ = tx.send(DaemonEvent::ChannelClosed {
                channel_id: channel_id_hex.to_string(),
            });
        }

        Ok(serde_json::json!({
            "channel_id": channel_id_hex,
            "status": "closed",
            "final_spent": final_state.spent,
            "locked_amount": final_state.locked_amount,
            "remaining": final_state.remaining(),
        }))
    }

    // -- Settlement IPC handlers --

    async fn handle_settlement_create_pool(&self, params: Option<Value>) -> Result<Value, String> {
        let sc = self.settlement_client.as_ref().ok_or("settlement client not available")?;
        let params = params.ok_or("missing params")?;
        let creator_hex = params.get("creator").and_then(|v| v.as_str()).ok_or("missing 'creator'")?;
        let tier = params.get("tier").and_then(|v| v.as_u64()).unwrap_or(2) as u8;
        let creator = parse_pubkey(creator_hex)?;

        let client = sc.lock().await;
        let result = client.create_creator_pool(&creator, tier).await.map_err(|e| e.to_string())?;

        Ok(serde_json::json!({
            "signature": result.signature,
            "confirmed": result.confirmed,
            "creator": creator_hex,
            "tier": tier,
        }))
    }

    async fn handle_settlement_fund_pool(&self, params: Option<Value>) -> Result<Value, String> {
        let sc = self.settlement_client.as_ref().ok_or("settlement client not available")?;
        let params = params.ok_or("missing params")?;
        let creator_hex = params.get("creator").and_then(|v| v.as_str()).ok_or("missing 'creator'")?;
        let amount = params.get("amount").and_then(|v| v.as_u64()).ok_or("missing 'amount'")?;
        let creator = parse_pubkey(creator_hex)?;

        let client = sc.lock().await;
        let result = client.fund_pool(&creator, amount).await.map_err(|e| e.to_string())?;

        if let Some(ref tx) = self.event_sender {
            let _ = tx.send(DaemonEvent::PoolFunded {
                creator: creator_hex.to_string(),
                amount,
            });
        }

        Ok(serde_json::json!({
            "signature": result.signature,
            "confirmed": result.confirmed,
            "creator": creator_hex,
            "amount": amount,
        }))
    }

    async fn handle_settlement_claim(&self, params: Option<Value>) -> Result<Value, String> {
        let sc = self.settlement_client.as_ref().ok_or("settlement client not available")?;
        let params = params.ok_or("missing params")?;
        let pool_hex = params.get("pool").and_then(|v| v.as_str()).ok_or("missing 'pool'")?;
        let weight = params.get("weight").and_then(|v| v.as_u64()).ok_or("missing 'weight'")?;
        let leaf_index = params.get("leaf_index").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        let pool = parse_pubkey(pool_hex)?;

        // Parse merkle_proof
        let proof: Vec<[u8; 32]> = if let Some(arr) = params.get("merkle_proof").and_then(|v| v.as_array()) {
            arr.iter()
                .filter_map(|v| v.as_str())
                .filter_map(|s| {
                    let bytes = hex::decode(s).ok()?;
                    if bytes.len() == 32 {
                        let mut arr = [0u8; 32];
                        arr.copy_from_slice(&bytes);
                        Some(arr)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![]
        };

        // Build a minimal receipt for the claim
        let storage_node_hex = params.get("operator").and_then(|v| v.as_str()).ok_or("missing 'operator'")?;
        let storage_node = parse_pubkey(storage_node_hex)?;
        let receipt = datacraft_core::StorageReceipt {
            content_id: datacraft_core::ContentId::from_bytes(&[0u8; 32]),
            storage_node,
            challenger: [0u8; 32],
            segment_index: 0,
            piece_id: [0u8; 32],
            timestamp: 0,
            nonce: [0u8; 32],
            proof_hash: [0u8; 32],
            signature: vec![],
        };

        let client = sc.lock().await;
        let result = client.claim_pdp(&pool, &receipt, weight, proof, leaf_index).await.map_err(|e| e.to_string())?;

        Ok(serde_json::json!({
            "signature": result.signature,
            "confirmed": result.confirmed,
            "pool": pool_hex,
            "weight": weight,
        }))
    }

    async fn handle_settlement_open_channel(&self, params: Option<Value>) -> Result<Value, String> {
        let sc = self.settlement_client.as_ref().ok_or("settlement client not available")?;
        let params = params.ok_or("missing params")?;
        let payee_hex = params.get("payee").and_then(|v| v.as_str()).ok_or("missing 'payee'")?;
        let amount = params.get("amount").and_then(|v| v.as_u64()).ok_or("missing 'amount'")?;
        let payee = parse_pubkey(payee_hex)?;

        let client = sc.lock().await;
        let result = client.open_payment_channel(&payee, amount).await.map_err(|e| e.to_string())?;

        Ok(serde_json::json!({
            "signature": result.signature,
            "confirmed": result.confirmed,
            "payee": payee_hex,
            "amount": amount,
        }))
    }

    async fn handle_settlement_close_channel(&self, params: Option<Value>) -> Result<Value, String> {
        let sc = self.settlement_client.as_ref().ok_or("settlement client not available")?;
        let params = params.ok_or("missing params")?;
        let user_hex = params.get("user").and_then(|v| v.as_str()).ok_or("missing 'user'")?;
        let node_hex = params.get("node").and_then(|v| v.as_str()).ok_or("missing 'node'")?;
        let amount = params.get("amount").and_then(|v| v.as_u64()).ok_or("missing 'amount'")?;
        let nonce = params.get("nonce").and_then(|v| v.as_u64()).ok_or("missing 'nonce'")?;
        let signature_hex = params.get("voucher_signature").and_then(|v| v.as_str()).unwrap_or("");

        let user = parse_pubkey(user_hex)?;
        let node = parse_pubkey(node_hex)?;
        let voucher_sig = if signature_hex.is_empty() {
            vec![]
        } else {
            hex::decode(signature_hex).map_err(|e| e.to_string())?
        };

        let client = sc.lock().await;
        let channel_pda = craftec_settlement::pda::payment_channel_pda(
            client.program_id(),
            &user,
            &node,
        );
        let result = client
            .close_payment_channel(&channel_pda, &user, &node, amount, nonce, voucher_sig)
            .await
            .map_err(|e| e.to_string())?;

        Ok(serde_json::json!({
            "signature": result.signature,
            "confirmed": result.confirmed,
            "user": user_hex,
            "node": node_hex,
            "amount": amount,
        }))
    }

    async fn handle_channel_list(&self, params: Option<Value>) -> Result<Value, String> {
        let store = self.channel_store.as_ref().ok_or("channel store not available")?;
        let cs = store.lock().await;

        let channels = if let Some(params) = params {
            if let Some(peer_hex) = params.get("peer").and_then(|v| v.as_str()) {
                let peer = parse_pubkey(peer_hex)?;
                cs.list_channels_by_peer(&peer)
            } else {
                cs.list_channels()
            }
        } else {
            cs.list_channels()
        };

        let items: Vec<Value> = channels.iter().map(|ch| {
            serde_json::json!({
                "channel_id": hex::encode(ch.channel_id),
                "sender": hex::encode(ch.sender),
                "receiver": hex::encode(ch.receiver),
                "locked_amount": ch.locked_amount,
                "spent": ch.spent,
                "remaining": ch.remaining(),
                "nonce": ch.nonce,
            })
        }).collect();

        Ok(serde_json::json!({ "channels": items }))
    }

    async fn handle_get_config(&self) -> Result<Value, String> {
        let config = self.daemon_config.as_ref().ok_or("daemon config not available")?;
        let config = config.lock().await;
        serde_json::to_value(&*config).map_err(|e| e.to_string())
    }

    async fn handle_set_config(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let config_arc = self.daemon_config.as_ref().ok_or("daemon config not available")?;
        let data_dir = self.data_dir.as_ref().ok_or("data dir not available")?;

        // Client may send {config: "<json string>"} or direct fields
        let partial = if let Some(config_str) = params.get("config").and_then(|v| v.as_str()) {
            serde_json::from_str::<Value>(config_str).map_err(|e| format!("invalid config JSON: {}", e))?
        } else {
            params
        };

        let mut config = config_arc.lock().await;
        config.merge(&partial);
        config.save(data_dir).map_err(|e| e.to_string())?;

        info!("Config updated and saved");
        serde_json::to_value(&*config).map_err(|e| e.to_string())
    }

    // -----------------------------------------------------------------------
    // Health & Statistics RPC methods
    // -----------------------------------------------------------------------

    /// Shared helper: compute health info for a single CID.
    /// Returns (min_rank, health_ratio, disk_usage).
    /// Health ratio = min per-segment ratio (pieces/k_for_segment), so the last
    /// segment (which has fewer source pieces) doesn't drag down the score.
    async fn compute_cid_health(&self, cid: &datacraft_core::ContentId) -> Result<(usize, f64, u64), String> {
        let client = self.client.lock().await;
        let store = client.store();

        let manifest = store.get_manifest(cid).ok();
        let segments = store.list_segments(cid).unwrap_or_default();
        let mut min_rank: Option<usize> = None;
        let mut min_ratio: Option<f64> = None;
        for &seg in &segments {
            let pieces = store.list_pieces(cid, seg).unwrap_or_default();
            let rank = pieces.len();
            min_rank = Some(min_rank.map_or(rank, |r: usize| r.min(rank)));
            // Per-segment k for correct last-segment health
            let seg_k = manifest.as_ref()
                .map(|m| m.k_for_segment(seg as usize))
                .unwrap_or(0);
            if seg_k > 0 {
                let ratio = rank as f64 / seg_k as f64;
                min_ratio = Some(min_ratio.map_or(ratio, |r: f64| r.min(ratio)));
            }
        }
        let min_rank = min_rank.unwrap_or(0);
        let health_ratio = min_ratio.unwrap_or(0.0);
        let disk_usage = store.cid_disk_usage(cid);

        Ok((min_rank, health_ratio, disk_usage))
    }

    /// `content.health` — Per-CID health info.
    async fn handle_content_health(&self, params: Option<Value>) -> Result<Value, String> {
        let cid = extract_cid(params)?;
        let client = self.client.lock().await;
        let store = client.store();

        let manifest = store.get_manifest(&cid).map_err(|e| e.to_string())?;
        let pinned = client.is_pinned(&cid);

        let segments_list = store.list_segments(&cid).unwrap_or_default();
        let mut segments_json = Vec::new();
        let mut min_rank: Option<usize> = None;
        let mut min_ratio: Option<f64> = None;

        for &seg in &segments_list {
            let pieces = store.list_pieces(&cid, seg).unwrap_or_default();
            let rank = pieces.len();
            min_rank = Some(min_rank.map_or(rank, |r: usize| r.min(rank)));
            let seg_k = manifest.k_for_segment(seg as usize);
            if seg_k > 0 {
                let ratio = rank as f64 / seg_k as f64;
                min_ratio = Some(min_ratio.map_or(ratio, |r: f64| r.min(ratio)));
            }
            segments_json.push(serde_json::json!({
                "index": seg,
                "local_pieces": rank,
                "rank": rank,
            }));
        }
        let min_rank_val = min_rank.unwrap_or(0);
        let disk_usage = store.cid_disk_usage(&cid);
        drop(client);

        // Get tracker state
        let (name, k, stage, role, provider_count) = if let Some(ref tracker) = self.content_tracker {
            let t = tracker.lock().await;
            if let Some(state) = t.get(&cid) {
                (
                    state.name.clone(),
                    state.k,
                    state.stage.to_string(),
                    match state.role {
                        crate::content_tracker::ContentRole::Publisher => "publisher",
                        crate::content_tracker::ContentRole::StorageProvider => "storage_provider",
                    }.to_string(),
                    state.provider_count,
                )
            } else {
                (String::new(), 0, String::new(), String::new(), 0)
            }
        } else {
            (String::new(), 0, String::new(), String::new(), 0)
        };

        let health_ratio = min_ratio.unwrap_or(0.0);

        // Build provider list with peer scorer info
        let mut providers_json = Vec::new();
        if let Some(ref tracker) = self.content_tracker {
            let t = tracker.lock().await;
            let providers = t.get_providers(&cid);
            drop(t);
            if let Some(ref scorer) = self.peer_scorer {
                let mut ps = scorer.lock().await;
                for peer in providers {
                    let region = ps.get_region(&peer).unwrap_or("unknown").to_string();
                    let score = ps.score(&peer);
                    let latency = ps.iter().find(|(p, _)| *p == &peer)
                        .map(|(_, s)| s.avg_latency_ms)
                        .unwrap_or(0.0);
                    providers_json.push(serde_json::json!({
                        "peer_id": peer.to_string(),
                        "region": region,
                        "score": score,
                        "latency_ms": latency,
                    }));
                }
            }
        }

        Ok(serde_json::json!({
            "content_id": cid.to_hex(),
            "name": name,
            "original_size": manifest.total_size,
            "segment_count": manifest.segment_count,
            "k": k,
            "segments": segments_json,
            "min_rank": min_rank_val,
            "health_ratio": health_ratio,
            "provider_count": provider_count,
            "providers": providers_json,
            "pinned": pinned,
            "role": role,
            "stage": stage,
            "local_disk_usage": disk_usage,
        }))
    }

    /// `content.list_detailed` — Enhanced list with health info per CID.
    async fn handle_content_list_detailed(&self) -> Result<Value, String> {
        // Single lock on client for all CIDs — avoids re-locking per CID.
        let client = self.client.lock().await;
        let items = client.list().map_err(|e| e.to_string())?;

        // Compute health inline while we hold the client lock.
        // Uses per-segment k from manifest so last segment doesn't drag down health.
        let mut cid_health: Vec<(datacraft_core::ContentId, usize, f64, u64)> = Vec::new();
        for item in &items {
            let manifest = client.store().get_manifest(&item.content_id).ok();
            let segments = client.store().list_segments(&item.content_id).unwrap_or_default();
            let mut min_rank: Option<usize> = None;
            let mut min_ratio: Option<f64> = None;
            for &seg in &segments {
                let pieces = client.store().list_pieces(&item.content_id, seg).unwrap_or_default();
                let rank = pieces.len();
                min_rank = Some(min_rank.map_or(rank, |r: usize| r.min(rank)));
                let seg_k = manifest.as_ref()
                    .map(|m| m.k_for_segment(seg as usize))
                    .unwrap_or(0);
                if seg_k > 0 {
                    let ratio = rank as f64 / seg_k as f64;
                    min_ratio = Some(min_ratio.map_or(ratio, |r: f64| r.min(ratio)));
                }
            }
            let disk_usage = client.store().cid_disk_usage(&item.content_id);
            cid_health.push((item.content_id, min_rank.unwrap_or(0), min_ratio.unwrap_or(0.0), disk_usage));
        }
        drop(client);

        // Now build the JSON response with tracker data.
        let tracker_data: Option<Vec<_>> = if let Some(ref tracker) = self.content_tracker {
            let t = tracker.lock().await;
            Some(items.iter().map(|item| t.get(&item.content_id).cloned()).collect())
        } else {
            None
        };

        let mut result = Vec::new();
        for (i, item) in items.iter().enumerate() {
            let (_, min_rank, health_ratio, disk_usage) = &cid_health[i];

            let mut obj = serde_json::json!({
                "content_id": item.content_id.to_hex(),
                "total_size": item.total_size,
                "segment_count": item.segment_count,
                "pinned": item.pinned,
                "min_rank": min_rank,
                "health_ratio": health_ratio,
                "local_disk_usage": disk_usage,
            });

            if let Some(ref td) = tracker_data {
                if let Some(ref state) = td[i] {
                    obj["name"] = serde_json::json!(state.name);
                    obj["encrypted"] = serde_json::json!(state.encrypted);
                    obj["stage"] = serde_json::json!(state.stage.to_string());
                    obj["local_pieces"] = serde_json::json!(state.local_pieces);
                    obj["provider_count"] = serde_json::json!(state.provider_count);
                    obj["role"] = serde_json::json!(match state.role {
                        crate::content_tracker::ContentRole::Publisher => "publisher",
                        crate::content_tracker::ContentRole::StorageProvider => "storage_provider",
                    });
                    obj["hot"] = serde_json::json!(state.provider_count > state.segment_count * 2);
                }
            }

            result.push(obj);
        }

        Ok(serde_json::json!(result))
    }

    /// `network.health` — Network-wide health statistics.
    async fn handle_network_health(&self) -> Result<Value, String> {
        // Network storage from peer scorer
        let (storage_summary, storage_node_count, unique_providers) = if let Some(ref scorer) = self.peer_scorer {
            let ps = scorer.lock().await;
            let summary = ps.network_storage_summary();
            let node_count = summary.storage_node_count;
            let unique = ps.iter().count();
            (Some(summary), node_count, unique)
        } else {
            (None, 0, 0)
        };

        let mut total_content = 0usize;
        let mut total_stored_bytes = 0u64;
        let mut healthy = 0usize;
        let mut degraded = 0usize;
        let mut health_sum = 0.0f64;

        // Collect tracker states first (release lock before accessing client).
        let states = if let Some(ref tracker) = self.content_tracker {
            let t = tracker.lock().await;
            t.list()
        } else {
            Vec::new()
        };

        if !states.is_empty() {
            // Single client lock for all CID health computations.
            let client = self.client.lock().await;
            total_content = states.len();
            for state in &states {
                let manifest = client.store().get_manifest(&state.content_id).ok();
                let segments = client.store().list_segments(&state.content_id).unwrap_or_default();
                let mut min_ratio: Option<f64> = None;
                let mut all_reconstructable = true;
                for &seg in &segments {
                    let pieces = client.store().list_pieces(&state.content_id, seg).unwrap_or_default();
                    let rank = pieces.len();
                    let seg_k = manifest.as_ref()
                        .map(|m| m.k_for_segment(seg as usize))
                        .unwrap_or(state.k);
                    if seg_k > 0 {
                        let ratio = rank as f64 / seg_k as f64;
                        min_ratio = Some(min_ratio.map_or(ratio, |r: f64| r.min(ratio)));
                        if rank < seg_k { all_reconstructable = false; }
                    }
                }
                let disk_usage = client.store().cid_disk_usage(&state.content_id);
                total_stored_bytes += disk_usage;
                let health_ratio = min_ratio.unwrap_or(0.0);
                health_sum += health_ratio;
                if all_reconstructable {
                    healthy += 1;
                } else {
                    degraded += 1;
                }
            }
        }

        let avg_health = if total_content > 0 { health_sum / total_content as f64 } else { 0.0 };

        let receipts_count = if let Some(ref rs) = self.receipt_store {
            let store = rs.lock().await;
            store.storage_receipt_count()
        } else {
            0
        };

        Ok(serde_json::json!({
            "total_content_count": total_content,
            "total_stored_bytes": total_stored_bytes,
            "total_network_storage_committed": storage_summary.as_ref().map(|s| s.total_committed).unwrap_or(0),
            "total_network_storage_used": storage_summary.as_ref().map(|s| s.total_used).unwrap_or(0),
            "storage_node_count": storage_node_count,
            "healthy_content_count": healthy,
            "degraded_content_count": degraded,
            "average_health_ratio": avg_health,
            "total_providers_unique": unique_providers,
            "receipts_this_epoch": receipts_count,
        }))
    }

    /// `node.stats` — This node's own statistics.
    async fn handle_node_stats(&self) -> Result<Value, String> {
        let mut content_count = 0usize;
        let mut published_count = 0usize;
        let mut stored_count = 0usize;
        let mut total_local_pieces = 0usize;

        if let Some(ref tracker) = self.content_tracker {
            let t = tracker.lock().await;
            for state in t.list() {
                content_count += 1;
                total_local_pieces += state.local_pieces;
                match state.role {
                    crate::content_tracker::ContentRole::Publisher => published_count += 1,
                    crate::content_tracker::ContentRole::StorageProvider => stored_count += 1,
                }
            }
        }

        let total_disk_usage = {
            let client = self.client.lock().await;
            client.store().disk_usage().unwrap_or(0)
        };

        let storage_root = if let Some(ref tree) = self.merkle_tree {
            let t = tree.lock().await;
            hex::encode(t.root())
        } else {
            String::new()
        };

        let cap_strings: Vec<String> = self.own_capabilities.iter().map(|c| c.to_string()).collect();

        let region = if let Some(ref cfg) = self.daemon_config {
            let c = cfg.lock().await;
            c.region.clone().unwrap_or_default()
        } else {
            String::new()
        };

        let receipts_generated = if let Some(ref rs) = self.receipt_store {
            let store = rs.lock().await;
            store.storage_receipt_count()
        } else {
            0
        };

        let uptime_secs = self.start_time.elapsed().as_secs();

        Ok(serde_json::json!({
            "content_count": content_count,
            "published_count": published_count,
            "stored_count": stored_count,
            "total_local_pieces": total_local_pieces,
            "total_disk_usage": total_disk_usage,
            "storage_root": storage_root,
            "capabilities": cap_strings,
            "region": region,
            "receipts_generated": receipts_generated,
            "uptime_secs": uptime_secs,
        }))
    }

    /// `content.segments` — Detailed per-segment breakdown.
    async fn handle_content_segments(&self, params: Option<Value>) -> Result<Value, String> {
        let cid = extract_cid(params)?;
        let client = self.client.lock().await;
        let store = client.store();

        // Get manifest for per-segment k calculation
        let manifest = store.get_manifest(&cid).ok();

        let segments_list = store.list_segments(&cid).unwrap_or_default();
        let mut segments_json = Vec::new();
        for &seg in &segments_list {
            let pieces = store.list_pieces(&cid, seg).unwrap_or_default();
            let piece_ids: Vec<String> = pieces.iter().map(|p| hex::encode(p)).collect();
            let local_count = pieces.len();
            // Use per-segment k (last segment may have fewer source pieces)
            let k = manifest.as_ref()
                .map(|m| m.k_for_segment(seg as usize))
                .unwrap_or(0);
            segments_json.push(serde_json::json!({
                "index": seg,
                "k": k,
                "local_pieces": local_count,
                "piece_ids": piece_ids,
                "reconstructable": local_count >= k && k > 0,
            }));
        }

        Ok(serde_json::json!({
            "content_id": cid.to_hex(),
            "segments": segments_json,
        }))
    }
}

fn parse_pubkey(hex_str: &str) -> Result<[u8; 32], String> {
    let bytes = hex::decode(hex_str).map_err(|e| e.to_string())?;
    if bytes.len() != 32 {
        return Err("expected 32 bytes hex".into());
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

impl IpcHandler for DataCraftHandler {
    fn handle(
        &self,
        method: &str,
        params: Option<Value>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + '_>> {
        let method = method.to_string();
        Box::pin(async move {
            debug!("IPC method: {}", method);
            match method.as_str() {
                "publish" => self.handle_publish(params).await,
                "fetch" => self.handle_fetch(params).await,
                "pin" => self.handle_pin(params).await,
                "unpin" => self.handle_unpin(params).await,
                "list" => self.handle_list().await,
                "status" => self.handle_status().await,
                "peers" => self.handle_peers().await,
                "node.capabilities" => self.handle_node_capabilities().await,
                "network.storage" => self.handle_network_storage().await,
                "extend" => self.handle_extend(params).await,
                "receipts.count" => self.handle_receipts_count().await,
                "receipts.query" => self.handle_receipts_query(params).await,
                "receipt.storage.list" => self.handle_storage_receipt_list(params).await,
                "data.providers" => self.handle_data_providers(params).await,
                "data.remove" => self.handle_data_remove(params).await,
                "data.delete_local" => self.handle_data_delete_local(params).await,
                "access.grant" => self.handle_access_grant(params).await,
                "access.revoke" => self.handle_access_revoke(params).await,
                "access.revoke_rotate" => self.handle_access_revoke_rotate(params).await,
                "access.list" => self.handle_access_list(params).await,
                "channel.open" => self.handle_channel_open(params).await,
                "channel.voucher" => self.handle_channel_voucher(params).await,
                "channel.close" => self.handle_channel_close(params).await,
                "channel.list" => self.handle_channel_list(params).await,
                "settlement.create_pool" => self.handle_settlement_create_pool(params).await,
                "settlement.fund_pool" => self.handle_settlement_fund_pool(params).await,
                "settlement.claim" => self.handle_settlement_claim(params).await,
                "settlement.open_channel" => self.handle_settlement_open_channel(params).await,
                "settlement.close_channel" => self.handle_settlement_close_channel(params).await,
                "get-config" => self.handle_get_config().await,
                "set-config" => self.handle_set_config(params).await,
                "content.health" => self.handle_content_health(params).await,
                "content.list_detailed" => self.handle_content_list_detailed().await,
                "content.segments" => self.handle_content_segments(params).await,
                "network.health" => self.handle_network_health().await,
                "node.stats" => self.handle_node_stats().await,
                "shutdown" => {
                    info!("Shutdown requested via RPC — broadcasting going-offline");
                    // Broadcast going-offline message before shutdown
                    if let Some(ref tx) = self.command_tx {
                        let local_peer_id = serde_json::json!({
                            "type": "going_offline",
                            "timestamp": std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        });
                        if let Ok(data) = serde_json::to_vec(&local_peer_id) {
                            let _ = tx.send(DataCraftCommand::BroadcastGoingOffline { data });
                        }
                    }
                    // Emit event
                    if let Some(ref etx) = self.event_sender {
                        let _ = etx.send(DaemonEvent::PeerGoingOffline {
                            peer_id: "self".to_string(),
                        });
                    }
                    // Respond first, then exit
                    let result = Ok(serde_json::json!({"status": "shutting_down"}));
                    // Schedule exit after response is sent (give time for gossipsub message to propagate)
                    tokio::spawn(async {
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        std::process::exit(0);
                    });
                    result
                },
                _ => Err(format!("unknown method: {}", method)),
            }
        })
    }
}

fn extract_cid(params: Option<Value>) -> Result<datacraft_core::ContentId, String> {
    let params = params.ok_or("missing params")?;
    let cid_hex = params
        .get("cid")
        .and_then(|v| v.as_str())
        .ok_or("missing 'cid' param")?;
    datacraft_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())
}
