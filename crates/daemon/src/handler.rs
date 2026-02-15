//! IPC request handler
//!
//! Implements craftec_ipc::IpcHandler for DataCraft daemon.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use craftec_ipc::server::IpcHandler;
use datacraft_client::DataCraftClient;
use datacraft_core::PublishOptions;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, warn};

use crate::channel_store::ChannelStore;
use crate::commands::DataCraftCommand;
use crate::protocol::DataCraftProtocol;
use crate::receipt_store::PersistentReceiptStore;

use datacraft_core::DataCraftCapability;
use std::collections::HashMap;

/// Peer capability tracker type alias.
type PeerCapabilities = Arc<Mutex<HashMap<libp2p::PeerId, (Vec<DataCraftCapability>, u64)>>>;

/// DataCraft IPC handler wrapping a DataCraftClient and protocol.
pub struct DataCraftHandler {
    client: Arc<Mutex<DataCraftClient>>,
    _protocol: Option<Arc<DataCraftProtocol>>,
    command_tx: Option<mpsc::UnboundedSender<DataCraftCommand>>,
    peer_capabilities: Option<PeerCapabilities>,
    receipt_store: Option<Arc<Mutex<PersistentReceiptStore>>>,
    channel_store: Option<Arc<Mutex<ChannelStore>>>,
}

impl DataCraftHandler {
    pub fn new(
        client: Arc<Mutex<DataCraftClient>>,
        protocol: Arc<DataCraftProtocol>,
        command_tx: mpsc::UnboundedSender<DataCraftCommand>,
        peer_capabilities: PeerCapabilities,
        receipt_store: Arc<Mutex<PersistentReceiptStore>>,
        channel_store: Arc<Mutex<ChannelStore>>,
    ) -> Self {
        Self { 
            client, 
            _protocol: Some(protocol),
            command_tx: Some(command_tx),
            peer_capabilities: Some(peer_capabilities),
            receipt_store: Some(receipt_store),
            channel_store: Some(channel_store),
        }
    }

    /// Create handler without protocol (for testing).
    pub fn new_without_protocol(client: Arc<Mutex<DataCraftClient>>) -> Self {
        Self { 
            client, 
            _protocol: None,
            command_tx: None,
            peer_capabilities: None,
            receipt_store: None,
            channel_store: None,
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
            erasure_config: None,
        };

        let mut client = self.client.lock().await;
        let result = client
            .publish(&PathBuf::from(path), &options)
            .map_err(|e| e.to_string())?;
        
        // Get the manifest for DHT announcement
        let manifest = client
            .store()
            .get_manifest(&result.content_id)
            .map_err(|e| format!("Failed to get manifest: {}", e))?;
        
        drop(client); // Release the lock

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

        let mut response = serde_json::json!({
            "cid": result.content_id.to_hex(),
            "size": result.total_size,
            "chunks": result.chunk_count as u64,
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

        // Milestones 2 & 3 & 4: Full P2P fetch pipeline
        // 1. Resolve providers from DHT
        // 2. Get manifest from DHT  
        // 3. Request missing shards from providers via P2P
        // 4. Continue to local reconstruction (erasure decode, verify CID, write file)
        if let Some(ref command_tx) = self.command_tx {
            debug!("Attempting DHT resolution for {}", cid);
            
            // First, try to get providers for this content
            let (reply_tx, reply_rx) = oneshot::channel();
            let command = DataCraftCommand::ResolveProviders {
                content_id: cid,
                reply_tx,
            };
            
            if command_tx.send(command).is_ok() {
                match reply_rx.await {
                    Ok(Ok(providers)) if !providers.is_empty() => {
                        debug!("Found {} providers for {}", providers.len(), cid);
                        
                        // Try to get the manifest from DHT
                        let (manifest_tx, manifest_rx) = oneshot::channel();
                        let command = DataCraftCommand::GetManifest {
                            content_id: cid,
            reply_tx: manifest_tx,
                        };
                        
                        if command_tx.send(command).is_ok() {
                            match manifest_rx.await {
                                Ok(Ok(manifest)) => {
                                    debug!("Retrieved manifest for {} from DHT", cid);
                                    
                                    // Milestone 3 & 4: Try to fetch missing shards from providers for full P2P pipeline
                                    if let Err(e) = self.fetch_missing_shards_from_peers(&cid, &manifest, &providers, command_tx).await {
                                        debug!("P2P shard transfer failed: {}, falling back to local reconstruction", e);
                                    } else {
                                        debug!("Successfully fetched missing shards via P2P, proceeding to reconstruction");
                                    }
                                }
                                Ok(Err(e)) => {
                                    debug!("Failed to get manifest from DHT: {}", e);
                                }
                                Err(e) => {
                                    debug!("Manifest request channel error: {}", e);
                                }
                            }
                        }
                    }
                    Ok(Ok(_)) => {
                        debug!("No providers found for {}", cid);
                    }
                    Ok(Err(e)) => {
                        debug!("Provider resolution failed: {}", e);
                    }
                    Err(e) => {
                        debug!("Provider resolution channel error: {}", e);
                    }
                }
            }
        }

        // Fall back to local reconstruction
        debug!("Using local reconstruction for {}", cid);
        let client = self.client.lock().await;
        client
            .reconstruct(&cid, &output, key.as_deref())
            .map_err(|e| e.to_string())?;

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
            "transfer": store.transfer_receipt_count(),
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
                "shard_index": r.shard_index,
                "storage_node": hex::encode(r.storage_node),
                "challenger": hex::encode(r.challenger),
                "timestamp": r.timestamp,
            }),
            crate::receipt_store::ReceiptEntry::Transfer(r) => serde_json::json!({
                "type": "transfer",
                "cid": r.content_id.to_hex(),
                "shard_index": r.shard_index,
                "server_node": hex::encode(r.server_node),
                "requester": hex::encode(r.requester),
                "bytes_served": r.bytes_served,
                "timestamp": r.timestamp,
            }),
        }).collect();

        Ok(serde_json::json!({ "receipts": results }))
    }

    async fn handle_peers(&self) -> Result<Value, String> {
        let caps = match &self.peer_capabilities {
            Some(pc) => pc.lock().await,
            None => return Ok(serde_json::json!({})),
        };
        let mut result = serde_json::Map::new();
        for (peer_id, (capabilities, timestamp)) in caps.iter() {
            let cap_strings: Vec<String> = capabilities.iter().map(|c| c.to_string()).collect();
            result.insert(
                peer_id.to_string(),
                serde_json::json!({
                    "capabilities": cap_strings,
                    "last_seen": timestamp,
                }),
            );
        }
        Ok(Value::Object(result))
    }

    /// Extend a CID by generating a new parity shard.
    async fn handle_extend(&self, params: Option<Value>) -> Result<Value, String> {
        let cid = extract_cid(params)?;
        let command_tx = self.command_tx.as_ref().ok_or("no network available")?;

        // 1. Get manifest (local or DHT)
        let manifest = {
            let client = self.client.lock().await;
            match client.store().get_manifest(&cid) {
                Ok(m) => m,
                Err(_) => {
                    drop(client);
                    let (tx, rx) = oneshot::channel();
                    command_tx.send(DataCraftCommand::GetManifest { content_id: cid, reply_tx: tx })
                        .map_err(|e| e.to_string())?;
                    rx.await.map_err(|e| e.to_string())??
                }
            }
        };

        // 2. Resolve providers
        let (tx, rx) = oneshot::channel();
        command_tx.send(DataCraftCommand::ResolveProviders { content_id: cid, reply_tx: tx })
            .map_err(|e| e.to_string())?;
        let providers = rx.await.map_err(|e| e.to_string())??;

        if providers.is_empty() {
            return Err("no providers found for CID".into());
        }

        // 3. Query all providers for max shard index
        let mut global_max: Option<u8> = None;
        {
            let client = self.client.lock().await;
            if let Some(local_max) = client.store().max_shard_index_for_content(&cid) {
                global_max = Some(local_max);
            }
        }

        for &provider in &providers {
            let (tx, rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::QueryMaxShardIndex {
                peer_id: provider,
                content_id: cid,
                reply_tx: tx,
            }).map_err(|e| e.to_string())?;

            if let Ok(Ok(Some(idx))) = rx.await {
                global_max = Some(global_max.map_or(idx, |m| m.max(idx)));
            }
        }

        // 4. Claim next index
        let k = manifest.k as u8;
        let initial_total = (manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards) as u8;
        let target_index = match global_max {
            Some(max) => max.checked_add(1).ok_or("shard index overflow")?,
            None => initial_total,
        };

        if target_index < k {
            return Err(format!("target index {} < k={}", target_index, k));
        }

        debug!("Extending {} with parity shard at index {}", cid, target_index);

        // 5. Ensure we have k data shards locally
        {
            let client = self.client.lock().await;
            let store = client.store();
            let mut missing = Vec::new();
            for i in 0..manifest.k {
                for chunk_idx in 0..manifest.chunk_count as u32 {
                    if store.get_shard(&cid, chunk_idx, i as u8).is_err() {
                        missing.push((chunk_idx, i as u8));
                    }
                }
            }
            drop(client);

            for (chunk_idx, shard_idx) in missing {
                let mut fetched = false;
                for &provider in &providers {
                    let (tx, rx) = oneshot::channel();
                    command_tx.send(DataCraftCommand::RequestShard {
                        peer_id: provider,
                        content_id: cid,
                        chunk_index: chunk_idx,
                        shard_index: shard_idx,
                        local_public_key: [0u8; 32],
                        reply_tx: tx,
                    }).map_err(|e| e.to_string())?;
                    if let Ok(Ok(data)) = rx.await {
                        let client = self.client.lock().await;
                        client.store().put_shard(&cid, chunk_idx, shard_idx, &data)
                            .map_err(|e| e.to_string())?;
                        fetched = true;
                        break;
                    }
                }
                if !fetched {
                    return Err(format!("failed to fetch data shard {}/{}", chunk_idx, shard_idx));
                }
            }
        }

        // 6. Generate parity shards + store
        {
            let client = self.client.lock().await;
            datacraft_client::extension::extend_content(client.store(), &manifest, target_index)
                .map_err(|e| e.to_string())?;
        }

        // 7. Announce as provider
        {
            let (tx, rx) = oneshot::channel();
            command_tx.send(DataCraftCommand::AnnounceProvider {
                content_id: cid,
                manifest: manifest.clone(),
                reply_tx: tx,
            }).map_err(|e| e.to_string())?;
            let _ = rx.await;
        }

        debug!("Extended {} with parity shard index {}", cid, target_index);

        Ok(serde_json::json!({
            "cid": cid.to_hex(),
            "shard_index": target_index,
        }))
    }

    /// Fetch missing shards from remote peers using P2P transfer.
    async fn fetch_missing_shards_from_peers(
        &self,
        content_id: &datacraft_core::ContentId,
        manifest: &datacraft_core::ChunkManifest,
        providers: &[libp2p::PeerId],
        command_tx: &tokio::sync::mpsc::UnboundedSender<DataCraftCommand>,
    ) -> Result<(), String> {
        debug!("Fetching missing shards for {} from {} providers", content_id, providers.len());
        
        let total_shards = manifest.erasure_config.data_shards + manifest.erasure_config.parity_shards;
        let client = self.client.lock().await;
        let store = client.store();
        
        // For each chunk, try to fetch missing shards from providers
        for chunk_idx in 0..manifest.chunk_count {
            let chunk_idx_u32 = chunk_idx as u32;
            // Check which shards we already have locally
            for shard_idx in 0..total_shards {
                if store.get_shard(content_id, chunk_idx_u32, shard_idx as u8).is_ok() {
                    continue; // We already have this shard
                }
                
                debug!("Trying to fetch missing shard {}/{}/{}", content_id, chunk_idx, shard_idx);
                
                // Try to fetch this shard from any provider
                let mut fetched = false;
                for &provider in providers {
                    debug!("Requesting shard {}/{}/{} from provider {}", content_id, chunk_idx, shard_idx, provider);
                    
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let command = DataCraftCommand::RequestShard {
                        peer_id: provider,
                        content_id: *content_id,
                        chunk_index: chunk_idx_u32,
                        shard_index: shard_idx as u8,
                        local_public_key: [0u8; 32], // TODO: use actual node keypair public key
                        reply_tx,
                    };
                    
                    if command_tx.send(command).is_err() {
                        continue;
                    }
                    
                    match reply_rx.await {
                        Ok(Ok(shard_data)) => {
                            debug!("Successfully received shard {}/{}/{} from {}", content_id, chunk_idx, shard_idx, provider);
                            
                            // Store the shard locally
                            if let Err(e) = store.put_shard(content_id, chunk_idx_u32, shard_idx as u8, &shard_data) {
                                warn!("Failed to store shard {}/{}/{}: {}", content_id, chunk_idx, shard_idx, e);
                                continue;
                            }
                            
                            fetched = true;
                            break; // Found this shard, move to next
                        }
                        Ok(Err(e)) => {
                            debug!("Failed to get shard {}/{}/{} from {}: {}", content_id, chunk_idx, shard_idx, provider, e);
                        }
                        Err(e) => {
                            debug!("Channel error when requesting shard {}/{}/{}: {}", content_id, chunk_idx, shard_idx, e);
                        }
                    }
                }
                
                if !fetched {
                    debug!("Failed to fetch shard {}/{}/{} from any provider", content_id, chunk_idx, shard_idx);
                    // Continue trying other shards - we might have enough for erasure recovery
                }
            }
        }
        
        drop(client); // Release the lock
        debug!("Completed P2P shard fetching for {} - downloaded missing shards from {} providers", content_id, providers.len());
        Ok(())
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

        Ok(serde_json::json!({
            "channel_id": channel_id_hex,
            "status": "closed",
            "final_spent": final_state.spent,
            "locked_amount": final_state.locked_amount,
            "remaining": final_state.remaining(),
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
                "extend" => self.handle_extend(params).await,
                "receipts.count" => self.handle_receipts_count().await,
                "receipts.query" => self.handle_receipts_query(params).await,
                "channel.open" => self.handle_channel_open(params).await,
                "channel.voucher" => self.handle_channel_voucher(params).await,
                "channel.close" => self.handle_channel_close(params).await,
                "channel.list" => self.handle_channel_list(params).await,
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
