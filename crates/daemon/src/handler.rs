//! IPC request handler
//!
//! Implements craftec_ipc::IpcHandler for CraftObj daemon.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use craftec_ipc::server::IpcHandler;
use craftobj_client::CraftObjClient;
use craftobj_core::PublishOptions;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, info, warn};


use crate::commands::CraftObjCommand;
use crate::config::DaemonConfig;
use crate::content_tracker::ContentTracker;
use crate::events::{DaemonEvent, EventSender};
use crate::protocol::CraftObjProtocol;
use crate::receipt_store::PersistentReceiptStore;
use crate::settlement::SolanaClient;

use craftobj_core::CraftObjCapability;
use ed25519_dalek;

use crate::peer_scorer::PeerScorer;

/// Shared peer scorer type alias.
type SharedPeerScorer = Arc<Mutex<PeerScorer>>;

/// Network health summary for a single CID.
struct CidNetworkHealth {
    /// Per-segment network piece counts (local + remote).
    seg_pieces: Vec<usize>,
    /// Total pieces across all segments (local + remote).
    total_pieces: usize,
    /// Network health ratio (min across segments of pieces/k).
    health_ratio: f64,
    /// Number of providers (remote peers with pieces + local if applicable).
    provider_count: usize,
    /// Provider details as JSON values.
    providers: Vec<Value>,
}

impl CraftObjHandler {
    /// Compute network health for a CID by aggregating local + remote pieces.
    /// `local_seg_pieces` should contain per-segment local piece counts.
    /// `manifest` provides segment count and per-segment k values.
    async fn compute_network_health(
        &self,
        cid_hex: &str,
        local_seg_pieces: &[usize],
        manifest: &craftobj_core::ContentManifest,
        include_provider_details: bool,
    ) -> CidNetworkHealth {
        let seg_count = manifest.segment_count();
        let mut network_seg_pieces = vec![0usize; seg_count];
        let mut providers: Vec<Value> = Vec::new();
        let mut network_total: usize = 0;
        let mut provider_count: usize = 0;

        // Add local pieces
        let local_total: usize = local_seg_pieces.iter().sum();
        network_total += local_total;
        for (i, &c) in local_seg_pieces.iter().enumerate() {
            if i < seg_count { network_seg_pieces[i] += c; }
        }

        // Add remote pieces from PieceMap (piece_counts removed from capability announcements)
        if let Some(ref pm) = self.piece_map {
            if let Ok(cid) = craftobj_core::ContentId::from_hex(cid_hex) {
                let map = pm.lock().await;
                // Count pieces per node per segment from PieceMap
                let mut node_pieces: std::collections::HashMap<Vec<u8>, Vec<usize>> = std::collections::HashMap::new();
                for seg in 0..seg_count as u32 {
                    for (node, _pid, _coeff) in map.pieces_for_segment(&cid, seg) {
                        let local_node = map.local_node();
                        if node == local_node { continue; } // Skip local, already counted
                        let entry = node_pieces.entry(node.clone()).or_insert_with(|| vec![0; seg_count]);
                        if (seg as usize) < seg_count {
                            entry[seg as usize] += 1;
                        }
                    }
                }
                for (node, seg_counts) in &node_pieces {
                    let total: usize = seg_counts.iter().sum();
                    if total > 0 {
                        network_total += total;
                        provider_count += 1;
                        for (i, &c) in seg_counts.iter().enumerate() {
                            if i < seg_count { network_seg_pieces[i] += c; }
                        }
                        if include_provider_details {
                            let peer_id_str = if let Ok(pid) = libp2p::PeerId::from_bytes(node) {
                                pid.to_string()
                            } else {
                                hex::encode(node)
                            };
                            providers.push(serde_json::json!({
                                "peer_id": peer_id_str,
                                "piece_count": total,
                                "segment_pieces": seg_counts,
                            }));
                        }
                    }
                }
            }
        }
        // Add tracked providers without PieceMap entries (if detailed)
        if let Some(ref scorer) = self.peer_scorer {
            let ps = scorer.lock().await;
            if include_provider_details {
                if let Some(ref tracker) = self.content_tracker {
                    if let Ok(cid) = craftobj_core::ContentId::from_hex(cid_hex) {
                        let t = tracker.lock().await;
                        let tracked = t.get_providers(&cid);
                        for peer in tracked {
                            if !providers.iter().any(|p| p["peer_id"].as_str() == Some(&peer.to_string())) {
                                let region = ps.get_region(&peer).unwrap_or("unknown").to_string();
                                let score = ps.score_readonly(&peer);
                                let latency = ps.get(&peer).map(|s| s.avg_latency_ms).unwrap_or(0.0);
                                let merkle_root = ps.get(&peer).map(|s| hex::encode(s.storage_root)).unwrap_or_default();
                                provider_count += 1;
                                providers.push(serde_json::json!({
                                    "peer_id": peer.to_string(),
                                    "piece_count": 0,
                                    "segment_pieces": [],
                                    "merkle_root": merkle_root,
                                    "last_seen": ps.get(&peer).map(|s| s.last_announcement.elapsed().as_secs()).unwrap_or(0),
                                    "region": region,
                                    "score": score,
                                    "latency_ms": latency,
                                }));
                            }
                        }
                    }
                }
            }
        }

        // Add local node as provider
        if local_total > 0 {
            provider_count += 1;
            if include_provider_details {
                let local_merkle = if let Some(ref tree) = self.merkle_tree {
                    let t = tree.lock().await;
                    hex::encode(t.root())
                } else {
                    String::new()
                };
                let local_pid = self.local_peer_id
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "local".to_string());
                providers.push(serde_json::json!({
                    "peer_id": local_pid,
                    "piece_count": local_total,
                    "segment_pieces": local_seg_pieces,
                    "merkle_root": local_merkle,
                    "last_seen": 0,
                    "region": "local",
                    "score": 1.0,
                    "latency_ms": 0.0,
                    "is_local": true,
                }));
            }
        }

        // Compute health ratio
        let health_ratio = if seg_count > 0 {
            let min_ratio = (0..seg_count).map(|i| {
                let seg_k = manifest.k_for_segment(i);
                let pieces = network_seg_pieces.get(i).copied().unwrap_or(0);
                if seg_k > 0 { pieces as f64 / seg_k as f64 } else { 1.0 }
            }).fold(f64::MAX, f64::min);
            if min_ratio == f64::MAX { 0.0 } else { min_ratio }
        } else {
            0.0
        };

        CidNetworkHealth {
            seg_pieces: network_seg_pieces,
            total_pieces: network_total,
            health_ratio,
            provider_count,
            providers,
        }
    }
}

/// CraftObj IPC handler wrapping a CraftObjClient and protocol.
pub struct CraftObjHandler {
    client: Arc<Mutex<CraftObjClient>>,
    _protocol: Option<Arc<CraftObjProtocol>>,
    command_tx: Option<mpsc::UnboundedSender<CraftObjCommand>>,
    peer_scorer: Option<SharedPeerScorer>,
    receipt_store: Option<Arc<Mutex<PersistentReceiptStore>>>,

    settlement_client: Option<Arc<Mutex<SolanaClient>>>,
    content_tracker: Option<Arc<Mutex<ContentTracker>>>,
    own_capabilities: Vec<CraftObjCapability>,
    daemon_config: Option<Arc<Mutex<DaemonConfig>>>,
    data_dir: Option<std::path::PathBuf>,
    event_sender: Option<EventSender>,
    /// Node signing key for manifest signing on publish.
    node_signing_key: Option<ed25519_dalek::SigningKey>,
    /// Eviction manager for recording access on fetch.
    eviction_manager: Option<Arc<Mutex<crate::eviction::EvictionManager>>>,
    /// Storage Merkle tree for incremental updates on store operations.
    merkle_tree: Option<Arc<Mutex<craftobj_store::merkle::StorageMerkleTree>>>,
    /// Local peer ID for filtering self from provider lists.
    pub local_peer_id: Option<libp2p::PeerId>,
    /// Challenger manager for PDP — register CIDs after publish/store.
    challenger: Option<Arc<Mutex<crate::challenger::ChallengerManager>>>,
    /// Demand signal tracker for content demand status.
    demand_tracker: Option<Arc<Mutex<crate::scaling::DemandTracker>>>,
    /// PieceMap for event-sourced piece tracking.
    piece_map: Option<Arc<Mutex<crate::piece_map::PieceMap>>>,
    /// Start time for uptime calculation.
    start_time: Instant,
    /// Shutdown signal — notified when RPC shutdown is requested.
    pub shutdown_notify: Option<Arc<tokio::sync::Notify>>,
}

impl CraftObjHandler {
    pub fn new(
        client: Arc<Mutex<CraftObjClient>>,
        protocol: Arc<CraftObjProtocol>,
        command_tx: mpsc::UnboundedSender<CraftObjCommand>,
        peer_scorer: SharedPeerScorer,
        receipt_store: Arc<Mutex<PersistentReceiptStore>>,
    ) -> Self {
        Self { 
            client, 
            _protocol: Some(protocol),
            command_tx: Some(command_tx),
            peer_scorer: Some(peer_scorer),
            receipt_store: Some(receipt_store),

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
            demand_tracker: None,
            piece_map: None,
            local_peer_id: None,
            start_time: Instant::now(),
            shutdown_notify: None,
        }
    }

    pub fn set_shutdown_notify(&mut self, notify: Arc<tokio::sync::Notify>) {
        self.shutdown_notify = Some(notify);
    }

    pub fn set_piece_map(&mut self, pm: Arc<Mutex<crate::piece_map::PieceMap>>) {
        self.piece_map = Some(pm);
    }

    pub fn set_demand_tracker(&mut self, tracker: Arc<Mutex<crate::scaling::DemandTracker>>) {
        self.demand_tracker = Some(tracker);
    }

    pub fn set_local_peer_id(&mut self, peer_id: libp2p::PeerId) {
        self.local_peer_id = Some(peer_id);
    }

    pub fn set_merkle_tree(&mut self, tree: Arc<Mutex<craftobj_store::merkle::StorageMerkleTree>>) {
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
    pub fn set_own_capabilities(&mut self, caps: Vec<CraftObjCapability>) {
        self.own_capabilities = caps;
    }

    /// Create handler without protocol (for testing).
    pub fn new_without_protocol(client: Arc<Mutex<CraftObjClient>>) -> Self {
        Self { 
            client, 
            _protocol: None,
            command_tx: None,
            peer_scorer: None,
            receipt_store: None,

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
            demand_tracker: None,
            piece_map: None,
            local_peer_id: None,
            start_time: Instant::now(),
            shutdown_notify: None,
        }
    }

    /// Emit PieceDropped events for all pieces of a CID (used when deleting content).
    async fn emit_pieces_dropped_for_content(&self, cid: &craftobj_core::ContentId) {
        if let Some(ref pm) = self.piece_map {
            // Collect all local pieces for this CID from the store before deletion
            let client = self.client.lock().await;
            let segments = match client.store().list_segments(cid) {
                Ok(s) => s,
                Err(_) => return,
            };
            drop(client);

            let mut map = pm.lock().await;
            let local_node = map.local_node().to_vec();
            for seg in segments {
                let pieces = {
                    let client = self.client.lock().await;
                    client.store().list_pieces(cid, seg).unwrap_or_default()
                };
                for pid in pieces {
                    let seq = map.next_seq();
                    let mut dropped = craftobj_core::PieceDropped {
                        node: local_node.clone(),
                        cid: *cid,
                        segment: seg,
                        piece_id: pid,
                        seq,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        signature: vec![],
                    };
                    if let Some(ref key) = self.node_signing_key {
                        dropped.sign(key);
                    }
                    let event = craftobj_core::PieceEvent::Dropped(dropped);
                    map.apply_event(&event);
                }
            }
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
            mgr.register_cid(result.content_id, Some(crate::health::TierInfo { min_piece_ratio: 1.5 }));
        }

        // Don't announce publisher as provider — publisher is a client that deletes
        // all pieces after distribution. Storage nodes announce themselves when they
        // receive the manifest via PushRecord handler.
        if let Some(ref command_tx) = self.command_tx {
            debug!("Publishing manifest for {} to DHT (without provider announcement)", result.content_id);
            
            let (reply_tx, reply_rx) = oneshot::channel();
            let command = CraftObjCommand::AnnounceProvider {
                content_id: result.content_id,
                manifest,
                
                reply_tx,
            };
            
            if let Err(e) = command_tx.send(command) {
                warn!("[handler.rs] Failed to send announce command: {}", e);
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
                        warn!("[handler.rs] Failed to announce {} to DHT: {}", result.content_id, e);
                    }
                    Err(e) => {
                        warn!("[handler.rs] DHT announcement reply channel closed: {}", e);
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
            let _ = command_tx.send(CraftObjCommand::TriggerDistribution);
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
                p.push(format!("craftobj-{}", &cid_hex[..8.min(cid_hex.len())]));
                p
            });
        let key = params
            .get("key")
            .and_then(|v| v.as_str())
            .map(|s| hex::decode(s).unwrap_or_default());

        // Optional byte-range: [range_start, range_end) — both in bytes, range_end exclusive.
        // If only range_start is given, returns from that offset to end of file.
        let range_start: Option<u64> = params.get("range_start").and_then(|v| v.as_u64());
        let range_end: Option<u64> = params.get("range_end").and_then(|v| v.as_u64());

        let cid =
            craftobj_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;

        // Try local-first fetch path: use locally cached manifest + peer scorer providers
        // This works even with few nodes where DHT routing tables are sparse
        let mut p2p_fetched = false;
        if let Some(ref command_tx) = self.command_tx {
            // Step 1: Try to get manifest locally; fall back to verification record
            let local_manifest = {
                let client = self.client.lock().await;
                let manifest_path = client.store().data_dir().join("manifests").join(format!("{}.json", cid.to_hex()));
                info!("[handler.rs] Checking manifest at {:?} (exists={})", manifest_path, manifest_path.exists());
                match client.store().get_record(&cid) {
                    Ok(m) => Some(m),
                    Err(_) => {
                        // No manifest found — content may not be published here
                        None
                    }
                }
            };

            if let Some(manifest) = local_manifest {
                info!("[handler.rs] Found manifest locally for {}", cid);

                // Step 2: Get providers from PieceMap (peers that hold pieces for this CID)
                let mut providers = Vec::new();
                if let Some(ref pm) = self.piece_map {
                    let map = pm.lock().await;
                    let local_node = map.local_node().to_vec();
                    // Collect unique providers across all segments
                    let mut seen = std::collections::HashSet::new();
                    for seg in 0..manifest.segment_count() as u32 {
                        for (node, _pid, _coeff) in map.pieces_for_segment(&cid, seg) {
                            if node == &local_node { continue; }
                            if seen.insert(node.clone()) {
                                if let Ok(peer_id) = libp2p::PeerId::from_bytes(node) {
                                    info!("[handler.rs] Provider {} found in PieceMap for {}", peer_id, cid);
                                    providers.push(peer_id);
                                }
                            }
                        }
                    }
                }

                info!("[handler.rs] Found {} providers for {} via PieceMap", providers.len(), cid);

                // Also resolve DHT providers and merge
                {
                    let (reply_tx, reply_rx) = oneshot::channel();
                    let command = CraftObjCommand::ResolveProviders {
                        content_id: cid,
                        reply_tx,
                    };
                    if command_tx.send(command).is_ok() {
                        if let Ok(Ok(dht_providers)) = reply_rx.await {
                            for p in dht_providers {
                                if self.local_peer_id.as_ref() != Some(&p) && !providers.contains(&p) {
                                    info!("[handler.rs] Adding DHT provider {} for {}", p, cid);
                                    providers.push(p);
                                }
                            }
                        }
                    }
                }

                if !providers.is_empty() {
                    info!("[handler.rs] Fetching {} from {} total providers", cid, providers.len());
                    match self.fetch_missing_pieces_from_peers(&cid, &manifest, &providers, command_tx).await {
                        Ok(()) => {
                            // Verify we actually have enough pieces for all segments
                            let all_segments_complete = {
                                let client = self.client.lock().await;
                                (0..manifest.segment_count() as u32).all(|seg_idx| {
                                    let k = manifest.k_for_segment(seg_idx as usize);
                                    let piece_ids = client.store().list_pieces(&cid, seg_idx).unwrap_or_default();
                                    let coeffs: Vec<Vec<u8>> = piece_ids.iter()
                                        .filter_map(|pid| client.store().get_piece(&cid, seg_idx, pid).ok())
                                        .map(|(_data, coeff)| coeff)
                                        .collect();
                                    let rank = if coeffs.is_empty() { 0 } else { craftec_erasure::check_independence(&coeffs) };
                                    if rank < k {
                                        info!("[handler.rs] Post-fetch check: segment {} still needs pieces (rank {}/{})", seg_idx, rank, k);
                                    }
                                    rank >= k
                                })
                            };
                            if all_segments_complete {
                                info!("[handler.rs] Successfully fetched all pieces via P2P");
                                p2p_fetched = true;
                            } else {
                                info!("[handler.rs] P2P fetch returned Ok but not all segments complete, falling through to DHT");
                            }
                        }
                        Err(e) => {
                            info!("[handler.rs] P2P fetch failed: {}", e);
                        }
                    }
                } else {
                    info!("[handler.rs] No providers found for {}", cid);
                }
            }

            // DHT fallback: try DHT resolution if first attempt didn't succeed
            if !p2p_fetched {
                info!("[handler.rs] Attempting DHT-only resolution for {}", cid);

                let (reply_tx, reply_rx) = oneshot::channel();
                let command = CraftObjCommand::ResolveProviders {
                    content_id: cid,
                    reply_tx,
                };

                if command_tx.send(command).is_ok() {
                    match reply_rx.await {
                        Ok(Ok(providers)) if !providers.is_empty() => {
                            info!("[handler.rs] Found {} providers for {} via DHT", providers.len(), cid);

                            // Bootstrap manifest from first piece if we don't have one locally.
                            // receive_pieces auto-creates the manifest from piece header fields.
                            let manifest = {
                                let client = self.client.lock().await;
                                match client.store().get_record(&cid) {
                                    Ok(m) => Some(m),
                                    Err(_) => {
                                        drop(client);
                                        info!("[handler.rs] No local manifest for {}, bootstrapping from first piece", cid);
                                        let mut bootstrapped = None;
                                        for provider in &providers {
                                            let (probe_tx, probe_rx) = tokio::sync::oneshot::channel();
                                            let probe_cmd = CraftObjCommand::FetchPieces {
                                                peer_id: *provider,
                                                content_id: cid,
                                                segment_index: 0,
                                                have_pieces: vec![],
                                                max_pieces: 1,
                                                reply_tx: probe_tx,
                                            };
                                            if command_tx.send(probe_cmd).is_ok() {
                                                if let Ok(Ok(Ok(pieces))) = tokio::time::timeout(
                                                    std::time::Duration::from_secs(10), probe_rx
                                                ).await {
                                                    if !pieces.is_empty() {
                                                        let client = self.client.lock().await;
                                                        if let Ok(m) = client.store().get_record(&cid) {
                                                            bootstrapped = Some(m);
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if bootstrapped.is_none() {
                                            info!("[handler.rs] Bootstrap piece fetch failed/timed out for {} from all providers", cid);
                                        }
                                        bootstrapped
                                    }
                                }
                            };

                            if let Some(manifest) = manifest {
                                info!("[handler.rs] Have manifest for {} ({} segments)", cid, manifest.segment_count());
                                if let Err(e) = self.fetch_missing_pieces_from_peers(&cid, &manifest, &providers, command_tx).await {
                                    info!("[handler.rs] DHT P2P piece transfer failed: {}, falling back to local reconstruction", e);
                                } else {
                                    info!("[handler.rs] Successfully fetched pieces via DHT P2P path");
                                }
                            } else {
                                info!("[handler.rs] No manifest available for {} — falling back to local reconstruction", cid);
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
        info!("[handler.rs] Using local reconstruction for {}", cid);
        let client = self.client.clone();
        let output_clone = output.clone();
        let is_encrypted = key.is_some(); // capture before key is moved into closure
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

        // SHA-256 integrity check: ContentId == SHA-256(decoded_content).
        // Detects silent decode corruption (rank-deficient recovery, bad coeff vectors, etc.)
        // Skip for encrypted content (cid hashes ciphertext, not plaintext; key was applied).
        if !is_encrypted {
            let check_output = output.clone();
            let check_cid = cid;
            let integrity_ok = tokio::task::spawn_blocking(move || -> Result<bool, String> {
                use sha2::{Digest, Sha256};
                use std::io::Read;
                let mut file = std::fs::File::open(&check_output)
                    .map_err(|e| format!("open for sha256: {e}"))?;
                let mut hasher = Sha256::new();
                let mut buf = [0u8; 65536];
                loop {
                    let n = file.read(&mut buf).map_err(|e| format!("read: {e}"))?;
                    if n == 0 { break; }
                    hasher.update(&buf[..n]);
                }
                let hash: [u8; 32] = hasher.finalize().into();
                Ok(hash == check_cid.0)
            })
            .await
            .map_err(|e| format!("sha256 task panicked: {e}"))??;

            if !integrity_ok {
                return Err(format!(
                    "SHA-256 integrity check FAILED for {} — decoded content does not match CID",
                    cid
                ));
            }
        }

        // If a byte range was requested, slice the decoded output file and
        // return both the full path and a range-extracted path.
        if let Some(start) = range_start {
            let full_path = output.clone();
            let range_path = {
                let mut rp = output.clone();
                let ext = rp.extension()
                    .map(|e| format!("{}.range", e.to_string_lossy()))
                    .unwrap_or_else(|| "range".to_string());
                rp.set_extension(ext);
                rp
            };
            let range_path_inner = range_path.clone(); // clone before move into closure

            let range_result = tokio::task::spawn_blocking(move || -> Result<u64, String> {
                use std::io::{Read, Seek, SeekFrom, Write};
                let mut src = std::fs::File::open(&full_path)
                    .map_err(|e| format!("open decoded file: {e}"))?;
                let total_size = src.metadata().map(|m| m.len()).unwrap_or(0);
                let end = range_end.unwrap_or(total_size).min(total_size);
                if start >= end {
                    return Err(format!("invalid range: start={start} >= end={end}"));
                }
                let len = end - start;
                src.seek(SeekFrom::Start(start))
                    .map_err(|e| format!("seek: {e}"))?;
                let mut buf = vec![0u8; len as usize];
                src.read_exact(&mut buf)
                    .map_err(|e| format!("read range: {e}"))?;
                let mut dst = std::fs::File::create(&range_path_inner)
                    .map_err(|e| format!("create range file: {e}"))?;
                dst.write_all(&buf)
                    .map_err(|e| format!("write range: {e}"))?;
                Ok(len)
            })
            .await
            .map_err(|e| format!("range task panicked: {e}"))??;

            return Ok(serde_json::json!({
                "path": output.to_string_lossy(),
                "range_path": range_path.to_string_lossy(),
                "range_start": start,
                "range_end": range_end,
                "range_bytes": range_result,
            }));
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
                    c.store().get_record(&item.content_id)
                        .map(|_m| String::new())
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
            let cid = craftobj_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;
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
        let receipts: Vec<&craftobj_core::StorageReceipt> =
            if let Some(cid_hex) = params.get("cid").and_then(|v| v.as_str()) {
                let cid = craftobj_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;
                store.query_by_cid(&cid).into_iter().map(|e| {
                    let crate::receipt_store::ReceiptEntry::Storage(r) = e; r
                }).collect()
            } else if let Some(node_hex) = params.get("node").and_then(|v| v.as_str()) {
                let node = parse_pubkey(node_hex)?;
                store.query_by_node(&node).into_iter().map(|e| {
                    let crate::receipt_store::ReceiptEntry::Storage(r) = e; r
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

    async fn handle_connected_peers(&self) -> Result<Value, String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let tx = self.command_tx.as_ref().ok_or("no command channel")?;
        tx.send(crate::commands::CraftObjCommand::ConnectedPeers { reply_tx })
            .map_err(|_| "command channel closed".to_string())?;
        let peers = reply_rx.await.map_err(|_| "reply channel closed".to_string())?;
        Ok(serde_json::json!({ "peers": peers }))
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
        let additional_pieces = params.as_ref()
            .and_then(|p| p.get("additional_pieces"))
            .and_then(|v| v.as_u64())
            .unwrap_or(1) as usize;
        let cid = extract_cid(params)?;

        let manifest = {
            let client = self.client.lock().await;
            client.store().get_record(&cid).map_err(|e| e.to_string())?
        };

        // Snapshot pieces before extend so we can emit PieceStored events for new ones
        let pieces_before: std::collections::HashMap<u32, std::collections::HashSet<[u8; 32]>> = {
            let client = self.client.lock().await;
            let mut before = std::collections::HashMap::new();
            for seg in 0..manifest.segment_count() as u32 {
                let ids = client.store().list_pieces(&cid, seg).unwrap_or_default();
                before.insert(seg, ids.into_iter().collect());
            }
            before
        };

        // Generate new pieces via recombination for each segment
        let result = {
            let client = self.client.lock().await;
            crate::health::heal_content(client.store(), &manifest, additional_pieces)
        };

        if result.pieces_generated == 0 {
            return Err(format!("failed to generate new pieces: {:?}", result.errors));
        }

        // Emit PieceStored events to PieceMap for newly generated pieces
        if let Some(ref pm) = self.piece_map {
            let client = self.client.lock().await;
            let local_node = self.local_peer_id.map(|p| p.to_bytes().to_vec()).unwrap_or_default();
            let mut map = pm.lock().await;
            for seg in 0..manifest.segment_count() as u32 {
                let ids_after = client.store().list_pieces(&cid, seg).unwrap_or_default();
                let before_set = pieces_before.get(&seg).cloned().unwrap_or_default();
                for pid in ids_after {
                    if !before_set.contains(&pid) {
                        if let Ok((_data, coefficients)) = client.store().get_piece(&cid, seg, &pid) {
                            let seq = map.next_seq();
                            let stored = craftobj_core::PieceStored {
                                node: local_node.clone(),
                                cid,
                                segment: seg,
                                piece_id: pid,
                                coefficients,
                                seq,
                                timestamp: std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                                signature: vec![],
                            };
                            map.apply_event(&craftobj_core::PieceEvent::Stored(stored));
                        }
                    }
                }
                map.track_segment(&cid, seg);
            }
        }

        // Announce as provider
        if let Some(ref command_tx) = self.command_tx {
            let (tx, rx) = oneshot::channel();
            command_tx.send(CraftObjCommand::AnnounceProvider {
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

    /// Fetch missing pieces from remote peers using ConnectionPool parallel transfer.
    ///
    /// Wraps `CraftObjCommand::FetchOnePieceRaw` in a `PieceRequester` adapter so that
    /// `ConnectionPool::fetch_segments` can drive all the network concurrency, linear
    /// independence checking, provider failure/replacement, and geo-scoring logic
    /// that lives in `craftobj-client/src/fetch.rs`.
    async fn fetch_missing_pieces_from_peers(
        &self,
        content_id: &craftobj_core::ContentId,
        manifest: &craftobj_core::ContentManifest,
        providers: &[libp2p::PeerId],
        command_tx: &tokio::sync::mpsc::UnboundedSender<CraftObjCommand>,
    ) -> Result<(), String> {
        use craftobj_client::{ConnectionPool, FetchConfig, PieceRequester, ProviderId};

        info!("[handler.rs] Fetching pieces for {} from {} providers (ConnectionPool)", content_id, providers.len());

        // Build geo-scored provider list using peer scorer
        let ranked_providers: Vec<libp2p::PeerId> = if let Some(ref scorer) = self.peer_scorer {
            let mut s = scorer.lock().await;
            s.rank_peers(providers)
        } else {
            providers.to_vec()
        };

        let provider_ids: Vec<ProviderId> = ranked_providers.iter()
            .map(|p| ProviderId(p.to_string()))
            .collect();

        // Adapter: implements PieceRequester via FetchOnePieceRaw command
        struct CommandChannelRequester {
            command_tx: tokio::sync::mpsc::UnboundedSender<CraftObjCommand>,
        }

        #[async_trait::async_trait]
        impl PieceRequester for CommandChannelRequester {
            async fn request_piece(
                &self,
                provider: &ProviderId,
                content_id: &craftobj_core::ContentId,
                segment_index: u32,
            ) -> craftobj_core::Result<(Vec<u8>, Vec<u8>)> {
                let peer_id = provider.0.parse::<libp2p::PeerId>()
                    .map_err(|_| craftobj_core::CraftObjError::StorageError("invalid peer id".into()))?;
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let cmd = CraftObjCommand::FetchOnePieceRaw {
                    peer_id,
                    content_id: *content_id,
                    segment_index,
                    reply_tx,
                };
                self.command_tx.send(cmd)
                    .map_err(|_| craftobj_core::CraftObjError::StorageError("command channel closed".into()))?;
                match reply_rx.await {
                    Ok(Ok(pair)) => Ok(pair),
                    Ok(Err(e)) => Err(craftobj_core::CraftObjError::StorageError(e)),
                    Err(_) => Err(craftobj_core::CraftObjError::StorageError("reply channel dropped".into())),
                }
            }
        }

        let requester = Arc::new(CommandChannelRequester { command_tx: command_tx.clone() });
        let mut pool = ConnectionPool::new(requester, provider_ids, FetchConfig::default());

        let segment_indices: Vec<u32> = (0..manifest.segment_count() as u32).collect();
        let pieces_by_segment = pool.fetch_segments(content_id, manifest, &segment_indices).await
            .map_err(|e| e.to_string())?;

        // Write fetched pieces to local FsStore
        let client = self.client.lock().await;
        for (seg_idx, pieces) in pieces_by_segment {
            for piece in pieces {
                use craftobj_store::piece_id_from_coefficients;
                let pid = piece_id_from_coefficients(&piece.coefficients);
                // Build a self-describing header from the manifest
                let header = craftobj_core::PieceHeader {
                    content_id: *content_id,
                    total_size: manifest.total_size,
                    segment_idx: seg_idx,
                    segment_count: manifest.segment_count() as u32,
                    k: manifest.k_for_segment(seg_idx as usize) as u32,
                    vtags_cid: manifest.vtags_cid,
                    coefficients: piece.coefficients.clone(),
                };
                if let Err(e) = client.store().store_piece_with_header(
                    content_id, seg_idx, &pid, &piece.data, &piece.coefficients, &header,
                ) {
                    warn!("[handler.rs] Failed to store piece for seg {}: {}", seg_idx, e);
                }
            }
        }

        Ok(())
    }


    // -- Content removal IPC handler --

    async fn handle_data_providers(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let cid_hex = params.get("cid").and_then(|v| v.as_str()).ok_or("missing 'cid'")?;
        let cid_bytes = hex::decode(cid_hex).map_err(|e| format!("invalid hex: {e}"))?;
        if cid_bytes.len() != 32 { return Err("CID must be 32 bytes".into()); }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&cid_bytes);
        let cid = craftobj_core::ContentId(arr);

        let command_tx = self.command_tx.as_ref().ok_or("no command channel")?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        command_tx.send(CraftObjCommand::ResolveProviders { content_id: cid, reply_tx: tx })
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
        let content_id = craftobj_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())?;

        // Emit PieceDropped events before deletion
        self.emit_pieces_dropped_for_content(&content_id).await;

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

        info!("[handler.rs] Deleted local content {}", content_id);
        Ok(serde_json::json!({ "deleted": true }))
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
        let receipt = craftobj_core::StorageReceipt {
            content_id: craftobj_core::ContentId::from_bytes(&[0u8; 32]),
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

        info!("[handler.rs] Config updated and saved");
        serde_json::to_value(&*config).map_err(|e| e.to_string())
    }

    // -----------------------------------------------------------------------
    // Health & Statistics RPC methods
    // -----------------------------------------------------------------------

    /// `content.health` — Per-CID health info.
    async fn handle_content_health(&self, params: Option<Value>) -> Result<Value, String> {
        let cid = extract_cid(params)?;
        let client = self.client.lock().await;
        let store = client.store();

        let manifest = store.get_record(&cid).map_err(|e| e.to_string())?;
        let pinned = client.is_pinned(&cid);

        // Collect local per-segment piece counts
        let seg_count = manifest.segment_count();
        let mut local_seg_pieces = vec![0usize; seg_count];
        let segments_list = store.list_segments(&cid).unwrap_or_default();
        for &seg in &segments_list {
            let pieces = store.list_pieces(&cid, seg).unwrap_or_default();
            if (seg as usize) < seg_count {
                local_seg_pieces[seg as usize] = pieces.len();
            }
        }
        let min_rank = local_seg_pieces.iter().copied().min().unwrap_or(0);
        let disk_usage = store.cid_disk_usage(&cid);
        drop(client);

        // Get tracker state
        let (name, k, stage, role) = if let Some(ref tracker) = self.content_tracker {
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
                )
            } else {
                (String::new(), 0, String::new(), String::new())
            }
        } else {
            (String::new(), 0, String::new(), String::new())
        };

        let cid_hex = cid.to_hex();
        let health = self.compute_network_health(&cid_hex, &local_seg_pieces, &manifest, true).await;

        // Demand signal status
        let has_demand = if let Some(ref dst) = self.demand_tracker {
            dst.lock().await.has_demand(&cid)
        } else {
            false
        };

        // Tier min ratio (hardcoded 1.5 for now — later from on-chain tier data)
        let tier_min_ratio: f64 = 1.5;

        // Build per-segment JSON with network data
        let mut segments_json: Vec<Value> = Vec::new();
        for seg_idx in 0..seg_count {
            let seg_k = manifest.k_for_segment(seg_idx);
            let local = local_seg_pieces.get(seg_idx).copied().unwrap_or(0);
            let network = health.seg_pieces.get(seg_idx).copied().unwrap_or(0);
            let target = (tier_min_ratio * seg_k as f64).ceil() as usize;
            let needs_repair = network < target;
            let needs_degradation = network > target && !has_demand;
            segments_json.push(serde_json::json!({
                "index": seg_idx,
                "local_pieces": local,
                "rank": local,
                "k": seg_k,
                "network_pieces": network,
                "network_reconstructable": network >= seg_k,
                "needs_repair": needs_repair,
                "needs_degradation": needs_degradation,
            }));
        }

        // Check if a health scan has ever completed for this CID and get last scan time
        let (health_scanned, last_scan_time) = if let Some(ref data_dir) = self.data_dir {
            let path = data_dir.join("health_history").join(format!("{}.jsonl", cid));
            if path.exists() {
                // Read last line to get the most recent scan timestamp
                let last_ts = std::fs::read_to_string(&path)
                    .ok()
                    .and_then(|contents| {
                        contents.lines().rev().find(|l| !l.is_empty()).and_then(|line| {
                            serde_json::from_str::<serde_json::Value>(line)
                                .ok()
                                .and_then(|v| v.get("timestamp").and_then(|t| t.as_u64()))
                        })
                    });
                (true, last_ts)
            } else {
                (false, None)
            }
        } else {
            (false, None)
        };

        Ok(serde_json::json!({
            "content_id": cid_hex,
            "name": name,
            "original_size": manifest.total_size,
            "segment_count": seg_count,
            "k": k,
            "segments": segments_json,
            "min_rank": min_rank,
            "health_ratio": health.health_ratio,
            "local_health_ratio": if k > 0 { min_rank as f64 / k as f64 } else { 0.0 },
            "network_total_pieces": health.total_pieces,
            "provider_count": health.provider_count,
            "providers": health.providers,
            "pinned": pinned,
            "role": role,
            "stage": stage,
            "local_disk_usage": disk_usage,
            "has_demand": has_demand,
            "tier_min_ratio": tier_min_ratio,
            "health_scanned": health_scanned,
            "last_scan_time": last_scan_time,
            "health_scan_interval_secs": crate::health_scan::DEFAULT_SCAN_INTERVAL_SECS,
        }))
    }

    /// `content.list_detailed` — Enhanced list with health info per CID.
    /// `content.health_history` — Load health timeline snapshots for a CID.
    async fn handle_content_health_history(&self, params: Option<Value>) -> Result<Value, String> {
        let cid = extract_cid(params.clone())?;
        let since = params
            .and_then(|p| p.get("since").and_then(|v| v.as_u64()));

        let data_dir = self.data_dir.as_ref()
            .ok_or_else(|| "No data directory configured".to_string())?;

        let path = data_dir.join("health_history").join(format!("{}.jsonl", cid));
        let file = std::fs::File::open(&path)
            .map_err(|_| "No health history available for this content".to_string())?;

        let reader = std::io::BufReader::new(file);
        let cutoff = since.unwrap_or_else(|| {
            // Default: last 1 hour
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64 - 3_600_000
        });

        let snapshots: Vec<craftobj_core::HealthSnapshot> = std::io::BufRead::lines(reader)
            .map_while(Result::ok)
            .filter_map(|line| serde_json::from_str(&line).ok())
            .filter(|s: &craftobj_core::HealthSnapshot| s.timestamp >= cutoff)
            .collect();

        serde_json::to_value(serde_json::json!({ "snapshots": snapshots }))
            .map_err(|e| e.to_string())
    }

    async fn handle_content_list_detailed(&self) -> Result<Value, String> {
        let client = self.client.lock().await;
        let items = client.list().map_err(|e| e.to_string())?;

        // Collect local per-segment piece counts and manifests per CID
        let mut cid_data: Vec<(craftobj_core::ContentId, Vec<usize>, Option<craftobj_core::ContentManifest>, u64)> = Vec::new();
        for item in &items {
            let manifest = client.store().get_record(&item.content_id).ok();
            let seg_count = manifest.as_ref().map(|m| m.segment_count()).unwrap_or(0);
            let mut local_seg_pieces = vec![0usize; seg_count];
            let segments = client.store().list_segments(&item.content_id).unwrap_or_default();
            for &seg in &segments {
                let pieces = client.store().list_pieces(&item.content_id, seg).unwrap_or_default();
                if (seg as usize) < seg_count {
                    local_seg_pieces[seg as usize] = pieces.len();
                }
            }
            let disk_usage = client.store().cid_disk_usage(&item.content_id);
            cid_data.push((item.content_id, local_seg_pieces, manifest, disk_usage));
        }
        drop(client);

        // Tracker data
        let tracker_data: Option<Vec<_>> = if let Some(ref tracker) = self.content_tracker {
            let t = tracker.lock().await;
            Some(items.iter().map(|item| t.get(&item.content_id).cloned()).collect())
        } else {
            None
        };

        let mut result = Vec::new();
        for (i, item) in items.iter().enumerate() {
            let (_, ref local_seg_pieces, ref manifest, disk_usage) = cid_data[i];
            let cid_hex = item.content_id.to_hex();

            // Use shared helper (no provider details needed for list view)
            let health = if let Some(ref m) = manifest {
                self.compute_network_health(&cid_hex, local_seg_pieces, m, false).await
            } else {
                CidNetworkHealth {
                    seg_pieces: vec![],
                    total_pieces: 0,
                    health_ratio: 0.0,
                    provider_count: 0,
                    providers: vec![],
                }
            };

            // Demand signal status
            let has_demand = if let Some(ref dst) = self.demand_tracker {
                dst.lock().await.has_demand(&item.content_id)
            } else {
                false
            };
            let tier_min_ratio: f64 = 1.5;

            // Check if a health scan has ever completed for this CID
            let health_scanned = if let Some(ref data_dir) = self.data_dir {
                data_dir.join("health_history").join(format!("{}.jsonl", cid_hex)).exists()
            } else {
                false
            };

            let mut obj = serde_json::json!({
                "content_id": cid_hex,
                "total_size": item.total_size,
                "segment_count": item.segment_count,
                "pinned": item.pinned,
                "min_rank": local_seg_pieces.iter().copied().min().unwrap_or(0),
                "health_ratio": health.health_ratio,
                "provider_count": health.provider_count,
                "network_total_pieces": health.total_pieces,
                "local_disk_usage": disk_usage,
                "has_demand": has_demand,
                "tier_min_ratio": tier_min_ratio,
                "health_scanned": health_scanned,
            });

            if let Some(ref td) = tracker_data {
                if let Some(ref state) = td[i] {
                    obj["name"] = serde_json::json!(state.name);
                    obj["encrypted"] = serde_json::json!(state.encrypted);
                    obj["stage"] = serde_json::json!(state.stage.to_string());
                    obj["local_pieces"] = serde_json::json!(state.local_pieces);
                    obj["role"] = serde_json::json!(match state.role {
                        crate::content_tracker::ContentRole::Publisher => "publisher",
                        crate::content_tracker::ContentRole::StorageProvider => "storage_provider",
                    });
                    obj["hot"] = serde_json::json!(health.provider_count > state.segment_count * 2);
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

        // Aggregate total piece counts from PieceMap
        let network_piece_counts: std::collections::HashMap<String, usize> = if let Some(ref pm) = self.piece_map {
            let map = pm.lock().await;
            let mut cid_pieces = std::collections::HashMap::new();
            for cid in map.all_cids() {
                let total = map.providers(&cid).len();
                if total > 0 {
                    cid_pieces.insert(cid.to_hex(), total);
                }
            }
            cid_pieces
        } else {
            std::collections::HashMap::new()
        };

        let mut total_content = 0usize;
        let mut total_local_bytes = 0u64;
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
            let client = self.client.lock().await;
            total_content = states.len();
            for state in &states {
                let cid_hex = state.content_id.to_hex();
                let disk_usage = client.store().cid_disk_usage(&state.content_id);
                total_local_bytes += disk_usage;

                // Use network-wide piece counts for health assessment
                let network_pieces = network_piece_counts.get(&cid_hex).copied().unwrap_or(0);
                let k = state.k;
                let seg_count = state.segment_count.max(1);

                if k > 0 {
                    // Network pieces spread across segments — estimate per-segment availability
                    let avg_per_segment = network_pieces as f64 / seg_count as f64;
                    let ratio = avg_per_segment / k as f64;
                    health_sum += ratio;
                    if avg_per_segment >= k as f64 {
                        healthy += 1;
                    } else {
                        degraded += 1;
                    }
                } else {
                    // Fallback to local assessment
                    let segments = client.store().list_segments(&state.content_id).unwrap_or_default();
                    let mut all_ok = true;
                    let mut min_ratio: Option<f64> = None;
                    for &seg in &segments {
                        let pieces = client.store().list_pieces(&state.content_id, seg).unwrap_or_default();
                        let manifest = client.store().get_record(&state.content_id).ok();
                        let seg_k = manifest.as_ref().map(|m| m.k_for_segment(seg as usize)).unwrap_or(0);
                        if seg_k > 0 {
                            let ratio = pieces.len() as f64 / seg_k as f64;
                            min_ratio = Some(min_ratio.map_or(ratio, |r: f64| r.min(ratio)));
                            if pieces.len() < seg_k { all_ok = false; }
                        }
                    }
                    health_sum += min_ratio.unwrap_or(0.0);
                    if all_ok { healthy += 1; } else { degraded += 1; }
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

        // total_stored_bytes now reflects network-wide storage from peer_scorer
        let total_network_used = storage_summary.as_ref().map(|s| s.total_used).unwrap_or(0);

        Ok(serde_json::json!({
            "total_content_count": total_content,
            "total_stored_bytes": total_network_used,
            "total_local_bytes": total_local_bytes,
            "total_network_storage_committed": storage_summary.as_ref().map(|s| s.total_committed).unwrap_or(0),
            "total_network_storage_used": total_network_used,
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

        let max_storage_bytes = if let Some(ref cfg) = self.daemon_config {
            let c = cfg.lock().await;
            c.max_storage_bytes
        } else {
            0
        };

        Ok(serde_json::json!({
            "content_count": content_count,
            "published_count": published_count,
            "stored_count": stored_count,
            "total_local_pieces": total_local_pieces,
            "total_disk_usage": total_disk_usage,
            "max_storage_bytes": max_storage_bytes,
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
        let manifest = store.get_record(&cid).ok();

        let segments_list = store.list_segments(&cid).unwrap_or_default();
        let mut segments_json = Vec::new();
        for &seg in &segments_list {
            let pieces = store.list_pieces(&cid, seg).unwrap_or_default();
            let piece_ids: Vec<String> = pieces.iter().map(hex::encode).collect();
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

    // -----------------------------------------------------------------------
    // Aggregator RPC
    // -----------------------------------------------------------------------

    async fn handle_aggregator_status(&self) -> Result<Value, String> {
        // TODO: wire up real aggregator state once shared state is plumbed through
        Ok(serde_json::json!({
            "running": false,
            "last_epoch": 0,
            "receipts_collected": 0,
            "last_merkle_root": null,
            "next_epoch_secs": 0
        }))
    }

    // ── KV Store (file-based, in data_dir/kv/) ──────────────────────────

    fn kv_dir(&self) -> std::result::Result<PathBuf, String> {
        let dir = self.data_dir.as_ref()
            .ok_or("data_dir not configured")?
            .join("kv");
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        Ok(dir)
    }

    fn kv_path(&self, key: &str) -> std::result::Result<PathBuf, String> {
        // Sanitize key to filesystem-safe name
        let safe: String = key.chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == ':' { c } else { '_' })
            .collect();
        if safe.is_empty() {
            return Err("empty key".into());
        }
        Ok(self.kv_dir()?.join(safe))
    }

    /// `kv.put` — Store a key-value pair. Params: `{"key": "...", "value": "..."}`
    async fn handle_kv_put(&self, params: Option<Value>) -> std::result::Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let key = params.get("key").and_then(|v| v.as_str()).ok_or("missing 'key'")?;
        let value = params.get("value").and_then(|v| v.as_str()).ok_or("missing 'value'")?;
        let path = self.kv_path(key)?;
        std::fs::write(&path, value).map_err(|e| e.to_string())?;
        debug!("kv.put: {} = {} ({} bytes)", key, &value[..value.len().min(32)], value.len());
        Ok(serde_json::json!({"ok": true}))
    }

    /// `kv.get` — Retrieve a value by key. Params: `{"key": "..."}`
    async fn handle_kv_get(&self, params: Option<Value>) -> std::result::Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let key = params.get("key").and_then(|v| v.as_str()).ok_or("missing 'key'")?;
        let path = self.kv_path(key)?;
        match std::fs::read_to_string(&path) {
            Ok(value) => Ok(serde_json::json!({"key": key, "value": value})),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(serde_json::json!({"key": key, "value": null}))
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// `kv.delete` — Delete a key. Params: `{"key": "..."}`
    async fn handle_kv_delete(&self, params: Option<Value>) -> std::result::Result<Value, String> {
        let params = params.ok_or("missing params")?;
        let key = params.get("key").and_then(|v| v.as_str()).ok_or("missing 'key'")?;
        let path = self.kv_path(key)?;
        let existed = path.exists();
        if existed {
            std::fs::remove_file(&path).map_err(|e| e.to_string())?;
        }
        Ok(serde_json::json!({"deleted": existed}))
    }

    /// `kv.list` — List keys, optionally filtered by prefix. Params: `{"prefix": "..."}`
    async fn handle_kv_list(&self, params: Option<Value>) -> std::result::Result<Value, String> {
        let prefix = params.as_ref()
            .and_then(|p| p.get("prefix"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let dir = self.kv_dir()?;
        let mut keys = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with(prefix) {
                        keys.push(name.to_string());
                    }
                }
            }
        }
        keys.sort();
        Ok(serde_json::json!({"keys": keys}))
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

impl IpcHandler for CraftObjHandler {
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
                "connected_peers" => self.handle_connected_peers().await,
                "node.capabilities" => self.handle_node_capabilities().await,
                "network.storage" => self.handle_network_storage().await,
                "extend" => self.handle_extend(params).await,
                "receipts.count" => self.handle_receipts_count().await,
                "receipts.query" => self.handle_receipts_query(params).await,
                "receipt.storage.list" => self.handle_storage_receipt_list(params).await,
                "data.providers" => self.handle_data_providers(params).await,
                "data.delete_local" => self.handle_data_delete_local(params).await,
                "settlement.create_pool" => self.handle_settlement_create_pool(params).await,
                "settlement.fund_pool" => self.handle_settlement_fund_pool(params).await,
                "settlement.claim" => self.handle_settlement_claim(params).await,
                "settlement.open_channel" => self.handle_settlement_open_channel(params).await,
                "settlement.close_channel" => self.handle_settlement_close_channel(params).await,
                "get-config" => self.handle_get_config().await,
                "set-config" => self.handle_set_config(params).await,
                "content.health" => self.handle_content_health(params).await,
                "content.health_history" => self.handle_content_health_history(params).await,
                "content.list_detailed" => self.handle_content_list_detailed().await,
                "content.segments" => self.handle_content_segments(params).await,
                "network.health" => self.handle_network_health().await,
                "node.stats" => self.handle_node_stats().await,
                "aggregator.status" => self.handle_aggregator_status().await,
                "kv.put" => self.handle_kv_put(params).await,
                "kv.get" => self.handle_kv_get(params).await,
                "kv.delete" => self.handle_kv_delete(params).await,
                "kv.list" => self.handle_kv_list(params).await,
                "shutdown" => {
                    info!("[handler.rs] Shutdown requested via RPC");
                    // Emit event
                    if let Some(ref etx) = self.event_sender {
                        let _ = etx.send(DaemonEvent::PeerGoingOffline {
                            peer_id: "self".to_string(),
                        });
                    }
                    // Respond first, then signal shutdown
                    let result = Ok(serde_json::json!({"status": "shutting_down"}));
                    if let Some(ref notify) = self.shutdown_notify {
                        let notify = notify.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            notify.notify_one();
                        });
                    } else {
                        // No shutdown_notify set — log warning but don't kill the process.
                        // The caller (CraftStudio/CLI) should abort the daemon task directly.
                        warn!("[handler.rs] Shutdown requested but no shutdown_notify set — ignoring");
                    }
                    result
                },
                _ => Err(format!("unknown method: {}", method)),
            }
        })
    }
}

fn extract_cid(params: Option<Value>) -> Result<craftobj_core::ContentId, String> {
    let params = params.ok_or("missing params")?;
    let cid_hex = params
        .get("cid")
        .and_then(|v| v.as_str())
        .ok_or("missing 'cid' param")?;
    craftobj_core::ContentId::from_hex(cid_hex).map_err(|e| e.to_string())
}
