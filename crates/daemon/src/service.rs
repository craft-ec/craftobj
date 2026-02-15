//! DataCraft daemon service
//!
//! Manages the libp2p swarm, IPC server, and content operations.

use std::collections::HashMap;
use std::sync::Arc;

use craftec_ipc::IpcServer;
use craftec_network::{build_swarm, NetworkConfig};
use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, ChunkManifest, CapabilityAnnouncement, DataCraftCapability};
use libp2p::identity::Keypair;
use tokio::sync::{mpsc, Mutex, oneshot};
use tracing::{debug, error, info, warn};

use crate::commands::DataCraftCommand;
use crate::handler::DataCraftHandler;
use crate::protocol::{DataCraftProtocol, DataCraftEvent};

/// Default socket path for the DataCraft daemon.
pub fn default_socket_path() -> String {
    if cfg!(target_os = "linux") {
        if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
            return format!("{}/datacraft.sock", dir);
        }
    }
    "/tmp/datacraft.sock".to_string()
}

/// Default data directory for DataCraft storage.
pub fn default_data_dir() -> std::path::PathBuf {
    let base = dirs_data_dir().unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
    base.join("datacraft")
}

fn dirs_data_dir() -> Option<std::path::PathBuf> {
    #[cfg(target_os = "macos")]
    {
        dirs_home().map(|h| h.join("Library/Application Support"))
    }
    #[cfg(target_os = "linux")]
    {
        std::env::var("XDG_DATA_HOME")
            .ok()
            .map(std::path::PathBuf::from)
            .or_else(|| dirs_home().map(|h| h.join(".local/share")))
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var("APPDATA")
            .ok()
            .map(std::path::PathBuf::from)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        None
    }
}

fn dirs_home() -> Option<std::path::PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(std::path::PathBuf::from)
}

/// Peer capability tracker — stores latest known capabilities per peer.
type PeerCapabilities = Arc<Mutex<HashMap<libp2p::PeerId, (Vec<DataCraftCapability>, u64)>>>;

/// Tracks pending DHT requests from IPC commands.
#[derive(Debug)]
enum PendingRequest {
    ResolveProviders {
        reply_tx: oneshot::Sender<Result<Vec<libp2p::PeerId>, String>>,
    },
    GetManifest {
        reply_tx: oneshot::Sender<Result<ChunkManifest, String>>,
    },
    GetAccessList {
        reply_tx: oneshot::Sender<Result<datacraft_core::access::AccessList, String>>,
    },
}

/// Global pending requests tracker.
type PendingRequests = Arc<Mutex<HashMap<ContentId, PendingRequest>>>;

/// Run the DataCraft daemon.
///
/// This starts the libp2p swarm and IPC server, then blocks until shutdown.
pub async fn run_daemon(
    keypair: Keypair,
    data_dir: std::path::PathBuf,
    socket_path: String,
    network_config: NetworkConfig,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Build client and shared store
    let client = DataCraftClient::new(&data_dir)?;
    let client = Arc::new(Mutex::new(client));
    
    let store = Arc::new(Mutex::new(datacraft_store::FsStore::new(&data_dir)?));

    // Build swarm
    let (mut swarm, local_peer_id) = build_swarm(keypair.clone(), network_config).await
        .map_err(|e| format!("Failed to build swarm: {}", e))?;
    info!("DataCraft node started: {}", local_peer_id);

    // Create event channel for protocol communication
    let (protocol_event_tx, mut protocol_event_rx) = mpsc::unbounded_channel::<DataCraftEvent>();
    
    // Create command channel for IPC → swarm communication
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<DataCraftCommand>();
    let command_tx_for_caps = command_tx.clone();
    
    // Create pending requests tracker for DHT operations
    let pending_requests: PendingRequests = Arc::new(Mutex::new(HashMap::new()));

    // Create and register DataCraft protocol
    let mut protocol = DataCraftProtocol::new(store.clone(), protocol_event_tx);

    // Extract ed25519 signing key from libp2p keypair for receipt signing
    if let Ok(ed25519_kp) = keypair.clone().try_into_ed25519() {
        let secret_bytes = ed25519_kp.secret();
        let signing_key = ed25519_dalek::SigningKey::from_bytes(secret_bytes.as_ref().try_into().expect("ed25519 secret is 32 bytes"));
        protocol.set_signing_key(signing_key);
        debug!("Configured protocol with ed25519 signing key for TransferReceipts");
    }

    let (incoming_streams, coord_streams) = protocol.register(&mut swarm)
        .map_err(|e| format!("Failed to register DataCraft protocol: {}", e))?;

    // Subscribe to gossipsub topics
    if let Err(e) = swarm
        .behaviour_mut()
        .subscribe_topic(datacraft_core::NODE_STATUS_TOPIC)
    {
        error!("Failed to subscribe to node status: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut()
        .subscribe_topic(datacraft_core::CAPABILITIES_TOPIC)
    {
        error!("Failed to subscribe to capabilities topic: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut()
        .subscribe_topic(datacraft_core::REMOVAL_TOPIC)
    {
        error!("Failed to subscribe to removal topic: {:?}", e);
    }

    // Peer capabilities tracker
    let peer_capabilities: PeerCapabilities = Arc::new(Mutex::new(HashMap::new()));

    // Removal cache for fast local checks
    let removal_cache = Arc::new(Mutex::new(crate::removal_cache::RemovalCache::new()));

    // Start IPC server with enhanced handler
    let ipc_server = IpcServer::new(&socket_path);
    // Persistent receipt store
    let receipts_path = std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join(".datacraft")
        .join("receipts.bin");
    let receipt_store = Arc::new(Mutex::new(
        crate::receipt_store::PersistentReceiptStore::new(receipts_path)
            .expect("failed to open receipt store"),
    ));

    // Wire persistent receipt store into the protocol handler
    protocol.set_persistent_receipt_store(receipt_store.clone());

    // Wire removal cache into the protocol for pre-serve checks
    protocol.set_removal_cache(removal_cache.clone());

    let protocol = Arc::new(protocol);

    // Payment channel store
    let channels_path = std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join(".datacraft")
        .join("channels");
    let channel_store = Arc::new(Mutex::new(
        crate::channel_store::ChannelStore::new(channels_path)
            .expect("failed to open channel store"),
    ));

    // Initialize settlement client (env-driven: set CRAFTEC_SOLANA_RPC_URL for real RPC)
    let settlement_config = crate::settlement::SettlementConfig::from_env();
    let mut settlement_client = crate::settlement::SolanaClient::new(settlement_config)
        .expect("failed to create settlement client");
    if let Ok(ed25519_kp) = keypair.clone().try_into_ed25519() {
        let secret_bytes = ed25519_kp.secret();
        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            secret_bytes.as_ref().try_into().expect("ed25519 secret is 32 bytes"),
        );
        settlement_client.set_signing_key(signing_key);
    }
    let settlement_client = Arc::new(Mutex::new(settlement_client));

    let mut handler = DataCraftHandler::new(client.clone(), protocol.clone(), command_tx, peer_capabilities.clone(), receipt_store.clone(), channel_store);
    handler.set_settlement_client(settlement_client);
    let handler = Arc::new(handler);

    info!("Starting IPC server on {}", socket_path);

    // Own capabilities — read from CRAFTEC_CAPABILITIES env (comma-separated)
    // e.g., CRAFTEC_CAPABILITIES=storage,client,aggregator
    // Default: Storage + Client
    let own_capabilities = match std::env::var("CRAFTEC_CAPABILITIES") {
        Ok(caps) => {
            let mut result = Vec::new();
            for cap in caps.split(',').map(|s| s.trim().to_lowercase()) {
                match cap.as_str() {
                    "storage" => result.push(DataCraftCapability::Storage),
                    "client" => result.push(DataCraftCapability::Client),
                    _ => warn!("Unknown capability '{}', skipping", cap),
                }
            }
            if result.is_empty() {
                vec![DataCraftCapability::Client]
            } else {
                result
            }
        }
        Err(_) => vec![DataCraftCapability::Storage, DataCraftCapability::Client],
    };

    // Create challenger manager
    let local_pubkey = crate::pdp::peer_id_to_local_pubkey(&local_peer_id);
    let mut challenger_mgr = crate::challenger::ChallengerManager::new(
        local_peer_id,
        local_pubkey,
        command_tx_for_caps.clone(),
    );
    if let Ok(ed25519_kp) = keypair.clone().try_into_ed25519() {
        let secret_bytes = ed25519_kp.secret();
        let signing_key = ed25519_dalek::SigningKey::from_bytes(
            secret_bytes.as_ref().try_into().expect("ed25519 secret is 32 bytes"),
        );
        challenger_mgr.set_signing_key(signing_key);
    }
    challenger_mgr.set_persistent_store(receipt_store.clone());
    let challenger_mgr = Arc::new(Mutex::new(challenger_mgr));

    // Run all components concurrently
    tokio::select! {
        result = ipc_server.run(handler) => {
            if let Err(e) = result {
                error!("IPC server error: {}", e);
            }
        }
        _ = drive_swarm(&mut swarm, protocol.clone(), &mut command_rx, pending_requests.clone(), peer_capabilities.clone(), removal_cache.clone()) => {
            info!("Swarm event loop ended");
        }
        _ = handle_incoming_streams(incoming_streams, protocol.clone()) => {
            info!("Incoming streams handler ended");
        }
        _ = handle_incoming_coord_streams(coord_streams, protocol.clone()) => {
            info!("Incoming coord streams handler ended");
        }
        _ = handle_protocol_events(&mut protocol_event_rx, pending_requests.clone()) => {
            info!("Protocol events handler ended");
        }
        _ = announce_capabilities_periodically(&local_peer_id, own_capabilities, command_tx_for_caps) => {
            info!("Capability announcement loop ended");
        }
        _ = run_challenger_loop(challenger_mgr, store.clone()) => {
            info!("Challenger loop ended");
        }
    }

    Ok(())
}

/// Drive the swarm event loop.
/// Type alias for shared removal cache.
type SharedRemovalCache = Arc<Mutex<crate::removal_cache::RemovalCache>>;

async fn drive_swarm(
    swarm: &mut craftec_network::CraftSwarm,
    protocol: Arc<DataCraftProtocol>,
    command_rx: &mut mpsc::UnboundedReceiver<DataCraftCommand>,
    pending_requests: PendingRequests,
    peer_capabilities: PeerCapabilities,
    removal_cache: SharedRemovalCache,
) {
    use libp2p::swarm::SwarmEvent;
    use libp2p::futures::StreamExt;

    loop {
        tokio::select! {
            // Handle swarm events
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("Listening on {}", address);
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                        info!("Connected to {}", peer_id);
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        info!("Disconnected from {}", peer_id);
                    }
                    SwarmEvent::Behaviour(event) => {
                        debug!("Behaviour event: {:?}", event);
                        // Try to extract gossipsub capability announcements before passing through
                        handle_gossipsub_capability(&event, &peer_capabilities).await;
                        handle_gossipsub_removal(&event, &removal_cache).await;
                        // Pass events to our protocol handler
                        protocol.handle_swarm_event(&SwarmEvent::Behaviour(event)).await;
                    }
                    _ => {}
                }
            }
            
            // Handle commands from IPC handler
            command = command_rx.recv() => {
                if let Some(cmd) = command {
                    handle_command(swarm, &protocol, cmd, pending_requests.clone()).await;
                }
            }
        }
    }
}

/// Handle a command from the IPC handler.
async fn handle_command(
    swarm: &mut craftec_network::CraftSwarm,
    protocol: &DataCraftProtocol,
    command: DataCraftCommand,
    pending_requests: PendingRequests,
) {
    match command {
        DataCraftCommand::AnnounceProvider { content_id, manifest, reply_tx } => {
            debug!("Handling announce provider command for {}", content_id);
            
            // Get the local peer ID
            let local_peer_id = *swarm.local_peer_id();
            
            let result = async {
                // First announce as provider
                protocol.announce_provider(swarm.behaviour_mut(), &content_id).await
                    .map_err(|e| format!("Failed to announce provider: {}", e))?;
                
                // Then publish the manifest
                protocol.publish_manifest(swarm.behaviour_mut(), &manifest, &local_peer_id).await
                    .map_err(|e| format!("Failed to publish manifest: {}", e))?;
                
                debug!("Successfully started DHT operations for {}", content_id);
                Ok(())
            }.await;
            
            let _ = reply_tx.send(result);
        }
        
        DataCraftCommand::ResolveProviders { content_id, reply_tx } => {
            debug!("Handling resolve providers command for {}", content_id);
            
            match protocol.resolve_providers(swarm.behaviour_mut(), &content_id).await {
                Ok(()) => {
                    // Store the reply channel to respond when DHT query completes
                    let mut pending = pending_requests.lock().await;
                    pending.insert(content_id, PendingRequest::ResolveProviders { reply_tx });
                    debug!("Started DHT provider resolution for {}", content_id);
                }
                Err(e) => {
                    let _ = reply_tx.send(Err(format!("Failed to start provider resolution: {}", e)));
                }
            }
        }
        
        DataCraftCommand::GetManifest { content_id, reply_tx } => {
            debug!("Handling get manifest command for {}", content_id);
            
            match protocol.get_manifest(swarm.behaviour_mut(), &content_id).await {
                Ok(()) => {
                    // Store the reply channel to respond when DHT query completes
                    let mut pending = pending_requests.lock().await;
                    pending.insert(content_id, PendingRequest::GetManifest { reply_tx });
                    debug!("Started DHT manifest retrieval for {}", content_id);
                }
                Err(e) => {
                    let _ = reply_tx.send(Err(format!("Failed to start manifest retrieval: {}", e)));
                }
            }
        }
        
        DataCraftCommand::PublishCapabilities { capabilities } => {
            let local_peer_id = *swarm.local_peer_id();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let announcement = CapabilityAnnouncement {
                peer_id: local_peer_id.to_bytes(),
                capabilities,
                timestamp,
                signature: vec![], // TODO: sign with node keypair
            };
            match serde_json::to_vec(&announcement) {
                Ok(data) => {
                    if let Err(e) = swarm
                        .behaviour_mut()
                        .publish_to_topic(datacraft_core::CAPABILITIES_TOPIC, data)
                    {
                        debug!("Failed to publish capabilities: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to serialize capability announcement: {}", e);
                }
            }
        }
        
        DataCraftCommand::RequestShard { peer_id, content_id, chunk_index, shard_index, local_public_key, reply_tx } => {
            debug!("Handling request shard command: {}/{}/{} from {}", content_id, chunk_index, shard_index, peer_id);
            
            let result = protocol
                .request_shard_from_peer(swarm.behaviour_mut(), peer_id, &content_id, chunk_index, shard_index, &local_public_key)
                .await;
            
            let _ = reply_tx.send(result);
        }
        
        DataCraftCommand::QueryMaxShardIndex { peer_id, content_id, reply_tx } => {
            debug!("Querying max shard index for {} from {}", content_id, peer_id);
            
            let result = protocol
                .query_max_shard_index(swarm.behaviour_mut(), peer_id, &content_id)
                .await;
            
            let _ = reply_tx.send(result);
        }
        
        DataCraftCommand::Extend { content_id: _, reply_tx } => {
            // The extend flow is orchestrated by the handler, not here.
            warn!("Extend command received in swarm loop — should be orchestrated by handler");
            let _ = reply_tx.send(Err("Extend should be orchestrated by the IPC handler".into()));
        }

        DataCraftCommand::PutReKey { content_id, entry, reply_tx } => {
            debug!("Handling put re-key command for {} → {}", content_id, hex::encode(entry.recipient_did));
            let local_peer_id = *swarm.local_peer_id();
            let result = datacraft_routing::ContentRouter::put_re_key(
                swarm.behaviour_mut(), &content_id, &entry, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        DataCraftCommand::RemoveReKey { content_id, recipient_did, reply_tx } => {
            debug!("Handling remove re-key command for {} → {}", content_id, hex::encode(recipient_did));
            let local_peer_id = *swarm.local_peer_id();
            let result = datacraft_routing::ContentRouter::remove_re_key(
                swarm.behaviour_mut(), &content_id, &recipient_did, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        DataCraftCommand::PutAccessList { access_list, reply_tx } => {
            debug!("Handling put access list command for {}", access_list.content_id);
            let local_peer_id = *swarm.local_peer_id();
            let result = datacraft_routing::ContentRouter::put_access_list(
                swarm.behaviour_mut(), &access_list, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        DataCraftCommand::GetAccessList { content_id, reply_tx } => {
            debug!("Handling get access list command for {}", content_id);
            match protocol.get_access_list_dht(swarm.behaviour_mut(), &content_id).await {
                Ok(()) => {
                    let mut pending = pending_requests.lock().await;
                    pending.insert(content_id, PendingRequest::GetAccessList { reply_tx });
                }
                Err(e) => {
                    let _ = reply_tx.send(Err(format!("Failed to start access list retrieval: {}", e)));
                }
            }
        }

        DataCraftCommand::PublishRemoval { content_id, notice, reply_tx } => {
            debug!("Handling publish removal command for {}", content_id);
            let local_peer_id = *swarm.local_peer_id();

            // Store in DHT
            let dht_result = datacraft_routing::ContentRouter::put_removal_notice(
                swarm.behaviour_mut(), &content_id, &notice, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());

            // Broadcast via gossipsub
            if let Ok(data) = bincode::serialize(&notice) {
                if let Err(e) = swarm.behaviour_mut()
                    .publish_to_topic(datacraft_core::REMOVAL_TOPIC, data) {
                    warn!("Failed to broadcast removal notice via gossipsub: {:?}", e);
                }
            }

            let _ = reply_tx.send(dht_result);
        }

        DataCraftCommand::CheckRemoval { content_id, reply_tx } => {
            debug!("Handling check removal command for {}", content_id);
            // For now, just start a DHT query. Full async response would need pending request tracking.
            // This is a simplified version — the RemovalCache handles most checks locally.
            let _ = datacraft_routing::ContentRouter::get_removal_notice(
                swarm.behaviour_mut(), &content_id,
            );
            // Reply immediately with None — the cache should be checked first by the caller.
            let _ = reply_tx.send(Ok(None));
        }
    }
}

/// Handle gossipsub capability announcement messages.
async fn handle_gossipsub_capability(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    peer_capabilities: &PeerCapabilities,
) {
    use craftec_network::behaviour::CraftBehaviourEvent;
    use libp2p::gossipsub;

    if let CraftBehaviourEvent::Gossipsub(gossipsub::Event::Message {
        message, ..
    }) = event
    {
        let topic_str = message.topic.as_str();
        if topic_str == datacraft_core::CAPABILITIES_TOPIC {
            match serde_json::from_slice::<CapabilityAnnouncement>(&message.data) {
                Ok(ann) => {
                    if let Ok(peer_id) = libp2p::PeerId::from_bytes(&ann.peer_id) {
                        debug!(
                            "Received capability announcement from {}: {:?}",
                            peer_id, ann.capabilities
                        );
                        let mut caps = peer_capabilities.lock().await;
                        // Only update if newer
                        let dominated = caps
                            .get(&peer_id)
                            .map(|(_, ts)| *ts < ann.timestamp)
                            .unwrap_or(true);
                        if dominated {
                            caps.insert(peer_id, (ann.capabilities, ann.timestamp));
                        }
                    }
                }
                Err(e) => {
                    debug!("Failed to parse capability announcement: {}", e);
                }
            }
        }
    }
}

/// Handle gossipsub removal notice messages.
async fn handle_gossipsub_removal(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    removal_cache: &SharedRemovalCache,
) {
    use craftec_network::behaviour::CraftBehaviourEvent;
    use libp2p::gossipsub;

    if let CraftBehaviourEvent::Gossipsub(gossipsub::Event::Message {
        message, ..
    }) = event
    {
        let topic_str = message.topic.as_str();
        if topic_str == datacraft_core::REMOVAL_TOPIC {
            match bincode::deserialize::<datacraft_core::RemovalNotice>(&message.data) {
                Ok(notice) => {
                    if notice.verify() {
                        debug!(
                            "Received removal notice for {} from {}",
                            notice.cid, notice.creator
                        );
                        let mut cache = removal_cache.lock().await;
                        cache.insert(notice);
                    } else {
                        debug!("Received invalid removal notice, ignoring");
                    }
                }
                Err(e) => {
                    debug!("Failed to parse removal notice: {}", e);
                }
            }
        }
    }
}

/// Periodically publish own capabilities via gossipsub.
async fn announce_capabilities_periodically(
    _local_peer_id: &libp2p::PeerId,
    capabilities: Vec<DataCraftCapability>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
) {
    use std::time::Duration;

    // Initial delay before first announcement
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(300)); // Every 5 minutes
    loop {
        interval.tick().await;
        debug!("Publishing capability announcement");
        let _ = command_tx.send(DataCraftCommand::PublishCapabilities {
            capabilities: capabilities.clone(),
        });
    }
}

/// Handle incoming transfer streams from peers.
async fn handle_incoming_streams(
    mut incoming_streams: libp2p_stream::IncomingStreams,
    protocol: Arc<DataCraftProtocol>,
) {
    use futures::StreamExt;

    info!("Starting incoming streams handler");
    
    while let Some((peer, stream)) = incoming_streams.next().await {
        debug!("Received incoming stream from peer: {}", peer);
        
        // Spawn a task to handle this stream
        let protocol_clone = protocol.clone();
        tokio::spawn(async move {
            protocol_clone.handle_incoming_stream(stream).await;
        });
    }
}

/// Handle protocol events (DHT query results, etc.).
async fn handle_protocol_events(
    event_rx: &mut mpsc::UnboundedReceiver<DataCraftEvent>,
    pending_requests: PendingRequests,
) {
    info!("Starting protocol events handler");
    
    while let Some(event) = event_rx.recv().await {
        match event {
            DataCraftEvent::ProvidersResolved { content_id, providers } => {
                info!("Found {} providers for {}", providers.len(), content_id);
                
                // Find and respond to the waiting request
                let mut pending = pending_requests.lock().await;
                if let Some(PendingRequest::ResolveProviders { reply_tx }) = pending.remove(&content_id) {
                    let _ = reply_tx.send(Ok(providers));
                    debug!("Responded to provider resolution request for {}", content_id);
                }
            }
            
            DataCraftEvent::ManifestRetrieved { content_id, manifest } => {
                info!("Retrieved manifest for {} ({} chunks)", content_id, manifest.chunk_count);
                
                // Find and respond to the waiting request
                let mut pending = pending_requests.lock().await;
                if let Some(PendingRequest::GetManifest { reply_tx }) = pending.remove(&content_id) {
                    let _ = reply_tx.send(Ok(manifest));
                    debug!("Responded to manifest retrieval request for {}", content_id);
                }
            }
            
            DataCraftEvent::AccessListRetrieved { content_id, access_list } => {
                info!("Retrieved access list for {} ({} entries)", content_id, access_list.entries.len());
                let mut pending = pending_requests.lock().await;
                if let Some(PendingRequest::GetAccessList { reply_tx }) = pending.remove(&content_id) {
                    let _ = reply_tx.send(Ok(access_list));
                    debug!("Responded to access list retrieval request for {}", content_id);
                }
            }

            DataCraftEvent::DhtError { content_id, error } => {
                warn!("DHT error for {}: {}", content_id, error);
                
                // Find and respond to any waiting request with error
                let mut pending = pending_requests.lock().await;
                if let Some(pending_request) = pending.remove(&content_id) {
                    match pending_request {
                        PendingRequest::ResolveProviders { reply_tx } => {
                            let _ = reply_tx.send(Err(error));
                        }
                        PendingRequest::GetManifest { reply_tx } => {
                            let _ = reply_tx.send(Err(error));
                        }
                        PendingRequest::GetAccessList { reply_tx } => {
                            let _ = reply_tx.send(Err(error));
                        }
                    }
                    debug!("Responded to DHT request with error for {}", content_id);
                }
            }
        }
    }
}

/// Periodically run the challenger duty cycle.
async fn run_challenger_loop(
    challenger: Arc<Mutex<crate::challenger::ChallengerManager>>,
    store: Arc<Mutex<datacraft_store::FsStore>>,
) {
    use std::time::Duration;

    // Initial delay
    tokio::time::sleep(Duration::from_secs(30)).await;

    let mut interval = tokio::time::interval(crate::challenger::CHALLENGE_INTERVAL);
    loop {
        interval.tick().await;
        let store_guard = store.lock().await;
        let mut mgr = challenger.lock().await;
        let rounds = mgr.periodic_check(&store_guard).await;
        if rounds > 0 {
            info!("Challenger completed {} rounds", rounds);
        }
    }
}

/// Handle incoming shard coordination streams from peers.
async fn handle_incoming_coord_streams(
    mut incoming: libp2p_stream::IncomingStreams,
    protocol: Arc<DataCraftProtocol>,
) {
    use futures::StreamExt;

    info!("Starting incoming shard coordination streams handler");

    while let Some((peer, stream)) = incoming.next().await {
        debug!("Received incoming shard-coord stream from peer: {}", peer);

        let protocol_clone = protocol.clone();
        tokio::spawn(async move {
            protocol_clone.handle_incoming_coord_stream(stream).await;
        });
    }
}
