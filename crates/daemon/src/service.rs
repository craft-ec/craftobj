//! DataCraft daemon service
//!
//! Manages the libp2p swarm, IPC server, and content operations.

use std::collections::HashMap;
use std::sync::Arc;

use craftec_ipc::IpcServer;
use craftec_network::{build_swarm, NetworkConfig};
use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, ContentManifest, CapabilityAnnouncement, DataCraftCapability};
use libp2p::identity::Keypair;
use tokio::sync::{mpsc, Mutex, oneshot};
use tracing::{debug, error, info, warn};

use crate::commands::DataCraftCommand;
use crate::events::{self, DaemonEvent, EventSender};
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

/// Shared peer scorer — tracks capabilities and reliability per peer.
type SharedPeerScorer = Arc<Mutex<crate::peer_scorer::PeerScorer>>;

/// Tracks pending DHT requests from IPC commands.
#[derive(Debug)]
enum PendingRequest {
    ResolveProviders {
        reply_tx: oneshot::Sender<Result<Vec<libp2p::PeerId>, String>>,
    },
    GetManifest {
        reply_tx: oneshot::Sender<Result<ContentManifest, String>>,
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
    ws_port: u16,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    run_daemon_with_config(keypair, data_dir, socket_path, network_config, ws_port, None, None).await
}

/// Run the DataCraft daemon with an optional config file path override.
pub async fn run_daemon_with_config(
    keypair: Keypair,
    data_dir: std::path::PathBuf,
    socket_path: String,
    network_config: NetworkConfig,
    ws_port: u16,
    config_path: Option<std::path::PathBuf>,
    node_signing_key: Option<ed25519_dalek::SigningKey>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Load daemon config (writes defaults if no config file exists yet)
    let daemon_config = match config_path {
        Some(ref path) => {
            let cfg = crate::config::DaemonConfig::load_from(path);
            if !path.exists() {
                if let Err(e) = cfg.save_to(path) {
                    warn!("Failed to write default config to {:?}: {}", path, e);
                }
            }
            cfg
        }
        None => {
            let cfg = crate::config::DaemonConfig::load(&data_dir);
            let config_file = data_dir.join("config.json");
            if !config_file.exists() {
                if let Err(e) = cfg.save(&data_dir) {
                    warn!("Failed to write default config to {:?}: {}", config_file, e);
                }
            }
            cfg
        }
    };
    info!("Daemon config: capability_announce={}s, reannounce_interval={}s, reannounce_threshold={}s",
        daemon_config.capability_announce_interval_secs,
        daemon_config.reannounce_interval_secs,
        daemon_config.reannounce_threshold_secs,
    );

    // Create broadcast channel for daemon events (WS push)
    let (event_tx, _) = events::event_channel(256);

    // Build client and shared store
    let client = DataCraftClient::new(&data_dir)?;
    let client = Arc::new(Mutex::new(client));
    
    let store = Arc::new(Mutex::new(datacraft_store::FsStore::new(&data_dir)?));

    // Build storage Merkle tree from existing pieces
    let merkle_tree = {
        let tmp_store = datacraft_store::FsStore::new(&data_dir)?;
        let tree = datacraft_store::merkle::StorageMerkleTree::build_from_store(&tmp_store)
            .unwrap_or_else(|e| {
                warn!("Failed to build storage Merkle tree: {}, starting empty", e);
                datacraft_store::merkle::StorageMerkleTree::new()
            });
        info!("Storage Merkle tree built: {} leaves, root={}", tree.len(), hex::encode(&tree.root()[..8]));
        Arc::new(Mutex::new(tree))
    };

    // Build swarm
    let (mut swarm, local_peer_id) = build_swarm(keypair.clone(), network_config).await
        .map_err(|e| format!("Failed to build swarm: {}", e))?;
    info!("DataCraft node started: {}", local_peer_id);

    // Set Kademlia to server mode so DHT queries work (especially on localhost / LAN)
    swarm.behaviour_mut().kademlia.set_mode(Some(libp2p::kad::Mode::Server));

    // Create event channel for protocol communication
    let (protocol_event_tx, mut protocol_event_rx) = mpsc::unbounded_channel::<DataCraftEvent>();
    
    // Create command channel for IPC → swarm communication
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<DataCraftCommand>();
    let command_tx_for_caps = command_tx.clone();
    let command_tx_for_maintenance = command_tx.clone();
    let command_tx_for_events = command_tx.clone();
    
    // Create pending requests tracker for DHT operations
    let pending_requests: PendingRequests = Arc::new(Mutex::new(HashMap::new()));

    // Create and register DataCraft protocol
    let mut protocol = DataCraftProtocol::new(store.clone(), protocol_event_tx);

    let incoming_streams = protocol.register(&mut swarm)
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
    if let Err(e) = swarm
        .behaviour_mut()
        .subscribe_topic(datacraft_core::STORAGE_RECEIPT_TOPIC)
    {
        error!("Failed to subscribe to storage receipt topic: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut()
        .subscribe_topic(datacraft_core::REPAIR_TOPIC)
    {
        error!("Failed to subscribe to repair topic: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut()
        .subscribe_topic(datacraft_core::SCALING_TOPIC)
    {
        error!("Failed to subscribe to scaling topic: {:?}", e);
    }

    // Peer scorer — tracks capabilities and reliability
    let peer_scorer: SharedPeerScorer = Arc::new(Mutex::new(crate::peer_scorer::PeerScorer::new()));

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

    // Create demand tracker early so we can wire it into protocol before Arc wrapping
    let demand_tracker: Arc<Mutex<crate::scaling::DemandTracker>> = Arc::new(Mutex::new(crate::scaling::DemandTracker::new()));
    protocol.set_demand_tracker(demand_tracker.clone());

    // Eviction manager (created early to wire into protocol before Arc wrapping)
    let eviction_config = crate::eviction::EvictionConfig {
        max_storage_bytes: daemon_config.max_storage_bytes,
        enable_eviction: true,
    };
    let eviction_manager = Arc::new(Mutex::new(crate::eviction::EvictionManager::new(&eviction_config)));
    protocol.set_eviction_manager(eviction_manager.clone());

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

    // Content lifecycle tracker — import any existing content from store
    let content_tracker = {
        let mut tracker = crate::content_tracker::ContentTracker::with_threshold(
            &data_dir,
            daemon_config.reannounce_threshold_secs,
        );
        // Import existing content — use a temporary FsStore to avoid locking the async client
        if let Ok(tmp_store) = datacraft_store::FsStore::new(&data_dir) {
            let imported = tracker.import_from_store(&tmp_store);
            if imported > 0 {
                info!("Imported {} existing content items into tracker on startup", imported);
            }
        }
        Arc::new(Mutex::new(tracker))
    };

    // Own capabilities — read from config file (env var override applied during config load)
    let own_capabilities = {
        let mut result = Vec::new();
        for cap in &daemon_config.capabilities {
            match cap.to_lowercase().as_str() {
                "storage" => result.push(DataCraftCapability::Storage),
                "client" => result.push(DataCraftCapability::Client),
                _ => warn!("Unknown capability '{}' in config, skipping", cap),
            }
        }
        if result.is_empty() {
            vec![DataCraftCapability::Client]
        } else {
            result
        }
    };
    info!("Capabilities: {:?}", own_capabilities);

    let daemon_config_shared = Arc::new(Mutex::new(daemon_config.clone()));
    let mut handler = DataCraftHandler::new(client.clone(), protocol.clone(), command_tx.clone(), peer_scorer.clone(), receipt_store.clone(), channel_store);
    handler.set_settlement_client(settlement_client);
    handler.set_content_tracker(content_tracker.clone());
    handler.set_own_capabilities(own_capabilities.clone());
    handler.set_daemon_config(daemon_config_shared, data_dir.clone());
    handler.set_eviction_manager(eviction_manager.clone());
    handler.set_event_sender(event_tx.clone());
    if let Some(key) = node_signing_key {
        handler.set_node_signing_key(key);
    }
    let handler = Arc::new(handler);

    info!("Starting IPC server on {}", socket_path);

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
    challenger_mgr.set_peer_scorer(peer_scorer.clone());

    // Shared PDP rank data between challenger and eviction loops
    let pdp_ranks: Arc<Mutex<crate::challenger::PdpRankData>> = Arc::new(Mutex::new(HashMap::new()));
    challenger_mgr.set_pdp_ranks(pdp_ranks.clone());

    let challenger_mgr = Arc::new(Mutex::new(challenger_mgr));

    // Load or generate API key for WebSocket authentication
    let api_key = crate::api_key::load_or_generate(&data_dir)
        .map_err(|e| format!("Failed to load/generate API key: {}", e))?;
    info!("API key loaded for WebSocket authentication");

    // Start WebSocket server if enabled
    let ws_handler = handler.clone();
    let ws_event_tx = event_tx.clone();
    let ws_future = async {
        if ws_port > 0 {
            if let Err(e) = crate::ws_server::run_ws_server(ws_port, ws_handler, api_key, ws_event_tx).await {
                error!("WebSocket server error: {}", e);
            }
        } else {
            // WS disabled — park forever so select! doesn't short-circuit
            std::future::pending::<()>().await;
        }
    };

    let _ = event_tx.send(DaemonEvent::DaemonStarted { listen_addresses: vec![] });

    // Emit startup sequence events
    let _ = event_tx.send(DaemonEvent::DiscoveryStatus {
        total_peers: 0,
        storage_peers: 0,
        action: format!("Starting mDNS discovery and Kademlia bootstrap"),
    });

    if ws_port > 0 {
        let _ = event_tx.send(DaemonEvent::ListeningOn {
            address: format!("ws://0.0.0.0:{}", ws_port),
        });
    }

    // Repair coordinator for network-wide self-healing
    let repair_command_tx = command_tx.clone();
    let mut repair_coord = crate::repair::RepairCoordinator::new(local_peer_id, repair_command_tx);
    repair_coord.set_peer_scorer(peer_scorer.clone());
    let repair_coordinator: Arc<Mutex<crate::repair::RepairCoordinator>> = Arc::new(Mutex::new(repair_coord));

    // Scaling: coordinator for demand-driven piece acquisition
    let scaling_command_tx = command_tx.clone();
    let scaling_coordinator: Arc<Mutex<crate::scaling::ScalingCoordinator>> = Arc::new(Mutex::new(
        crate::scaling::ScalingCoordinator::new(local_peer_id, scaling_command_tx),
    ));

    // Aggregator config — configurable epoch, default 10 min
    let aggregator_config = crate::aggregator::AggregatorConfig {
        epoch_duration: std::time::Duration::from_secs(
            daemon_config.aggregation_epoch_secs.unwrap_or(600),
        ),
        pool_id: [0u8; 32], // TODO: load from config/on-chain
    };

    // Run all components concurrently
    tokio::select! {
        result = ipc_server.run(handler) => {
            if let Err(e) = result {
                error!("IPC server error: {}", e);
            }
        }
        _ = ws_future => {
            info!("WebSocket server ended");
        }
        _ = drive_swarm(&mut swarm, protocol.clone(), &mut command_rx, pending_requests.clone(), peer_scorer.clone(), removal_cache.clone(), own_capabilities.clone(), command_tx_for_caps.clone(), event_tx.clone(), content_tracker.clone(), client.clone(), daemon_config.max_storage_bytes, repair_coordinator.clone(), store.clone(), scaling_coordinator.clone(), demand_tracker.clone(), merkle_tree.clone()) => {
            info!("Swarm event loop ended");
        }
        _ = handle_incoming_streams(incoming_streams, protocol.clone()) => {
            info!("Incoming streams handler ended");
        }
        _ = handle_protocol_events(&mut protocol_event_rx, pending_requests.clone(), event_tx.clone(), content_tracker.clone(), command_tx_for_events) => {
            info!("Protocol events handler ended");
        }
        _ = announce_capabilities_periodically(&local_peer_id, own_capabilities, command_tx_for_caps, daemon_config.capability_announce_interval_secs, client.clone(), daemon_config.max_storage_bytes, daemon_config.region.clone(), merkle_tree.clone()) => {
            info!("Capability announcement loop ended");
        }
        _ = run_challenger_loop(challenger_mgr, store.clone(), event_tx.clone()) => {
            info!("Challenger loop ended");
        }
        _ = crate::reannounce::content_maintenance_loop(
            content_tracker,
            command_tx_for_maintenance.clone(),
            client.clone(),
            daemon_config.reannounce_interval_secs,
            event_tx.clone(),
            peer_scorer.clone(),
        ) => {
            info!("Content maintenance loop ended");
        }
        _ = scaling_maintenance_loop(demand_tracker, command_tx_for_maintenance, local_peer_id, peer_scorer.clone()) => {
            info!("Scaling maintenance loop ended");
        }
        _ = eviction_maintenance_loop(eviction_manager, store.clone(), event_tx.clone(), pdp_ranks.clone()) => {
            info!("Eviction maintenance loop ended");
        }
        _ = crate::aggregator::run_aggregation_loop(receipt_store.clone(), event_tx.clone(), aggregator_config) => {
            info!("Aggregation loop ended");
        }
    }

    Ok(())
}

/// Periodically check demand and broadcast signals for hot content.
async fn scaling_maintenance_loop(
    demand_tracker: Arc<Mutex<crate::scaling::DemandTracker>>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    local_peer_id: libp2p::PeerId,
    peer_scorer: Arc<Mutex<crate::peer_scorer::PeerScorer>>,
) {
    use std::time::Duration;
    // Initial delay
    tokio::time::sleep(Duration::from_secs(30)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(60));
    loop {
        interval.tick().await;
        let mut tracker = demand_tracker.lock().await;
        let hot_cids = tracker.check_demand();
        for (cid, demand_level) in hot_cids {
            // Check if there are non-provider storage peers who could accept new pieces.
            // We pass an empty provider list since content_tracker doesn't track per-CID
            // provider PeerIds yet — has_non_provider_targets checks if ANY storage peers
            // exist beyond the (empty) provider set, which is a safe over-approximation.
            let has_targets = crate::push_target::has_non_provider_targets(
                &local_peer_id,
                &[], // TODO: pass actual provider PeerIds when content_tracker tracks them
                &Some(peer_scorer.clone()),
            );
            if tracker.should_broadcast_demand(&cid, has_targets) {
                if !has_targets {
                    debug!("Skipping scaling notice for {}: no non-provider targets", cid);
                    continue;
                }
                let signal = crate::scaling::create_demand_signal(cid, demand_level, 0, &local_peer_id);
                if let Ok(data) = bincode::serialize(&signal) {
                    let _ = command_tx.send(DataCraftCommand::BroadcastDemandSignal {
                        signal_data: data,
                    });
                    info!("Broadcasting demand signal for {}: level={}", cid, demand_level);
                }
            }
        }
        tracker.cleanup();
    }
}

/// Periodically check storage pressure and evict free content.
async fn eviction_maintenance_loop(
    eviction_manager: Arc<Mutex<crate::eviction::EvictionManager>>,
    store: Arc<Mutex<datacraft_store::FsStore>>,
    event_tx: EventSender,
    pdp_ranks: Arc<Mutex<crate::challenger::PdpRankData>>,
) {
    use std::time::Duration;
    // Initial delay
    tokio::time::sleep(Duration::from_secs(60)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(300));
    loop {
        interval.tick().await;
        let store_guard = store.lock().await;
        let mut mgr = eviction_manager.lock().await;

        // Build retirement data from latest PDP rank snapshots
        let ranks_snapshot = pdp_ranks.lock().await;
        let mut k_by_cid = HashMap::new();
        let mut segment_ranks_by_cid = HashMap::new();
        for (cid, (k, seg_ranks)) in ranks_snapshot.iter() {
            k_by_cid.insert(*cid, *k);
            segment_ranks_by_cid.insert(*cid, seg_ranks.clone());
        }
        drop(ranks_snapshot);

        let result = mgr.run_maintenance(
            &store_guard,
            &k_by_cid,
            &segment_ranks_by_cid,
        );

        for (cid, reason) in &result.evicted {
            let _ = event_tx.send(DaemonEvent::ContentEvicted {
                content_id: cid.to_hex(),
                reason: reason.to_string(),
            });
        }
        for (cid, reason) in &result.retired {
            let _ = event_tx.send(DaemonEvent::ContentRetired {
                content_id: cid.to_hex(),
                reason: reason.to_string(),
            });
        }
    }
}

/// Drive the swarm event loop.
/// Type alias for shared removal cache.
type SharedRemovalCache = Arc<Mutex<crate::removal_cache::RemovalCache>>;

async fn drive_swarm(
    swarm: &mut craftec_network::CraftSwarm,
    protocol: Arc<DataCraftProtocol>,
    command_rx: &mut mpsc::UnboundedReceiver<DataCraftCommand>,
    pending_requests: PendingRequests,
    peer_scorer: SharedPeerScorer,
    removal_cache: SharedRemovalCache,
    own_capabilities: Vec<DataCraftCapability>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    event_tx: EventSender,
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
    client: Arc<Mutex<datacraft_client::DataCraftClient>>,
    max_storage_bytes: u64,
    repair_coordinator: Arc<Mutex<crate::repair::RepairCoordinator>>,
    store_for_repair: Arc<Mutex<datacraft_store::FsStore>>,
    scaling_coordinator: Arc<Mutex<crate::scaling::ScalingCoordinator>>,
    demand_tracker: Arc<Mutex<crate::scaling::DemandTracker>>,
    merkle_tree: Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>,
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
                        let _ = event_tx.send(DaemonEvent::ListeningOn { address: address.to_string() });
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        let total = swarm.connected_peers().count();
                        info!("Connected to {} ({} peers total)", peer_id, total);
                        let _ = event_tx.send(DaemonEvent::PeerConnected {
                            peer_id: peer_id.to_string(),
                            address: endpoint.get_remote_address().to_string(),
                            total_peers: total,
                        });
                        // Announce capabilities: immediately + delayed retry after gossipsub mesh forms
                        let used = client.lock().await.store().disk_usage().unwrap_or(0);
                        let sr = merkle_tree.lock().await.root();
                        let _ = command_tx.send(DataCraftCommand::PublishCapabilities {
                            capabilities: own_capabilities.clone(),
                            storage_committed_bytes: max_storage_bytes,
                            storage_used_bytes: used,
                            storage_root: sr,
                        });
                        // Gossipsub mesh formation takes ~1-3s after connection.
                        // Retry to ensure the new peer receives our announcement.
                        let delayed_caps = own_capabilities.clone();
                        let delayed_tx = command_tx.clone();
                        let delayed_client = client.clone();
                        let delayed_max = max_storage_bytes;
                        let delayed_merkle = merkle_tree.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                            let used = delayed_client.lock().await.store().disk_usage().unwrap_or(0);
                            let sr = delayed_merkle.lock().await.root();
                            let _ = delayed_tx.send(DataCraftCommand::PublishCapabilities {
                                capabilities: delayed_caps,
                                storage_committed_bytes: delayed_max,
                                storage_used_bytes: used,
                                storage_root: sr,
                            });
                        });
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        let remaining = swarm.connected_peers().count();
                        info!("Disconnected from {} ({} peers remaining)", peer_id, remaining);
                        let _ = event_tx.send(DaemonEvent::PeerDisconnected {
                            peer_id: peer_id.to_string(),
                            remaining_peers: remaining,
                        });
                    }
                    SwarmEvent::Behaviour(event) => {
                        debug!("Behaviour event: {:?}", event);
                        // Handle mDNS discovery: add discovered peers to Kademlia and dial them
                        handle_mdns_event(swarm, &event, &event_tx);
                        // Try to extract gossipsub capability announcements before passing through
                        let new_storage_peer = handle_gossipsub_capability(&event, &peer_scorer, &event_tx).await;
                        // Evict stale peers after processing announcements
                        {
                            let mut scorer = peer_scorer.lock().await;
                            scorer.evict_stale(std::time::Duration::from_secs(900)); // 15min TTL (3x announce interval)
                        }
                        // Trigger immediate distribution when a new storage peer appears
                        if new_storage_peer {
                            info!("New storage peer detected — triggering immediate content distribution");
                            let ct = content_tracker.clone();
                            let ctx = command_tx.clone();
                            let cl = client.clone();
                            let etx = event_tx.clone();
                            let ps = peer_scorer.clone();
                            tokio::spawn(async move {
                                crate::reannounce::trigger_immediate_reannounce(&ct, &ctx, &cl, &etx, &ps).await;
                            });
                        }
                        handle_gossipsub_removal(&event, &removal_cache, &event_tx).await;
                        handle_gossipsub_storage_receipt(&event, &event_tx).await;
                        handle_gossipsub_repair(&event, &repair_coordinator, &store_for_repair, &event_tx).await;
                        handle_gossipsub_scaling(&event, &scaling_coordinator, &store_for_repair, max_storage_bytes).await;
                        // Pass events to our protocol handler
                        protocol.handle_swarm_event(&SwarmEvent::Behaviour(event)).await;
                    }
                    _ => {}
                }
            }
            
            // Handle commands from IPC handler
            command = command_rx.recv() => {
                if let Some(cmd) = command {
                    match cmd {
                        DataCraftCommand::TriggerDistribution => {
                            info!("Received TriggerDistribution command — scheduling distribution in 5s");
                            let ct = content_tracker.clone();
                            let ctx = command_tx.clone();
                            let cl = client.clone();
                            let etx = event_tx.clone();
                            let ps = peer_scorer.clone();
                            tokio::spawn(async move {
                                // Short delay to let gossipsub capability announcements arrive first
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                crate::reannounce::trigger_immediate_reannounce(&ct, &ctx, &cl, &etx, &ps).await;
                            });
                        }
                        other => {
                            handle_command(swarm, &protocol, other, pending_requests.clone(), &event_tx).await;
                        }
                    }
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
    event_tx: &EventSender,
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
                
                let _ = event_tx.send(DaemonEvent::ProviderAnnounced { content_id: content_id.to_hex() });
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
        
        DataCraftCommand::PublishCapabilities { capabilities, storage_committed_bytes, storage_used_bytes, storage_root } => {
            let cap_strings: Vec<String> = capabilities.iter().map(|c| c.to_string()).collect();
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
                storage_committed_bytes,
                storage_used_bytes,
                storage_root,
                region: None, // TODO: pass region from config
            };
            match serde_json::to_vec(&announcement) {
                Ok(data) => {
                    if let Err(e) = swarm
                        .behaviour_mut()
                        .publish_to_topic(datacraft_core::CAPABILITIES_TOPIC, data)
                    {
                        debug!("Failed to publish capabilities: {:?}", e);
                    } else {
                        let _ = event_tx.send(DaemonEvent::CapabilityPublished {
                            capabilities: cap_strings,
                            storage_committed: storage_committed_bytes,
                            storage_used: storage_used_bytes,
                        });
                    }
                }
                Err(e) => {
                    error!("Failed to serialize capability announcement: {}", e);
                }
            }
        }
        
        DataCraftCommand::RequestPiece { peer_id, content_id, segment_index, piece_id, reply_tx } => {
            debug!("Handling request piece command: {}/{} from {}", content_id, segment_index, peer_id);
            
            let result = protocol
                .request_piece_from_peer(swarm.behaviour_mut(), peer_id, &content_id, segment_index, &piece_id)
                .await;
            
            let _ = reply_tx.send(result);
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

        DataCraftCommand::BroadcastStorageReceipt { receipt_data } => {
            debug!("Broadcasting StorageReceipt via gossipsub ({} bytes)", receipt_data.len());
            if let Err(e) = swarm.behaviour_mut()
                .publish_to_topic(datacraft_core::STORAGE_RECEIPT_TOPIC, receipt_data) {
                warn!("Failed to broadcast StorageReceipt via gossipsub: {:?}", e);
            }
        }

        DataCraftCommand::BroadcastRepairMessage { repair_data } => {
            debug!("Broadcasting repair message via gossipsub ({} bytes)", repair_data.len());
            if let Err(e) = swarm.behaviour_mut()
                .publish_to_topic(datacraft_core::REPAIR_TOPIC, repair_data) {
                warn!("Failed to broadcast repair message via gossipsub: {:?}", e);
            }
        }

        DataCraftCommand::BroadcastDemandSignal { signal_data } => {
            debug!("Broadcasting demand signal via gossipsub ({} bytes)", signal_data.len());
            if let Err(e) = swarm.behaviour_mut()
                .publish_to_topic(datacraft_core::SCALING_TOPIC, signal_data) {
                warn!("Failed to broadcast demand signal via gossipsub: {:?}", e);
            }
        }

        DataCraftCommand::PushPiece { peer_id, content_id, segment_index, piece_id, coefficients, piece_data, reply_tx } => {
            debug!("Handling push piece command: {}/{} to {}", content_id, segment_index, peer_id);
            
            let result = protocol
                .push_piece_to_peer(swarm.behaviour_mut(), peer_id, &content_id, segment_index, &piece_id, &coefficients, &piece_data)
                .await;
            
            let _ = reply_tx.send(result);
        }

        DataCraftCommand::PushManifest { peer_id, content_id, manifest_json, reply_tx } => {
            debug!("Handling push manifest command for {} to {}", content_id, peer_id);
            let result = protocol
                .push_manifest_to_peer(swarm.behaviour_mut(), peer_id, &content_id, &manifest_json)
                .await;
            let _ = reply_tx.send(result);
        }

        DataCraftCommand::RequestInventory { peer_id, content_id, reply_tx } => {
            debug!("Handling inventory request for {} from {}", content_id, peer_id);
            let result = protocol
                .request_inventory_from_peer(swarm.behaviour_mut(), peer_id, &content_id)
                .await;
            let _ = reply_tx.send(result);
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
        DataCraftCommand::TriggerDistribution => {
            // Handled in drive_swarm before dispatch — should not reach here
            unreachable!("TriggerDistribution should be intercepted in drive_swarm");
        }
    }
}

/// Handle mDNS discovery events: add peers to Kademlia and dial them.
fn handle_mdns_event(
    swarm: &mut craftec_network::CraftSwarm,
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    event_tx: &EventSender,
) {
    use craftec_network::behaviour::CraftBehaviourEvent;
    use libp2p::mdns;

    if let CraftBehaviourEvent::Mdns(mdns::Event::Discovered(peers)) = event {
        for (peer_id, addr) in peers {
            info!("mDNS discovered peer {} at {}", peer_id, addr);
            let _ = event_tx.send(DaemonEvent::PeerDiscovered {
                peer_id: peer_id.to_string(),
                address: addr.to_string(),
            });
            swarm.behaviour_mut().add_address(peer_id, addr.clone());
            // Dial the peer to establish a connection
            if let Err(e) = swarm.dial(addr.clone()) {
                debug!("Failed to dial mDNS peer {} at {}: {:?}", peer_id, addr, e);
            }
        }
    }
}

/// Handle gossipsub capability announcement messages.
/// Returns `true` if a new peer with `Storage` capability was discovered.
async fn handle_gossipsub_capability(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    peer_scorer: &SharedPeerScorer,
    event_tx: &EventSender,
) -> bool {
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
                        let has_storage = ann.capabilities.contains(&DataCraftCapability::Storage);
                        let cap_strings: Vec<String> = ann.capabilities.iter().map(|c| c.to_string()).collect();
                        info!(
                            "Received capability announcement from {}: {:?}",
                            peer_id, ann.capabilities
                        );
                        let _ = event_tx.send(DaemonEvent::CapabilityAnnounced {
                            peer_id: peer_id.to_string(),
                            capabilities: cap_strings,
                            storage_committed: ann.storage_committed_bytes,
                            storage_used: ann.storage_used_bytes,
                        });
                        let mut scorer = peer_scorer.lock().await;
                        // Check if this is a NEW storage peer (not previously known)
                        let is_new = scorer.get(&peer_id).is_none();
                        // Only update if newer
                        let dominated = scorer
                            .get(&peer_id)
                            .map(|_| true) // Always accept since scorer tracks announcement time
                            .unwrap_or(true);
                        if dominated {
                            scorer.update_capabilities_with_storage(
                                &peer_id,
                                ann.capabilities,
                                ann.timestamp,
                                ann.storage_committed_bytes,
                                ann.storage_used_bytes,
                            );
                        }
                        return is_new && has_storage;
                    }
                }
                Err(e) => {
                    debug!("Failed to parse capability announcement: {}", e);
                }
            }
        }
    }
    false
}

/// Handle gossipsub removal notice messages.
async fn handle_gossipsub_removal(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    removal_cache: &SharedRemovalCache,
    event_tx: &EventSender,
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
                    let valid = notice.verify();
                    let _ = event_tx.send(DaemonEvent::RemovalNoticeReceived {
                        content_id: notice.cid.to_hex(),
                        creator: notice.creator.clone(),
                        valid,
                    });
                    if valid {
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

/// Handle gossipsub StorageReceipt messages (for aggregator collection).
async fn handle_gossipsub_storage_receipt(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    event_tx: &EventSender,
) {
    use craftec_network::behaviour::CraftBehaviourEvent;
    use libp2p::gossipsub;

    if let CraftBehaviourEvent::Gossipsub(gossipsub::Event::Message {
        message, ..
    }) = event
    {
        let topic_str = message.topic.as_str();
        if topic_str == datacraft_core::STORAGE_RECEIPT_TOPIC {
            match bincode::deserialize::<datacraft_core::StorageReceipt>(&message.data) {
                Ok(receipt) => {
                    debug!(
                        "Received StorageReceipt via gossipsub: content_id={}, storage_node={}",
                        receipt.content_id,
                        hex::encode(receipt.storage_node),
                    );
                    let _ = event_tx.send(DaemonEvent::StorageReceiptReceived {
                        content_id: receipt.content_id.to_hex(),
                        storage_node: hex::encode(receipt.storage_node),
                    });
                    // TODO: forward to aggregator service channel when running as aggregator node
                }
                Err(e) => {
                    debug!("Failed to parse StorageReceipt from gossipsub: {}", e);
                }
            }
        }
    }
}

/// Handle gossipsub repair messages (signals and announcements).
async fn handle_gossipsub_repair(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    repair_coordinator: &Arc<Mutex<crate::repair::RepairCoordinator>>,
    store: &Arc<Mutex<datacraft_store::FsStore>>,
    _event_tx: &EventSender,
) {
    use libp2p::gossipsub;

    use craftec_network::behaviour::CraftBehaviourEvent;
    if let CraftBehaviourEvent::Gossipsub(gossipsub::Event::Message {
        message, ..
    }) = event
    {
        let topic_str = message.topic.as_str();
        if topic_str == datacraft_core::REPAIR_TOPIC {
            match bincode::deserialize::<datacraft_core::RepairMessage>(&message.data) {
                Ok(datacraft_core::RepairMessage::Signal(signal)) => {
                    debug!(
                        "Received repair signal for {}/seg{}: {} pieces needed",
                        signal.content_id, signal.segment_index, signal.pieces_needed
                    );
                    let store_guard = store.lock().await;
                    let mut coord = repair_coordinator.lock().await;
                    if let Some(delay) = coord.handle_repair_signal(&signal, &store_guard) {
                        // Schedule delayed repair
                        let rc = repair_coordinator.clone();
                        let st = store.clone();
                        let cid = signal.content_id;
                        let seg = signal.segment_index;
                        drop(store_guard);
                        drop(coord);
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            let store_guard = st.lock().await;
                            let manifest = match store_guard.get_manifest(&cid) {
                                Ok(m) => m,
                                Err(_) => return,
                            };
                            let mut coord = rc.lock().await;
                            coord.execute_repair(&store_guard, &manifest, cid, seg, &[]);
                        });
                    }
                }
                Ok(datacraft_core::RepairMessage::Announcement(announcement)) => {
                    debug!(
                        "Received repair announcement for {}/seg{} from repairer",
                        announcement.content_id, announcement.segment_index
                    );
                    let mut coord = repair_coordinator.lock().await;
                    coord.handle_repair_announcement(&announcement);
                }
                Err(e) => {
                    debug!("Failed to parse repair message from gossipsub: {}", e);
                }
            }
        }
    }
}

/// Handle incoming demand/scaling signals from gossipsub (push-based).
/// Providers see the notice, create a piece via RLNC recombination, and push to non-provider.
async fn handle_gossipsub_scaling(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    scaling_coordinator: &Arc<Mutex<crate::scaling::ScalingCoordinator>>,
    store: &Arc<Mutex<datacraft_store::FsStore>>,
    _max_storage_bytes: u64,
) {
    use libp2p::gossipsub;
    use craftec_network::behaviour::CraftBehaviourEvent;

    if let CraftBehaviourEvent::Gossipsub(gossipsub::Event::Message {
        message, ..
    }) = event
    {
        let topic_str = message.topic.as_str();
        if topic_str == datacraft_core::SCALING_TOPIC {
            match bincode::deserialize::<datacraft_core::DemandSignal>(&message.data) {
                Ok(signal) => {
                    debug!(
                        "Received scaling notice for {}: level={}, providers={}",
                        signal.content_id, signal.demand_level, signal.current_providers
                    );
                    let store_guard = store.lock().await;
                    let mut coord = scaling_coordinator.lock().await;
                    // Only providers (nodes holding ≥2 pieces) will get Some(delay)
                    if let Some(delay) = coord.handle_scaling_notice(&signal, &store_guard) {
                        let cid = signal.content_id;
                        drop(store_guard);
                        let coord_clone = scaling_coordinator.clone();
                        let store_clone = store.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            let store_guard = store_clone.lock().await;
                            let coord = coord_clone.lock().await;
                            coord.execute_scaling(&store_guard, cid, &[]);
                        });
                    }
                }
                Err(e) => {
                    debug!("Failed to parse demand signal from gossipsub: {}", e);
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
    interval_secs: u64,
    client: Arc<Mutex<datacraft_client::DataCraftClient>>,
    max_storage_bytes: u64,
    region: Option<String>,
    merkle_tree: Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>,
) {
    use std::time::Duration;

    // Initial delay before first announcement
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        let used = client.lock().await.store().disk_usage().unwrap_or(0);
        let storage_root = merkle_tree.lock().await.root();
        debug!("Publishing capability announcement (storage: {}/{} bytes, merkle root: {})", used, max_storage_bytes, hex::encode(&storage_root[..8]));
        let _ = command_tx.send(DataCraftCommand::PublishCapabilities {
            capabilities: capabilities.clone(),
            storage_committed_bytes: max_storage_bytes,
            storage_used_bytes: used,
            storage_root,
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
    daemon_event_tx: EventSender,
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
) {
    info!("Starting protocol events handler");
    
    while let Some(event) = event_rx.recv().await {
        match event {
            DataCraftEvent::ProvidersResolved { content_id, providers } => {
                info!("Found {} providers for {}", providers.len(), content_id);
                let _ = daemon_event_tx.send(DaemonEvent::ProvidersResolved {
                    content_id: content_id.to_hex(),
                    count: providers.len(),
                });
                
                // Find and respond to the waiting request
                let mut pending = pending_requests.lock().await;
                if let Some(PendingRequest::ResolveProviders { reply_tx }) = pending.remove(&content_id) {
                    let _ = reply_tx.send(Ok(providers));
                    debug!("Responded to provider resolution request for {}", content_id);
                }
            }
            
            DataCraftEvent::ManifestRetrieved { content_id, manifest } => {
                info!("Retrieved manifest for {} ({} segments)", content_id, manifest.segment_count);
                let _ = daemon_event_tx.send(DaemonEvent::ManifestRetrieved {
                    content_id: content_id.to_hex(),
                    segments: manifest.segment_count,
                });
                
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

            DataCraftEvent::PiecePushReceived { content_id } => {
                let mut t = content_tracker.lock().await;
                t.increment_local_pieces(&content_id);
            }
            DataCraftEvent::ManifestPushReceived { content_id, manifest } => {
                info!("Received manifest push for {} — tracking as storage provider", content_id);
                let mut t = content_tracker.lock().await;
                t.track_stored(content_id, &manifest);
                drop(t);

                // Announce as provider in DHT so other nodes can discover us
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let cmd = DataCraftCommand::AnnounceProvider {
                    content_id,
                    manifest: manifest.clone(),
                    reply_tx,
                };
                if command_tx.send(cmd).is_ok() {
                    match reply_rx.await {
                        Ok(Ok(())) => info!("Announced as provider for {} after receiving push", content_id),
                        Ok(Err(e)) => warn!("Failed to announce as provider for {}: {}", content_id, e),
                        Err(_) => {}
                    }
                }
            }
            DataCraftEvent::DhtError { content_id, error } => {
                warn!("DHT error for {}: {}", content_id, error);
                let _ = daemon_event_tx.send(DaemonEvent::DhtError {
                    content_id: content_id.to_hex(),
                    error: error.clone(),
                    next_action: "Will re-announce in next maintenance cycle".to_string(),
                });
                
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
    event_tx: EventSender,
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
            let _ = event_tx.send(DaemonEvent::ChallengerRoundCompleted { rounds: rounds as u32 });
        }
    }
}
