//! DataCraft daemon service
//!
//! Manages the libp2p swarm, IPC server, and content operations.

use std::collections::HashMap;
use std::sync::Arc;

use craftec_ipc::IpcServer;
use craftec_network::NetworkConfig;
use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, ContentManifest, CapabilityAnnouncement, DataCraftCapability, WireStatus};
use datacraft_transfer::{DataCraftRequest, DataCraftResponse};
use libp2p::identity::Keypair;
use tokio::sync::{mpsc, Mutex, oneshot};
use tracing::{debug, error, info, warn};

use crate::stream_manager::{StreamManager, InboundMessage, OutboundMessage, transfer_stream_protocol};

use crate::behaviour::{build_datacraft_swarm, DataCraftBehaviourEvent, DataCraftSwarm};
use crate::commands::DataCraftCommand;
use crate::events::{self, DaemonEvent, EventSender};
use crate::handler::DataCraftHandler;
use crate::peer_reconnect::PeerReconnector;
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
                    warn!("[service.rs] Failed to write default config to {:?}: {}", path, e);
                }
            }
            cfg
        }
        None => {
            let cfg = crate::config::DaemonConfig::load(&data_dir);
            let config_file = data_dir.join("config.json");
            if !config_file.exists() {
                if let Err(e) = cfg.save(&data_dir) {
                    warn!("[service.rs] Failed to write default config to {:?}: {}", config_file, e);
                }
            }
            cfg
        }
    };
    info!("[service.rs] Daemon config: capability_announce={}s, reannounce_interval={}s, reannounce_threshold={}s",
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
                warn!("[service.rs] Failed to build storage Merkle tree: {}, starting empty", e);
                datacraft_store::merkle::StorageMerkleTree::new()
            });
        info!("[service.rs] Storage Merkle tree built: {} leaves, root={}", tree.len(), hex::encode(&tree.root()[..8]));
        Arc::new(Mutex::new(tree))
    };

    // Build swarm with DataCraft wrapper behaviour (CraftBehaviour + libp2p_stream)
    let (mut swarm, local_peer_id) = build_datacraft_swarm(keypair.clone(), network_config).await
        .map_err(|e| format!("Failed to build swarm: {}", e))?;
    info!("[service.rs] DataCraft node started: {}", local_peer_id);

    // Set Kademlia to server mode so DHT queries work (especially on localhost / LAN)
    swarm.behaviour_mut().craft.kademlia.set_mode(Some(libp2p::kad::Mode::Server));

    // Dial bootstrap peers for reliable discovery beyond mDNS
    for addr_str in &daemon_config.boot_peers {
        match addr_str.parse::<libp2p::Multiaddr>() {
            Ok(addr) => {
                // Extract peer ID if present in multiaddr
                let peer_id = addr.iter().find_map(|proto| {
                    if let libp2p::multiaddr::Protocol::P2p(pid) = proto {
                        Some(pid)
                    } else {
                        None
                    }
                });
                if let Some(pid) = peer_id {
                    let dial_addr: libp2p::Multiaddr = addr.iter()
                        .filter(|p| !matches!(p, libp2p::multiaddr::Protocol::P2p(_)))
                        .collect();
                    swarm.behaviour_mut().craft.add_address(&pid, dial_addr);
                    if let Err(e) = swarm.dial(pid) {
                        warn!("[service.rs] Failed to dial boot peer {}: {:?}", addr_str, e);
                    } else {
                        info!("[service.rs] Dialing boot peer: {}", addr_str);
                    }
                } else {
                    // No peer ID in multiaddr — dial the address directly.
                    // Won't add to Kademlia but will establish a connection for gossipsub.
                    info!("[service.rs] Dialing boot peer (no peer_id): {}", addr_str);
                    if let Err(e) = swarm.dial(addr) {
                        warn!("[service.rs] Failed to dial boot peer {}: {:?}", addr_str, e);
                    }
                }
            }
            Err(e) => warn!("[service.rs] Invalid boot peer address '{}': {}", addr_str, e),
        }
    }

    // Create event channel for protocol communication
    let (protocol_event_tx, mut protocol_event_rx) = mpsc::unbounded_channel::<DataCraftEvent>();
    
    // Create command channel for IPC → swarm communication
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<DataCraftCommand>();
    let command_tx_for_caps = command_tx.clone();
    let command_tx_for_maintenance = command_tx.clone();
    let command_tx_for_events = command_tx.clone();
    
    // Create pending requests tracker for DHT operations
    let pending_requests: PendingRequests = Arc::new(Mutex::new(HashMap::new()));

    // Create DataCraft protocol handler (DHT operations only)
    let protocol = DataCraftProtocol::new(protocol_event_tx);

    // Subscribe to gossipsub topics
    if let Err(e) = swarm
        .behaviour_mut().craft
        .subscribe_topic(datacraft_core::NODE_STATUS_TOPIC)
    {
        error!("Failed to subscribe to node status: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut().craft
        .subscribe_topic(datacraft_core::CAPABILITIES_TOPIC)
    {
        error!("Failed to subscribe to capabilities topic: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut().craft
        .subscribe_topic(datacraft_core::REMOVAL_TOPIC)
    {
        error!("Failed to subscribe to removal topic: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut().craft
        .subscribe_topic(datacraft_core::STORAGE_RECEIPT_TOPIC)
    {
        error!("Failed to subscribe to storage receipt topic: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut().craft
        .subscribe_topic(datacraft_core::REPAIR_TOPIC)
    {
        error!("Failed to subscribe to repair topic: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut().craft
        .subscribe_topic(datacraft_core::DEGRADATION_TOPIC)
    {
        error!("Failed to subscribe to degradation topic: {:?}", e);
    }
    if let Err(e) = swarm
        .behaviour_mut().craft
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

    // Create demand tracker
    let demand_tracker: Arc<Mutex<crate::scaling::DemandTracker>> = Arc::new(Mutex::new(crate::scaling::DemandTracker::new()));

    // Eviction manager
    let eviction_config = crate::eviction::EvictionConfig {
        max_storage_bytes: daemon_config.max_storage_bytes,
        enable_eviction: true,
    };
    let eviction_manager = Arc::new(Mutex::new(crate::eviction::EvictionManager::new(&eviction_config)));

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

    // Verify store integrity on startup
    {
        if let Ok(tmp_store) = datacraft_store::FsStore::new(&data_dir) {
            match tmp_store.verify_integrity() {
                Ok(removed) if removed > 0 => {
                    warn!("[service.rs] Startup integrity check removed {} corrupted pieces", removed);
                }
                Err(e) => warn!("[service.rs] Integrity check failed: {}", e),
                _ => {}
            }
        }
    }

    // Check disk space on startup
    if let Err(e) = crate::disk_monitor::check_startup(&data_dir) {
        error!("Disk space critically low: {}", e);
        return Err(e.into());
    }

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
                info!("[service.rs] Imported {} existing content items into tracker on startup", imported);
            }
            // Sync tracker state with disk: validate local piece counts, reset stale remote state
            tracker.sync_with_store(&tmp_store);
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
                _ => warn!("[service.rs] Unknown capability '{}' in config, skipping", cap),
            }
        }
        if result.is_empty() {
            vec![DataCraftCapability::Client]
        } else {
            result
        }
    };
    info!("[service.rs] Capabilities: {:?}", own_capabilities);

    let daemon_config_shared = Arc::new(Mutex::new(daemon_config.clone()));
    let mut handler = DataCraftHandler::new(client.clone(), protocol.clone(), command_tx.clone(), peer_scorer.clone(), receipt_store.clone(), channel_store);
    handler.set_settlement_client(settlement_client);
    handler.set_content_tracker(content_tracker.clone());
    handler.set_own_capabilities(own_capabilities.clone());
    handler.set_daemon_config(daemon_config_shared, data_dir.clone());
    handler.set_eviction_manager(eviction_manager.clone());
    handler.set_event_sender(event_tx.clone());
    handler.set_merkle_tree(merkle_tree.clone());
    if let Some(key) = node_signing_key {
        handler.set_node_signing_key(key);
    }
    info!("[service.rs] Starting IPC server on {}", socket_path);

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
    challenger_mgr.set_merkle_tree(merkle_tree.clone());
    let demand_signal_tracker: Arc<Mutex<crate::scaling::DemandSignalTracker>> = Arc::new(Mutex::new(crate::scaling::DemandSignalTracker::new()));
    challenger_mgr.set_demand_signal_tracker(demand_signal_tracker.clone());

    let challenger_mgr = Arc::new(Mutex::new(challenger_mgr));

    // Wire challenger into handler for CID registration on publish/store
    handler.set_challenger(challenger_mgr.clone());
    handler.set_local_peer_id(local_peer_id);
    let handler = Arc::new(handler);

    // Load or generate API key for WebSocket authentication
    let api_key = crate::api_key::load_or_generate(&data_dir)
        .map_err(|e| format!("Failed to load/generate API key: {}", e))?;
    info!("[service.rs] API key loaded for WebSocket authentication");

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

    // Degradation coordinator for over-replication reduction
    let degradation_command_tx = command_tx.clone();
    let degradation_coord = crate::degradation::DegradationCoordinator::new(local_peer_id, degradation_command_tx);
    let degradation_coordinator: Arc<Mutex<crate::degradation::DegradationCoordinator>> = Arc::new(Mutex::new(degradation_coord));

    // Scaling: coordinator for demand-driven piece acquisition
    let scaling_command_tx = command_tx.clone();
    let mut scaling_coord = crate::scaling::ScalingCoordinator::new(local_peer_id, scaling_command_tx);
    scaling_coord.set_merkle_tree(merkle_tree.clone());
    let scaling_coordinator: Arc<Mutex<crate::scaling::ScalingCoordinator>> = Arc::new(Mutex::new(scaling_coord));

    // Derive ed25519 signing key for capability announcement signing
    let swarm_signing_key = keypair.clone().try_into_ed25519().ok().map(|ed25519_kp| {
        let secret_bytes = ed25519_kp.secret();
        ed25519_dalek::SigningKey::from_bytes(
            secret_bytes.as_ref().try_into().expect("ed25519 secret is 32 bytes"),
        )
    });

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
            info!("[service.rs] WebSocket server ended");
        }
        _ = drive_swarm(&mut swarm, protocol.clone(), &mut command_rx, pending_requests.clone(), peer_scorer.clone(), removal_cache.clone(), own_capabilities.clone(), command_tx_for_caps.clone(), event_tx.clone(), content_tracker.clone(), client.clone(), daemon_config.max_storage_bytes, repair_coordinator.clone(), store.clone(), scaling_coordinator.clone(), demand_tracker.clone(), merkle_tree.clone(), daemon_config.region.clone(), swarm_signing_key, receipt_store.clone(), daemon_config.max_peer_connections, degradation_coordinator.clone(), demand_signal_tracker.clone()) => {
            info!("[service.rs] Swarm event loop ended");
        }
        _ = handle_protocol_events(&mut protocol_event_rx, pending_requests.clone(), event_tx.clone(), content_tracker.clone(), command_tx_for_events, challenger_mgr.clone()) => {
            info!("[service.rs] Protocol events handler ended");
        }
        _ = announce_capabilities_periodically(&local_peer_id, own_capabilities, command_tx_for_caps, daemon_config.capability_announce_interval_secs, client.clone(), daemon_config.max_storage_bytes, daemon_config.region.clone(), merkle_tree.clone(), content_tracker.clone()) => {
            info!("[service.rs] Capability announcement loop ended");
        }
        _ = run_challenger_loop(challenger_mgr, store.clone(), event_tx.clone()) => {
            info!("[service.rs] Challenger loop ended");
        }
        _ = crate::reannounce::content_maintenance_loop(
            content_tracker.clone(),
            command_tx_for_maintenance.clone(),
            client.clone(),
            daemon_config.reannounce_interval_secs,
            event_tx.clone(),
            peer_scorer.clone(),
        ) => {
            info!("[service.rs] Content maintenance loop ended");
        }
        _ = scaling_maintenance_loop(demand_tracker, command_tx_for_maintenance, local_peer_id, peer_scorer.clone(), content_tracker.clone()) => {
            info!("[service.rs] Scaling maintenance loop ended");
        }
        _ = eviction_maintenance_loop(eviction_manager, store.clone(), event_tx.clone(), pdp_ranks.clone(), merkle_tree.clone()) => {
            info!("[service.rs] Eviction maintenance loop ended");
        }
        _ = crate::aggregator::run_aggregation_loop(receipt_store.clone(), event_tx.clone(), aggregator_config) => {
            info!("[service.rs] Aggregation loop ended");
        }
        _ = gc_loop(store.clone(), content_tracker.clone(), client.clone(), merkle_tree.clone(), event_tx.clone(), daemon_config.gc_interval_secs, daemon_config.max_storage_bytes) => {
            info!("[service.rs] GC loop ended");
        }
        _ = content_health_loop(content_tracker.clone(), store.clone(), event_tx.clone(), daemon_config.health_check_interval_secs) => {
            info!("[service.rs] Content health loop ended");
        }
        _ = disk_monitor_loop(data_dir.clone(), event_tx.clone(), daemon_config.health_check_interval_secs) => {
            info!("[service.rs] Disk monitor loop ended");
        }
        _ = data_retention_loop(receipt_store.clone()) => {
            info!("[service.rs] Data retention loop ended");
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
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
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
            // Get known providers for this CID to exclude from push targets
            let known_providers = content_tracker.lock().await.get_providers(&cid);
            let has_targets = crate::push_target::has_non_provider_targets(
                &local_peer_id,
                &known_providers,
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
                    info!("[service.rs] Broadcasting demand signal for {}: level={}", cid, demand_level);
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
    merkle_tree: Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>,
) {
    use std::time::Duration;
    const DISK_SPACE_THRESHOLD: u64 = 100 * 1024 * 1024; // 100 MB

    // Initial delay
    tokio::time::sleep(Duration::from_secs(60)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(300));
    loop {
        interval.tick().await;

        // Check disk space and warn if low
        {
            let s = store.lock().await;
            let path = s.base_dir();
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt;
                if let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) {
                    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
                    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
                    if ret == 0 {
                        let available = stat.f_bavail as u64 * stat.f_frsize as u64;
                        if available < DISK_SPACE_THRESHOLD {
                            warn!("[service.rs] Low disk space: {} bytes available (threshold: {} bytes)", available, DISK_SPACE_THRESHOLD);
                            let _ = event_tx.send(DaemonEvent::StoragePressure {
                                available_bytes: available,
                                threshold_bytes: DISK_SPACE_THRESHOLD,
                            });
                        }
                    }
                }
            }
        }

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

        // Rebuild merkle tree after eviction/retirement
        if !result.evicted.is_empty() || !result.retired.is_empty() {
            if let Ok(new_tree) = datacraft_store::merkle::StorageMerkleTree::build_from_store(&store_guard) {
                *merkle_tree.lock().await = new_tree;
                debug!("Rebuilt storage Merkle tree after eviction");
            }
        }
    }
}

/// Periodic garbage collection: delete unpinned content, enforce storage limits.
async fn gc_loop(
    store: Arc<Mutex<datacraft_store::FsStore>>,
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
    client: Arc<Mutex<datacraft_client::DataCraftClient>>,
    merkle_tree: Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>,
    event_tx: EventSender,
    gc_interval_secs: u64,
    _max_storage_bytes: u64,
) {
    use std::time::Duration;

    if gc_interval_secs == 0 {
        debug!("GC disabled (gc_interval_secs=0)");
        std::future::pending::<()>().await;
        return;
    }

    // Initial delay to let the daemon stabilize
    tokio::time::sleep(Duration::from_secs(120)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(gc_interval_secs));
    loop {
        interval.tick().await;
        debug!("Starting GC sweep");

        let s = store.lock().await;
        let all_cids = match s.list_content() {
            Ok(cids) => cids,
            Err(e) => {
                warn!("[service.rs] GC: failed to list content: {}", e);
                continue;
            }
        };

        // Get pinned CIDs from the client's PinManager
        let pinned_cids: std::collections::HashSet<_> = {
            let c = client.lock().await;
            all_cids.iter().filter(|cid| c.is_pinned(cid)).cloned().collect()
        };

        // Protect Publisher and StorageProvider content from GC.
        // Storage providers must keep their pieces — they're serving the network.
        // Only fetched/temporary content (not tracked or unknown role) gets GC'd.
        let tracker = content_tracker.lock().await;
        let protected_cids: std::collections::HashSet<_> = all_cids
            .iter()
            .filter(|cid| {
                tracker
                    .get(cid)
                    .map(|state| {
                        state.role == crate::content_tracker::ContentRole::Publisher
                            || state.role == crate::content_tracker::ContentRole::StorageProvider
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        drop(tracker);

        let mut deleted_count = 0u64;

        for cid in &all_cids {
            if pinned_cids.contains(cid) || protected_cids.contains(cid) {
                continue; // Keep pinned, published, and storage provider content
            }

            // Unpinned, untracked content (fetched temporarily) -> delete
            if let Ok(()) = s.delete_content(cid) {
                deleted_count += 1;
                content_tracker.lock().await.remove(cid);
                debug!("GC: deleted unpinned content {}", cid);
            }
        }

        // Rebuild merkle tree if we deleted anything
        if deleted_count > 0 {
            info!("[service.rs] GC: deleted {} unpinned content items", deleted_count);
            if let Ok(new_tree) = datacraft_store::merkle::StorageMerkleTree::build_from_store(&s) {
                *merkle_tree.lock().await = new_tree;
                debug!("GC: rebuilt storage Merkle tree");
            }
            let _ = event_tx.send(DaemonEvent::GcCompleted {
                deleted_count,
                deleted_bytes: 0,
            });
        }
    }
}

/// Type alias for shared removal cache.
type SharedRemovalCache = Arc<Mutex<crate::removal_cache::RemovalCache>>;

async fn drive_swarm(
    swarm: &mut DataCraftSwarm,
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
    region: Option<String>,
    signing_key: Option<ed25519_dalek::SigningKey>,
    receipt_store: Arc<Mutex<crate::receipt_store::PersistentReceiptStore>>,
    max_peer_connections: usize,
    degradation_coordinator: Arc<Mutex<crate::degradation::DegradationCoordinator>>,
    demand_signal_tracker: Arc<Mutex<crate::scaling::DemandSignalTracker>>,
) {
    use libp2p::swarm::SwarmEvent;
    use libp2p::futures::StreamExt;

    // Stream manager for persistent piece transfer
    let mut stream_control = swarm.behaviour().stream.new_control();
    let (mut stream_manager, mut inbound_rx, outbound_tx) = StreamManager::new(stream_control);

    // Peer reconnector for exponential backoff reconnection
    let mut peer_reconnector = PeerReconnector::new();
    // Channel for spawned inbound handlers to return responses without blocking swarm loop
    // Responses now written directly on the inbound stream by spawned handlers (bidirectional streams).

    let mut reconnect_interval = tokio::time::interval(std::time::Duration::from_secs(5));
    reconnect_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Peer heartbeat timer — checks connected peers every 60s
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Track last activity time per peer for heartbeat timeout detection
    let mut peer_last_seen: HashMap<libp2p::PeerId, std::time::Instant> = HashMap::new();

    loop {
        tokio::select! {
            // Handle swarm events
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        info!("[service.rs] Listening on {}", address);
                        let _ = event_tx.send(DaemonEvent::ListeningOn { address: address.to_string() });
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, num_established, .. } => {
                        let total = swarm.connected_peers().count();
                        info!("[service.rs] ConnectionEstablished to {} (num_established={}, {} peers total, endpoint={:?})", peer_id, num_established, total, endpoint);
                        // Max peer connection limit check
                        if total > max_peer_connections && num_established.get() == 1 {
                            warn!("[service.rs] Max peer connections reached ({}/{}), rejecting {}", total, max_peer_connections, peer_id);
                            // Close the connection by not opening streams and letting it timeout
                        }
                        // Clear reconnector state on successful connection
                        peer_reconnector.on_reconnected(&peer_id);
                        stream_manager.clear_open_cooldown(&peer_id);
                        stream_manager.ensure_opening(peer_id);
                        // Track for heartbeat
                        peer_last_seen.insert(peer_id, std::time::Instant::now());
                        let _ = event_tx.send(DaemonEvent::PeerConnected {
                            peer_id: peer_id.to_string(),
                            address: endpoint.get_remote_address().to_string(),
                            total_peers: total,
                        });
                        // Announce capabilities: immediately + delayed retry after gossipsub mesh forms
                        let used = client.lock().await.store().disk_usage().unwrap_or(0);
                        let sr = merkle_tree.lock().await.root();
                        let pc = {
                            let t = content_tracker.lock().await;
                            let c = client.lock().await;
                            let mut counts = std::collections::HashMap::new();
                            for state in t.list() {
                                if state.local_pieces > 0 {
                                    let mut seg_counts = Vec::new();
                                    for seg in 0..state.segment_count as u32 {
                                        seg_counts.push(c.store().list_pieces(&state.content_id, seg).unwrap_or_default().len());
                                    }
                                    counts.insert(state.content_id.to_hex(), seg_counts);
                                }
                            }
                            counts
                        };
                        let _ = command_tx.send(DataCraftCommand::PublishCapabilities {
                            capabilities: own_capabilities.clone(),
                            storage_committed_bytes: max_storage_bytes,
                            storage_used_bytes: used,
                            storage_root: sr,
                            piece_counts: pc,
                        });
                        // Gossipsub mesh formation takes ~1-3s after connection.
                        // Retry to ensure the new peer receives our announcement.
                        let delayed_caps = own_capabilities.clone();
                        let delayed_tx = command_tx.clone();
                        let delayed_client = client.clone();
                        let delayed_max = max_storage_bytes;
                        let delayed_merkle = merkle_tree.clone();
                        let delayed_ct = content_tracker.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                            let used = delayed_client.lock().await.store().disk_usage().unwrap_or(0);
                            let sr = delayed_merkle.lock().await.root();
                            let pc = {
                                let t = delayed_ct.lock().await;
                                let c = delayed_client.lock().await;
                                let mut counts = std::collections::HashMap::new();
                                for state in t.list() {
                                    if state.local_pieces > 0 {
                                        let mut seg_counts = Vec::new();
                                        for seg in 0..state.segment_count as u32 {
                                            seg_counts.push(c.store().list_pieces(&state.content_id, seg).unwrap_or_default().len());
                                        }
                                        counts.insert(state.content_id.to_hex(), seg_counts);
                                    }
                                }
                                counts
                            };
                            let _ = delayed_tx.send(DataCraftCommand::PublishCapabilities {
                                capabilities: delayed_caps,
                                storage_committed_bytes: delayed_max,
                                storage_used_bytes: used,
                                storage_root: sr,
                                piece_counts: pc,
                            });
                        });
                    }
                    SwarmEvent::ConnectionClosed { peer_id, num_established, .. } => {
                        let remaining = swarm.connected_peers().count();
                        info!("[service.rs] ConnectionClosed for {} (num_established={}, {} peers remaining)", peer_id, num_established, remaining);
                        if num_established > 0 {
                            // A connection closed but we still have others. Our outbound stream
                            // may have been on the closed connection. Force re-open to land on
                            // a surviving connection.
                            info!("[service.rs] ConnectionClosed but {} remaining for {} — forcing outbound stream re-open", num_established, peer_id);
                            stream_manager.force_reopen_outbound(&peer_id);
                            continue;
                        }
                        info!("[service.rs] Fully disconnected from {} — cleaning up", peer_id);
                        peer_scorer.lock().await.remove_peer(&peer_id);
                        peer_last_seen.remove(&peer_id);
                        stream_manager.on_peer_disconnected(&peer_id);
                        // Track for reconnection with last-known addresses from Kademlia routing table
                        let addrs: Vec<libp2p::Multiaddr> = {
                            let mut result = Vec::new();
                            for bucket in swarm.behaviour_mut().craft.kademlia.kbuckets() {
                                for entry in bucket.iter() {
                                    if entry.node.key.preimage() == &peer_id {
                                        result.extend(entry.node.value.iter().cloned());
                                    }
                                }
                            }
                            result
                        };
                        peer_reconnector.track_disconnected(peer_id, addrs);
                        let _ = event_tx.send(DaemonEvent::PeerDisconnected {
                            peer_id: peer_id.to_string(),
                            remaining_peers: remaining,
                        });
                    }
                    SwarmEvent::Behaviour(event) => {
                        match event {
                            DataCraftBehaviourEvent::Craft(ref craft_event) => {
                                debug!("Craft behaviour event: {:?}", craft_event);
                                // Update last-seen for gossipsub message sources
                                if let craftec_network::behaviour::CraftBehaviourEvent::Gossipsub(
                                    libp2p::gossipsub::Event::Message { propagation_source, .. }
                                ) = craft_event {
                                    peer_last_seen.insert(*propagation_source, std::time::Instant::now());
                                }
                                // Handle going-offline messages from peers
                                handle_gossipsub_going_offline(craft_event, &peer_scorer, &event_tx).await;
                                // Handle mDNS discovery
                                handle_mdns_event(swarm, craft_event, &event_tx);
                                // Try to extract gossipsub capability announcements
                                let new_storage_peer = handle_gossipsub_capability(craft_event, &peer_scorer, &event_tx).await;
                                {
                                    let mut scorer = peer_scorer.lock().await;
                                    scorer.evict_stale(std::time::Duration::from_secs(900));
                                }
                                if new_storage_peer {
                                    debug!("New storage peer detected — will receive pieces via equalization");
                                }
                                handle_gossipsub_removal(craft_event, &removal_cache, &event_tx).await;
                                handle_gossipsub_storage_receipt(craft_event, &event_tx, &receipt_store).await;
                                handle_gossipsub_repair(craft_event, &repair_coordinator, &store_for_repair, &event_tx, &content_tracker).await;
                                handle_gossipsub_degradation(craft_event, &degradation_coordinator, &store_for_repair, &event_tx, &merkle_tree).await;
                                handle_gossipsub_scaling(craft_event, &scaling_coordinator, &store_for_repair, max_storage_bytes, &content_tracker, &demand_signal_tracker).await;
                                // Handle Kademlia events for DHT queries
                                if let craftec_network::behaviour::CraftBehaviourEvent::Kademlia(ref kad_event) = craft_event {
                                    protocol.handle_kademlia_event(kad_event).await;
                                }
                            }
                            DataCraftBehaviourEvent::Stream(()) => {
                                // libp2p_stream produces no swarm events
                            }
                        }
                    }
                    _ => {}
                }
            }
            
            // Handle commands from IPC handler
            command = command_rx.recv() => {
                if let Some(cmd) = command {
                    match cmd {
                        DataCraftCommand::TriggerDistribution => {
                            info!("[service.rs] Received TriggerDistribution command — running initial push");
                            let ct = content_tracker.clone();
                            let ctx = command_tx.clone();
                            let cl = client.clone();
                            let etx = event_tx.clone();
                            let ps = peer_scorer.clone();
                            tokio::spawn(async move {
                                // Short delay to let gossipsub capability announcements arrive first
                                info!("[service.rs] TriggerDistribution: sleeping 5s for capability announcements");
                                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                let needs_push = {
                                    let t = ct.lock().await;
                                    t.needs_distribution()
                                };
                                info!("[service.rs] TriggerDistribution: {} CIDs need distribution", needs_push.len());
                                for cid in needs_push {
                                    info!("[service.rs] TriggerDistribution: running initial push for {}", cid);
                                    crate::reannounce::run_initial_push(&cid, &ct, &ctx, &cl, &ps, &etx).await;
                                    info!("[service.rs] TriggerDistribution: initial push done for {}", cid);
                                }
                            });
                        }
                        other => {
                            handle_command(swarm, &protocol, other, pending_requests.clone(), &outbound_tx, &event_tx, &region, &signing_key).await;
                        }
                    }
                }
            }
            // Incoming streams handled by StreamManager's inbound_acceptor task

            // Process inbound messages from peer streams
            Some(msg) = inbound_rx.recv() => {
                info!("[service.rs] Processing inbound from {} seq={}: {:?}", msg.peer, msg.seq_id, std::mem::discriminant(&msg.request));
                // Spawn handler as task to avoid blocking the swarm event loop
                // while waiting for store lock (maintenance cycle may hold it).
                let store_clone = store_for_repair.clone();
                let ct_clone = content_tracker.clone();
                let proto_clone = protocol.clone();
                let dt_clone = demand_tracker.clone();
                let peer = msg.peer;
                let seq_id = msg.seq_id;
                let mut stream = msg.stream;
                tokio::spawn(async move {
                    let response = handle_incoming_transfer_request(
                        &peer, msg.request, &store_clone, &ct_clone, &proto_clone, &dt_clone,
                    ).await;
                    info!("[service.rs] Spawned handler done for {} seq={}: {:?}", peer, seq_id, std::mem::discriminant(&response));
                    // Write response back on the SAME stream the request came from
                    match datacraft_transfer::wire::write_response_frame(&mut stream, seq_id, &response).await {
                        Ok(()) => info!("[service.rs] Wrote response to {} seq={}", peer, seq_id),
                        Err(e) => warn!("[service.rs] Failed to write response to {} seq={}: {}", peer, seq_id, e),
                    }
                });
            }

            // Peer reconnection + stream manager maintenance
            _ = reconnect_interval.tick() => {
                stream_manager.poll_open_streams();
                stream_manager.cleanup_dead_streams();
                let due = peer_reconnector.peers_due_for_reconnect();
                for (peer_id, addrs) in due {
                    for addr in &addrs {
                        debug!("Attempting reconnection to {} at {}", peer_id, addr);
                        if let Err(e) = swarm.dial(addr.clone()) {
                            debug!("Reconnect dial to {} failed: {:?}", peer_id, e);
                        }
                    }
                }
            }
            // Peer heartbeat — check for unresponsive peers
            _ = heartbeat_interval.tick() => {
                let now = std::time::Instant::now();
                let timeout = std::time::Duration::from_secs(30);
                let connected: Vec<libp2p::PeerId> = swarm.connected_peers().cloned().collect();
                for peer_id in connected {
                    // Mark first-seen for peers we haven't tracked yet
                    peer_last_seen.entry(peer_id).or_insert(now);
                    if let Some(last) = peer_last_seen.get(&peer_id) {
                        if now.duration_since(*last) > timeout {
                            warn!("[service.rs] Peer {} heartbeat timeout (no activity for >30s)", peer_id);
                            peer_scorer.lock().await.record_timeout(&peer_id);
                            let _ = event_tx.send(DaemonEvent::PeerHeartbeatTimeout {
                                peer_id: peer_id.to_string(),
                            });
                        }
                    }
                }
            }
        }
    }
}

/// Handle an incoming transfer request from a peer via persistent stream.
async fn handle_incoming_transfer_request(
    peer: &libp2p::PeerId,
    request: DataCraftRequest,
    store: &Arc<Mutex<datacraft_store::FsStore>>,
    _content_tracker: &Arc<Mutex<crate::content_tracker::ContentTracker>>,
    protocol: &Arc<DataCraftProtocol>,
    demand_tracker: &Arc<Mutex<crate::scaling::DemandTracker>>,
) -> DataCraftResponse {
    match request {
        DataCraftRequest::PieceSync { content_id, segment_index, have_pieces, max_pieces, .. } => {
            info!("[service.rs] Handling PieceSync from {} for {}/seg{} (they have {} pieces, want max {})", peer, content_id, segment_index, have_pieces.len(), max_pieces);
            info!("[service.rs] PieceSync handler: acquiring store lock...");
            let store_guard = store.lock().await;
            info!("[service.rs] PieceSync handler: store lock acquired");
            let piece_ids = store_guard.list_pieces(&content_id, segment_index).unwrap_or_default();
            info!("[service.rs] We have {} pieces for {}/seg{}", piece_ids.len(), content_id, segment_index);

            let have_set: std::collections::HashSet<[u8; 32]> = have_pieces.into_iter().collect();
            let mut pieces = Vec::new();

            for pid in piece_ids {
                if have_set.contains(&pid) {
                    continue;
                }
                if pieces.len() >= max_pieces as usize {
                    break;
                }
                match store_guard.get_piece(&content_id, segment_index, &pid) {
                    Ok((data, coefficients)) => {
                        pieces.push(datacraft_transfer::PiecePayload {
                            segment_index,
                            piece_id: pid,
                            coefficients,
                            data,
                        });
                    }
                    Err(e) => {
                        debug!("Failed to read piece {}: {}", hex::encode(&pid[..4]), e);
                    }
                }
            }

            info!("[service.rs] Responding with {} pieces for {}/seg{}", pieces.len(), content_id, segment_index);
            // Record demand for scaling decisions
            if !pieces.is_empty() {
                let mut dt = demand_tracker.lock().await;
                dt.record_fetch(content_id);
            }
            DataCraftResponse::PieceBatch { pieces }
        }
        DataCraftRequest::PiecePush { content_id, segment_index, piece_id, coefficients, data } => {
            info!("[service.rs] Handling PiecePush from {} for {}/seg{}", peer, content_id, segment_index);
            info!("[service.rs] PiecePush handler: acquiring store lock...");
            let store_guard = store.lock().await;
            info!("[service.rs] PiecePush handler: store lock acquired");

            // Check if we have the manifest (must receive ManifestPush first)
            if store_guard.get_manifest(&content_id).is_err() {
                warn!("[service.rs] Received piece push for {} but no manifest — rejecting", content_id);
                return DataCraftResponse::Ack { status: WireStatus::Error };
            }

            match store_guard.store_piece(&content_id, segment_index, &piece_id, &data, &coefficients) {
                Ok(()) => {
                    info!("[service.rs] Stored pushed piece for {}/seg{}", content_id, segment_index);
                    // Notify protocol of piece push for tracker update
                    if let Err(e) = protocol.event_tx.send(DataCraftEvent::PiecePushReceived { content_id }) {
                        debug!("Failed to send piece push event: {}", e);
                    }
                    DataCraftResponse::Ack { status: WireStatus::Ok }
                }
                Err(e) => {
                    warn!("[service.rs] Failed to store pushed piece: {}", e);
                    DataCraftResponse::Ack { status: WireStatus::Error }
                }
            }
        }
        DataCraftRequest::ManifestPush { content_id, manifest_json } => {
            info!("[service.rs] Handling ManifestPush from {} for {}", peer, content_id);
            match serde_json::from_slice::<datacraft_core::ContentManifest>(&manifest_json) {
                Ok(manifest) => {
                    info!("[service.rs] ManifestPush handler: acquiring store lock...");
                    let store_guard = store.lock().await;
                    info!("[service.rs] ManifestPush handler: store lock acquired");
                    match store_guard.store_manifest(&manifest) {
                        Ok(()) => {
                            info!("[service.rs] Stored manifest for {}", content_id);
                            if let Err(e) = protocol.event_tx.send(DataCraftEvent::ManifestPushReceived {
                                content_id,
                                manifest,
                            }) {
                                debug!("Failed to send manifest push event: {}", e);
                            }
                            DataCraftResponse::Ack { status: WireStatus::Ok }
                        }
                        Err(e) => {
                            warn!("[service.rs] Failed to store manifest: {}", e);
                            DataCraftResponse::Ack { status: WireStatus::Error }
                        }
                    }
                }
                Err(e) => {
                    warn!("[service.rs] Failed to parse manifest JSON: {}", e);
                    DataCraftResponse::Ack { status: WireStatus::Error }
                }
            }
        }
    }
}

/// Handle a command from the IPC handler.
async fn handle_command(
    swarm: &mut DataCraftSwarm,
    protocol: &Arc<DataCraftProtocol>,
    command: DataCraftCommand,
    pending_requests: PendingRequests,
    outbound_tx: &mpsc::Sender<OutboundMessage>,
    event_tx: &EventSender,
    region: &Option<String>,
    signing_key: &Option<ed25519_dalek::SigningKey>,
) {
    match command {
        DataCraftCommand::AnnounceProvider { content_id, manifest, reply_tx } => {
            debug!("Handling announce provider command for {}", content_id);
            
            // Get the local peer ID
            let local_peer_id = *swarm.local_peer_id();
            
            let result = async {
                // Don't announce publisher as provider here — publishers delete all
                // pieces after distribution. Storage nodes announce themselves in the
                // PushManifest handler when they receive the manifest.
                // We only publish the manifest to DHT so fetchers can find it.
                protocol.publish_manifest(&mut swarm.behaviour_mut().craft, &manifest, &local_peer_id).await
                    .map_err(|e| format!("Failed to publish manifest: {}", e))?;
                
                let _ = event_tx.send(DaemonEvent::ProviderAnnounced { content_id: content_id.to_hex() });
                debug!("Successfully started DHT operations for {}", content_id);
                Ok(())
            }.await;
            
            let _ = reply_tx.send(result);
        }
        
        DataCraftCommand::ResolveProviders { content_id, reply_tx } => {
            debug!("Handling resolve providers command for {}", content_id);
            
            match protocol.resolve_providers(&mut swarm.behaviour_mut().craft, &content_id).await {
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
            
            match protocol.get_manifest(&mut swarm.behaviour_mut().craft, &content_id).await {
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
        
        DataCraftCommand::PublishCapabilities { capabilities, storage_committed_bytes, storage_used_bytes, storage_root, piece_counts } => {
            let cap_strings: Vec<String> = capabilities.iter().map(|c| c.to_string()).collect();
            let local_peer_id = *swarm.local_peer_id();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let mut announcement = CapabilityAnnouncement {
                peer_id: local_peer_id.to_bytes(),
                capabilities,
                timestamp,
                signature: vec![],
                storage_committed_bytes,
                storage_used_bytes,
                storage_root,
                region: region.clone(),
                piece_counts,
            };
            // Sign the announcement if we have a signing key
            if let Some(key) = signing_key {
                use ed25519_dalek::Signer;
                let data = announcement.signable_data();
                let sig: ed25519_dalek::Signature = key.sign(&data);
                announcement.signature = sig.to_bytes().to_vec();
            }
            match serde_json::to_vec(&announcement) {
                Ok(data) => {
                    if let Err(e) = swarm
                        .behaviour_mut().craft
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
        
        DataCraftCommand::PieceSync { peer_id, content_id, segment_index, merkle_root, have_pieces, max_pieces, reply_tx } => {
            info!("[service.rs] Handling PieceSync command: {}/{} to peer {} (sending {} have_pieces, max_pieces={})", content_id, segment_index, peer_id, have_pieces.len(), max_pieces);
            let request = DataCraftRequest::PieceSync {
                content_id,
                segment_index,
                merkle_root,
                have_pieces,
                max_pieces,
            };
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = OutboundMessage { peer: peer_id, request, reply_tx: Some(ack_tx) };
            let tx = outbound_tx.clone();
            tokio::spawn(async move {
                info!("[service.rs] Sending PieceSync outbound message to {}", peer_id);
                if tx.send(msg).await.is_err() {
                    warn!("[service.rs] PieceSync to {}: outbound channel closed", peer_id);
                    let _ = reply_tx.send(Err("outbound channel closed".into()));
                    return;
                }
                info!("[service.rs] PieceSync sent to outbound queue for {}, waiting for ack (5s timeout)", peer_id);
                match tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx).await {
                    Ok(Ok(response)) => { 
                        let desc = match &response {
                            datacraft_transfer::DataCraftResponse::PieceBatch { pieces } => format!("{} pieces", pieces.len()),
                            other => format!("{:?}", other),
                        };
                        info!("[service.rs] PieceSync to {}: got response: {}", peer_id, desc);
                        let _ = reply_tx.send(Ok(response)); 
                    }
                    Ok(Err(_)) => { 
                        warn!("[service.rs] PieceSync to {}: ack channel closed (dropped)", peer_id);
                        let _ = reply_tx.send(Err("ack channel closed".into())); 
                    }
                    Err(_) => { 
                        warn!("[service.rs] PieceSync to {}: timed out after 5s", peer_id);
                        let _ = reply_tx.send(Err("piece sync timed out".into())); 
                    }
                }
            });
        }

        DataCraftCommand::PutReKey { content_id, entry, reply_tx } => {
            debug!("Handling put re-key command for {} → {}", content_id, hex::encode(entry.recipient_did));
            let local_peer_id = *swarm.local_peer_id();
            let result = datacraft_routing::ContentRouter::put_re_key(
                &mut swarm.behaviour_mut().craft, &content_id, &entry, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        DataCraftCommand::RemoveReKey { content_id, recipient_did, reply_tx } => {
            debug!("Handling remove re-key command for {} → {}", content_id, hex::encode(recipient_did));
            let local_peer_id = *swarm.local_peer_id();
            let result = datacraft_routing::ContentRouter::remove_re_key(
                &mut swarm.behaviour_mut().craft, &content_id, &recipient_did, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        DataCraftCommand::PutAccessList { access_list, reply_tx } => {
            debug!("Handling put access list command for {}", access_list.content_id);
            let local_peer_id = *swarm.local_peer_id();
            let result = datacraft_routing::ContentRouter::put_access_list(
                &mut swarm.behaviour_mut().craft, &access_list, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        DataCraftCommand::GetAccessList { content_id, reply_tx } => {
            debug!("Handling get access list command for {}", content_id);
            match protocol.get_access_list_dht(&mut swarm.behaviour_mut().craft, &content_id).await {
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
                &mut swarm.behaviour_mut().craft, &content_id, &notice, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());

            // Broadcast via gossipsub
            if let Ok(data) = bincode::serialize(&notice) {
                if let Err(e) = swarm.behaviour_mut().craft
                    .publish_to_topic(datacraft_core::REMOVAL_TOPIC, data) {
                    warn!("[service.rs] Failed to broadcast removal notice via gossipsub: {:?}", e);
                }
            }

            let _ = reply_tx.send(dht_result);
        }

        DataCraftCommand::BroadcastStorageReceipt { receipt_data } => {
            debug!("Broadcasting StorageReceipt via gossipsub ({} bytes)", receipt_data.len());
            if let Err(e) = swarm.behaviour_mut().craft
                .publish_to_topic(datacraft_core::STORAGE_RECEIPT_TOPIC, receipt_data) {
                warn!("[service.rs] Failed to broadcast StorageReceipt via gossipsub: {:?}", e);
            }
        }

        DataCraftCommand::BroadcastRepairMessage { repair_data } => {
            debug!("Broadcasting repair message via gossipsub ({} bytes)", repair_data.len());
            if let Err(e) = swarm.behaviour_mut().craft
                .publish_to_topic(datacraft_core::REPAIR_TOPIC, repair_data) {
                warn!("[service.rs] Failed to broadcast repair message via gossipsub: {:?}", e);
            }
        }

        DataCraftCommand::BroadcastDegradationMessage { degradation_data } => {
            debug!("Broadcasting degradation message via gossipsub ({} bytes)", degradation_data.len());
            if let Err(e) = swarm.behaviour_mut().craft
                .publish_to_topic(datacraft_core::DEGRADATION_TOPIC, degradation_data) {
                warn!("[service.rs] Failed to broadcast degradation message via gossipsub: {:?}", e);
            }
        }

        DataCraftCommand::BroadcastDemandSignal { signal_data } => {
            debug!("Broadcasting demand signal via gossipsub ({} bytes)", signal_data.len());
            if let Err(e) = swarm.behaviour_mut().craft
                .publish_to_topic(datacraft_core::SCALING_TOPIC, signal_data) {
                warn!("[service.rs] Failed to broadcast demand signal via gossipsub: {:?}", e);
            }
        }

        DataCraftCommand::PushPiece { peer_id, content_id, segment_index, piece_id, coefficients, piece_data, reply_tx } => {
            debug!("Handling push piece command: {}/{} to {}", content_id, segment_index, peer_id);
            let request = DataCraftRequest::PiecePush {
                content_id,
                segment_index,
                piece_id,
                coefficients,
                data: piece_data,
            };
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = OutboundMessage { peer: peer_id, request, reply_tx: Some(ack_tx) };
            let tx = outbound_tx.clone();
            tokio::spawn(async move {
                info!("[service.rs] PushPiece: sending to outbound queue for {}", peer_id);
                if tx.send(msg).await.is_err() {
                    warn!("[service.rs] PushPiece to {}: outbound channel closed", peer_id);
                    let _ = reply_tx.send(Err("outbound channel closed".into()));
                    return;
                }
                info!("[service.rs] PushPiece: waiting for ack from {} (5s timeout)", peer_id);
                match tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx).await {
                    Ok(Ok(DataCraftResponse::Ack { status })) => {
                        info!("[service.rs] PushPiece to {}: got ack status={:?}", peer_id, status);
                        if status == datacraft_core::WireStatus::Ok {
                            let _ = reply_tx.send(Ok(()));
                        } else {
                            let _ = reply_tx.send(Err(format!("PushPiece ack: {:?}", status)));
                        }
                    }
                    Ok(Ok(other)) => { 
                        warn!("[service.rs] PushPiece to {}: unexpected response {:?}", peer_id, std::mem::discriminant(&other));
                        let _ = reply_tx.send(Err("unexpected response type".into())); 
                    }
                    Ok(Err(_)) => { 
                        warn!("[service.rs] PushPiece to {}: ack channel closed", peer_id);
                        let _ = reply_tx.send(Err("ack channel closed".into())); 
                    }
                    Err(_) => {
                        warn!("[service.rs] PushPiece to {}: timed out after 5s", peer_id);
                        let _ = reply_tx.send(Err("piece push timed out".into()));
                    }
                }
            });
        }

        DataCraftCommand::PushManifest { peer_id, content_id, manifest_json, reply_tx } => {
            debug!("Handling push manifest command for {} to {}", content_id, peer_id);
            let request = DataCraftRequest::ManifestPush {
                content_id,
                manifest_json,
            };
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = OutboundMessage { peer: peer_id, request, reply_tx: Some(ack_tx) };
            let tx = outbound_tx.clone();
            tokio::spawn(async move {
                info!("[service.rs] PushManifest: sending to outbound queue for {}", peer_id);
                if tx.send(msg).await.is_err() {
                    warn!("[service.rs] PushManifest to {}: outbound channel closed", peer_id);
                    let _ = reply_tx.send(Err("outbound channel closed".into()));
                    return;
                }
                info!("[service.rs] PushManifest: waiting for ack from {} (5s timeout)", peer_id);
                match tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx).await {
                    Ok(Ok(DataCraftResponse::Ack { status })) => {
                        info!("[service.rs] PushManifest to {}: got ack status={:?}", peer_id, status);
                        if status == datacraft_core::WireStatus::Ok {
                            let _ = reply_tx.send(Ok(()));
                        } else {
                            let _ = reply_tx.send(Err(format!("ManifestPush ack: {:?}", status)));
                        }
                    }
                    Ok(Ok(other)) => { 
                        warn!("[service.rs] PushManifest to {}: unexpected response {:?}", peer_id, std::mem::discriminant(&other));
                        let _ = reply_tx.send(Err("unexpected response type".into())); 
                    }
                    Ok(Err(_)) => { 
                        warn!("[service.rs] PushManifest to {}: ack channel closed", peer_id);
                        let _ = reply_tx.send(Err("ack channel closed".into())); 
                    }
                    Err(_) => {
                        warn!("[service.rs] PushManifest to {}: timed out after 5s", peer_id);
                        let _ = reply_tx.send(Err("manifest push timed out".into()));
                    }
                }
            });
        }

        // RequestInventory removed — challenger now uses PieceSync

        DataCraftCommand::CheckRemoval { content_id, reply_tx } => {
            debug!("Handling check removal command for {}", content_id);
            // For now, just start a DHT query. Full async response would need pending request tracking.
            // This is a simplified version — the RemovalCache handles most checks locally.
            let _ = datacraft_routing::ContentRouter::get_removal_notice(
                &mut swarm.behaviour_mut().craft, &content_id,
            );
            // Reply immediately with None — the cache should be checked first by the caller.
            let _ = reply_tx.send(Ok(None));
        }
        DataCraftCommand::BroadcastGoingOffline { data } => {
            debug!("Broadcasting going-offline message ({} bytes)", data.len());
            if let Err(e) = swarm.behaviour_mut().craft
                .publish_to_topic(datacraft_core::NODE_STATUS_TOPIC, data) {
                warn!("[service.rs] Failed to broadcast going-offline message: {:?}", e);
            }
        }

        DataCraftCommand::TriggerDistribution => {
            // Handled in drive_swarm before dispatch — should not reach here
            unreachable!("TriggerDistribution should be intercepted in drive_swarm");
        }
    }
}

/// Handle mDNS discovery events: add peers to Kademlia and dial them.
fn handle_mdns_event(
    swarm: &mut DataCraftSwarm,
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    event_tx: &EventSender,
) {
    use craftec_network::behaviour::CraftBehaviourEvent;
    use libp2p::mdns;

    if let CraftBehaviourEvent::Mdns(mdns::Event::Discovered(peers)) = event {
        for (peer_id, addr) in peers {
            info!("[service.rs] mDNS discovered peer {} at {}", peer_id, addr);
            let _ = event_tx.send(DaemonEvent::PeerDiscovered {
                peer_id: peer_id.to_string(),
                address: addr.to_string(),
            });
            swarm.behaviour_mut().craft.add_address(peer_id, addr.clone());
            // Dial by peer ID — address is already registered via add_address.
            // Dialing the raw multiaddr (with /p2p/ suffix) causes relay transport
            // to attempt noise handshake first and fail, adding seconds of delay.
            if let Err(e) = swarm.dial(*peer_id) {
                debug!("Failed to dial mDNS peer {}: {:?}", peer_id, e);
            }
        }
        if let Err(e) = swarm.behaviour_mut().craft.bootstrap() {
            debug!("Kademlia bootstrap after mDNS: {:?}", e);
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
                            scorer.update_capabilities_full(
                                &peer_id,
                                ann.capabilities,
                                ann.timestamp,
                                ann.storage_committed_bytes,
                                ann.storage_used_bytes,
                                ann.region,
                                ann.piece_counts,
                                ann.storage_root,
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
    receipt_store: &Arc<Mutex<crate::receipt_store::PersistentReceiptStore>>,
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
                    // Forward to receipt store for aggregator collection
                    let mut store = receipt_store.lock().await;
                    if let Err(e) = store.add_storage(receipt) {
                        debug!("Failed to store gossipsub receipt: {}", e);
                    }
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
    content_tracker: &Arc<Mutex<crate::content_tracker::ContentTracker>>,
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
                    info!("[service.rs] gossipsub_repair: acquiring store lock...");
                    let store_guard = store.lock().await;
                    info!("[service.rs] gossipsub_repair: store lock acquired");
                    let mut coord = repair_coordinator.lock().await;
                    if let Some(delay) = coord.handle_repair_signal(&signal, &store_guard) {
                        // Schedule delayed repair
                        let rc = repair_coordinator.clone();
                        let st = store.clone();
                        let ct = content_tracker.clone();
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
                            let providers = ct.lock().await.get_providers(&cid);
                            let mut coord = rc.lock().await;
                            coord.execute_repair(&store_guard, &manifest, cid, seg, &providers);
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

/// Handle incoming degradation signals and announcements from gossipsub.
async fn handle_gossipsub_degradation(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    degradation_coordinator: &Arc<Mutex<crate::degradation::DegradationCoordinator>>,
    store: &Arc<Mutex<datacraft_store::FsStore>>,
    _event_tx: &EventSender,
    merkle_tree: &Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>,
) {
    use libp2p::gossipsub;
    use craftec_network::behaviour::CraftBehaviourEvent;

    if let CraftBehaviourEvent::Gossipsub(gossipsub::Event::Message {
        message, ..
    }) = event
    {
        let topic_str = message.topic.as_str();
        if topic_str == datacraft_core::DEGRADATION_TOPIC {
            match bincode::deserialize::<datacraft_core::DegradationMessage>(&message.data) {
                Ok(datacraft_core::DegradationMessage::Signal(signal)) => {
                    debug!(
                        "Received degradation signal for {}/seg{}: {} excess pieces",
                        signal.content_id, signal.segment_index, signal.excess_pieces
                    );
                    let store_guard = store.lock().await;
                    let mut coord = degradation_coordinator.lock().await;
                    if let Some(delay) = coord.handle_degradation_signal(&signal, &store_guard) {
                        let dc = degradation_coordinator.clone();
                        let st = store.clone();
                        let mt = merkle_tree.clone();
                        let cid = signal.content_id;
                        let seg = signal.segment_index;
                        drop(store_guard);
                        drop(coord);
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            let store_guard = st.lock().await;
                            let mut tree = mt.lock().await;
                            let mut coord = dc.lock().await;
                            coord.execute_degradation(&store_guard, cid, seg, Some(&mut tree));
                        });
                    }
                }
                Ok(datacraft_core::DegradationMessage::Announcement(announcement)) => {
                    debug!(
                        "Received degradation announcement for {}/seg{} from dropper",
                        announcement.content_id, announcement.segment_index
                    );
                    let mut coord = degradation_coordinator.lock().await;
                    coord.handle_degradation_announcement(&announcement);
                }
                Err(e) => {
                    debug!("Failed to parse degradation message from gossipsub: {}", e);
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
    content_tracker: &Arc<Mutex<crate::content_tracker::ContentTracker>>,
    demand_signal_tracker: &Arc<Mutex<crate::scaling::DemandSignalTracker>>,
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
                    // Record demand signal for degradation awareness
                    demand_signal_tracker.lock().await.record_signal(signal.content_id);
                    let store_guard = store.lock().await;
                    let mut coord = scaling_coordinator.lock().await;
                    // Only providers (nodes holding ≥2 pieces) will get Some(delay)
                    if let Some(delay) = coord.handle_scaling_notice(&signal, &store_guard) {
                        let cid = signal.content_id;
                        drop(store_guard);
                        let coord_clone = scaling_coordinator.clone();
                        let store_clone = store.clone();
                        let ct = content_tracker.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            let store_guard = store_clone.lock().await;
                            let providers = ct.lock().await.get_providers(&cid);
                            let coord = coord_clone.lock().await;
                            coord.execute_scaling(&store_guard, cid, &providers);
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

/// Handle gossipsub "going offline" messages from peers.
async fn handle_gossipsub_going_offline(
    event: &craftec_network::behaviour::CraftBehaviourEvent,
    peer_scorer: &SharedPeerScorer,
    event_tx: &EventSender,
) {
    use craftec_network::behaviour::CraftBehaviourEvent;
    use libp2p::gossipsub;

    if let CraftBehaviourEvent::Gossipsub(gossipsub::Event::Message {
        propagation_source,
        message,
        ..
    }) = event
    {
        let topic_str = message.topic.as_str();
        if topic_str == datacraft_core::NODE_STATUS_TOPIC {
            // Try to parse as a going-offline message
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&message.data) {
                if val.get("type").and_then(|t| t.as_str()) == Some("going_offline") {
                    info!("[service.rs] Peer {} announced going offline", propagation_source);
                    peer_scorer.lock().await.remove_peer(propagation_source);
                    let _ = event_tx.send(DaemonEvent::PeerGoingOffline {
                        peer_id: propagation_source.to_string(),
                    });
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
    _region: Option<String>,
    merkle_tree: Arc<Mutex<datacraft_store::merkle::StorageMerkleTree>>,
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
) {
    use std::time::Duration;

    // Initial delay before first announcement
    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        let used = client.lock().await.store().disk_usage().unwrap_or(0);
        let storage_root = merkle_tree.lock().await.root();
        // Build piece counts from content tracker
        let piece_counts: std::collections::HashMap<String, Vec<usize>> = {
            let t = content_tracker.lock().await;
            let c = client.lock().await;
            let mut counts = std::collections::HashMap::new();
            for state in t.list() {
                if state.local_pieces > 0 {
                    let mut seg_counts = Vec::new();
                    for seg in 0..state.segment_count as u32 {
                        let n = c.store().list_pieces(&state.content_id, seg).unwrap_or_default().len();
                        seg_counts.push(n);
                    }
                    counts.insert(state.content_id.to_hex(), seg_counts);
                }
            }
            counts
        };
        debug!("Publishing capability announcement (storage: {}/{} bytes, merkle root: {}, cids: {})", used, max_storage_bytes, hex::encode(&storage_root[..8]), piece_counts.len());
        let _ = command_tx.send(DataCraftCommand::PublishCapabilities {
            capabilities: capabilities.clone(),
            storage_committed_bytes: max_storage_bytes,
            storage_used_bytes: used,
            storage_root,
            piece_counts,
        });
    }
}

// Incoming streams handled by StreamManager in drive_swarm.

/// Handle protocol events (DHT query results, etc.).
async fn handle_protocol_events(
    event_rx: &mut mpsc::UnboundedReceiver<DataCraftEvent>,
    pending_requests: PendingRequests,
    daemon_event_tx: EventSender,
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
    command_tx: mpsc::UnboundedSender<DataCraftCommand>,
    challenger: Arc<Mutex<crate::challenger::ChallengerManager>>,
) {
    info!("[service.rs] Starting protocol events handler");
    
    while let Some(event) = event_rx.recv().await {
        match event {
            DataCraftEvent::ProvidersResolved { content_id, providers } => {
                info!("[service.rs] Found {} providers for {}", providers.len(), content_id);
                let _ = daemon_event_tx.send(DaemonEvent::ProvidersResolved {
                    content_id: content_id.to_hex(),
                    count: providers.len(),
                });

                // Track providers in content tracker for push target selection
                {
                    let mut tracker = content_tracker.lock().await;
                    for &peer in &providers {
                        tracker.add_provider(&content_id, peer);
                    }
                }
                
                // Find and respond to the waiting request
                let mut pending = pending_requests.lock().await;
                if let Some(PendingRequest::ResolveProviders { reply_tx }) = pending.remove(&content_id) {
                    let _ = reply_tx.send(Ok(providers));
                    debug!("Responded to provider resolution request for {}", content_id);
                }
            }
            
            DataCraftEvent::ManifestRetrieved { content_id, manifest } => {
                info!("[service.rs] Retrieved manifest for {} ({} segments)", content_id, manifest.segment_count);
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
                info!("[service.rs] Retrieved access list for {} ({} entries)", content_id, access_list.entries.len());
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
                info!("[service.rs] Received manifest push for {} — tracking as storage provider", content_id);
                let mut t = content_tracker.lock().await;
                t.track_stored(content_id, &manifest);
                drop(t);

                // Register CID with challenger for PDP tracking
                {
                    let mut mgr = challenger.lock().await;
                    // Default baseline: 1.5x redundancy target for all content
                    mgr.register_cid(content_id, Some(crate::health::TierInfo { min_piece_ratio: 1.5 }));
                }

                // Announce as provider in DHT so other nodes can discover us
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let cmd = DataCraftCommand::AnnounceProvider {
                    content_id,
                    manifest: manifest.clone(),
                    reply_tx,
                };
                if command_tx.send(cmd).is_ok() {
                    match reply_rx.await {
                        Ok(Ok(())) => info!("[service.rs] Announced as provider for {} after receiving push", content_id),
                        Ok(Err(e)) => warn!("[service.rs] Failed to announce as provider for {}: {}", content_id, e),
                        Err(_) => {}
                    }
                }

                // Query DHT for other providers — needed for repair/scaling push target selection
                let tracker_clone = content_tracker.clone();
                let cmd_tx = command_tx.clone();
                tokio::spawn(async move {
                    // Small delay to let other nodes announce first
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                    let cmd = DataCraftCommand::ResolveProviders { content_id, reply_tx };
                    if cmd_tx.send(cmd).is_ok() {
                        if let Ok(Ok(providers)) = reply_rx.await {
                            let mut t = tracker_clone.lock().await;
                            for peer in &providers {
                                t.add_provider(&content_id, *peer);
                            }
                            info!("[service.rs] Storage node discovered {} providers for {}", providers.len(), content_id);
                        }
                    }
                });
            }
            DataCraftEvent::DhtError { content_id, error } => {
                warn!("[service.rs] DHT error for {}: {}", content_id, error);
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

/// Periodic content health monitoring.
///
/// Checks local piece counts against required k for each tracked CID.
/// Emits ContentDegraded/ContentCritical events when health drops.
async fn content_health_loop(
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
    store: Arc<Mutex<datacraft_store::FsStore>>,
    event_tx: EventSender,
    interval_secs: u64,
) {
    use std::time::Duration;

    // Initial delay
    tokio::time::sleep(Duration::from_secs(60)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        let tracker = content_tracker.lock().await;
        let all = tracker.list();
        drop(tracker);

        let store_guard = store.lock().await;
        for state in &all {
            let cid = &state.content_id;
            let total_needed = state.segment_count * state.k;
            if total_needed == 0 {
                continue;
            }

            // Count actual local pieces
            let mut actual_pieces = 0usize;
            for seg in 0..state.segment_count as u32 {
                actual_pieces += store_guard.list_pieces(cid, seg).unwrap_or_default().len();
            }

            let health = actual_pieces as f64 / total_needed as f64;
            if health < 0.5 {
                warn!("[service.rs] Content {} critically degraded: health={:.2}", cid, health);
                let _ = event_tx.send(DaemonEvent::ContentCritical {
                    content_id: cid.to_hex(),
                    health,
                    providers: state.provider_count,
                });
                // Mark degraded in tracker
                content_tracker.lock().await.mark_degraded(cid);
            } else if health < 0.8 {
                info!("[service.rs] Content {} degraded: health={:.2}", cid, health);
                let _ = event_tx.send(DaemonEvent::ContentDegraded {
                    content_id: cid.to_hex(),
                    health,
                    providers: state.provider_count,
                });
            }
        }
    }
}

/// Periodic disk space monitoring.
async fn disk_monitor_loop(
    data_dir: std::path::PathBuf,
    event_tx: EventSender,
    interval_secs: u64,
) {
    use std::time::Duration;

    tokio::time::sleep(Duration::from_secs(30)).await;

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        if let Some(ds) = crate::disk_monitor::get_disk_space(&data_dir) {
            if ds.percent_used >= crate::disk_monitor::WARN_THRESHOLD {
                warn!(
                    "Disk space warning: {:.1}% used ({} / {} bytes)",
                    ds.percent_used * 100.0, ds.used_bytes, ds.total_bytes
                );
                let _ = event_tx.send(DaemonEvent::DiskSpaceWarning {
                    used_bytes: ds.used_bytes,
                    total_bytes: ds.total_bytes,
                    percent: ds.percent_used * 100.0,
                });
            }
        }
    }
}

/// Prune receipts older than 24 hours (normal node data retention).
/// Aggregator nodes handle lifetime data separately.
async fn data_retention_loop(
    receipt_store: Arc<Mutex<crate::receipt_store::PersistentReceiptStore>>,
) {
    use std::time::Duration;

    // Run every hour
    let mut interval = tokio::time::interval(Duration::from_secs(3600));
    loop {
        interval.tick().await;
        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(24 * 3600);
        let mut store = receipt_store.lock().await;
        match store.prune_before(cutoff) {
            Ok(pruned) if pruned > 0 => {
                info!("[service.rs] Data retention: pruned {} receipts older than 24h", pruned);
            }
            Ok(_) => {}
            Err(e) => {
                warn!("[service.rs] Data retention prune failed: {}", e);
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
        // Create a temporary FsStore for the challenger to avoid holding the
        // shared store_for_repair Mutex during network round-trips (PDP challenges
        // can take tens of seconds, blocking all PieceSync handlers).
        let tmp_store = {
            let guard = store.lock().await;
            let base_dir = guard.base_dir().to_path_buf();
            drop(guard);
            match datacraft_store::FsStore::new(&base_dir) {
                Ok(s) => s,
                Err(e) => {
                    warn!("[service.rs] Challenger: failed to create temp store: {}", e);
                    continue;
                }
            }
        };
        let mut mgr = challenger.lock().await;
        let rounds = mgr.periodic_check(&tmp_store).await;
        if rounds > 0 {
            info!("[service.rs] Challenger completed {} rounds", rounds);
            let _ = event_tx.send(DaemonEvent::ChallengerRoundCompleted { rounds: rounds as u32 });
        }
    }
}
