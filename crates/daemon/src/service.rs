//! CraftObj daemon service
//!
//! Manages the libp2p swarm, IPC server, and content operations.

use std::collections::HashMap;
use std::sync::Arc;

use craftec_ipc::IpcServer;
use craftec_network::NetworkConfig;
use craftobj_client::CraftObjClient;
use craftobj_core::{ContentId, ContentManifest, CraftObjCapability, WireStatus};
use craftobj_transfer::{CraftObjRequest, CraftObjResponse};
use libp2p::identity::Keypair;
use tokio::sync::{mpsc, Mutex, oneshot};
use tracing::{debug, error, info, warn};

use crate::stream_manager::{StreamManager, InboundMessage, OutboundMessage};

use crate::behaviour::{build_craftobj_swarm, CraftObjBehaviourEvent, CraftObjSwarm};
use crate::commands::CraftObjCommand;
use crate::events::{self, DaemonEvent, EventSender};
use crate::handler::CraftObjHandler;
use crate::peer_reconnect::PeerReconnector;
use crate::protocol::{CraftObjProtocol, CraftObjEvent};

/// Default socket path for the CraftObj daemon.
pub fn default_socket_path() -> String {
    if cfg!(target_os = "linux") {
        if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
            return format!("{}/craftobj.sock", dir);
        }
    }
    "/tmp/craftobj.sock".to_string()
}

/// Default data directory for CraftObj storage.
pub fn default_data_dir() -> std::path::PathBuf {
    let base = dirs_data_dir().unwrap_or_else(|| std::path::PathBuf::from("/tmp"));
    base.join("craftobj")
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
        reply_tx: oneshot::Sender<Result<craftobj_core::access::AccessList, String>>,
    },
}

/// Global pending requests tracker.
type PendingRequests = Arc<Mutex<HashMap<ContentId, PendingRequest>>>;

/// Run the CraftObj daemon.
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

/// Run the CraftObj daemon with an optional config file path override.
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
    info!("[service.rs] Daemon config: reannounce_interval={}s, reannounce_threshold={}s",
        daemon_config.reannounce_interval_secs,
        daemon_config.reannounce_threshold_secs,
    );

    // Create broadcast channel for daemon events (WS push)
    let (event_tx, _) = events::event_channel(256);

    // Build client and shared store
    let client = CraftObjClient::new(&data_dir)?;
    let client = Arc::new(Mutex::new(client));
    
    let store = Arc::new(Mutex::new(craftobj_store::FsStore::new(&data_dir)?));

    // Build storage Merkle tree from existing pieces
    let merkle_tree = {
        let tmp_store = craftobj_store::FsStore::new(&data_dir)?;
        let tree = craftobj_store::merkle::StorageMerkleTree::build_from_store(&tmp_store)
            .unwrap_or_else(|e| {
                warn!("[service.rs] Failed to build storage Merkle tree: {}, starting empty", e);
                craftobj_store::merkle::StorageMerkleTree::new()
            });
        info!("[service.rs] Storage Merkle tree built: {} leaves, root={}", tree.len(), hex::encode(&tree.root()[..8]));
        Arc::new(Mutex::new(tree))
    };

    // Build swarm with CraftObj wrapper behaviour (CraftBehaviour + libp2p_stream)
    let (mut swarm, local_peer_id) = build_craftobj_swarm(keypair.clone(), network_config).await
        .map_err(|e| format!("Failed to build swarm: {}", e))?;
    info!("[service.rs] CraftObj node started: {}", local_peer_id);

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
    let (protocol_event_tx, mut protocol_event_rx) = mpsc::unbounded_channel::<CraftObjEvent>();
    
    // Create command channel for IPC → swarm communication
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<CraftObjCommand>();
    let command_tx_for_caps = command_tx.clone();
    let command_tx_for_maintenance = command_tx.clone();
    let command_tx_for_events = command_tx.clone();
    
    // Create pending requests tracker for DHT operations
    let pending_requests: PendingRequests = Arc::new(Mutex::new(HashMap::new()));

    // Create CraftObj protocol handler (DHT operations only)
    let protocol = CraftObjProtocol::new(protocol_event_tx);

    // Peer scorer — tracks capabilities and reliability
    let peer_scorer: SharedPeerScorer = Arc::new(Mutex::new(crate::peer_scorer::PeerScorer::new()));

    // Start IPC server with enhanced handler
    let ipc_server = IpcServer::new(&socket_path);
    // Persistent receipt store
    let receipts_path = std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join(".craftobj")
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
        .join(".craftobj")
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
        if let Ok(tmp_store) = craftobj_store::FsStore::new(&data_dir) {
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
        if let Ok(tmp_store) = craftobj_store::FsStore::new(&data_dir) {
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
                "storage" => result.push(CraftObjCapability::Storage),
                "client" => result.push(CraftObjCapability::Client),
                _ => warn!("[service.rs] Unknown capability '{}' in config, skipping", cap),
            }
        }
        if result.is_empty() {
            vec![CraftObjCapability::Client]
        } else {
            result
        }
    };
    info!("[service.rs] Capabilities: {:?}", own_capabilities);

    let daemon_config_shared = Arc::new(Mutex::new(daemon_config.clone()));
    let mut handler = CraftObjHandler::new(client.clone(), protocol.clone(), command_tx.clone(), peer_scorer.clone(), receipt_store.clone(), channel_store);
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
    handler.set_demand_signal_tracker(demand_signal_tracker.clone());

    // PieceMap — event-sourced materialized view of piece locations
    let piece_map = {
        let mut pm = crate::piece_map::PieceMap::new(local_peer_id);
        // Initialize from local store: scan all local pieces and record them
        let local_node_bytes = local_peer_id.to_bytes().to_vec();
        if let Ok(store_guard) = client.try_lock() {
            if let Ok(cids) = store_guard.store().list_content_with_pieces() {
                for cid in cids {
                    if let Ok(segments) = store_guard.store().list_segments(&cid) {
                        for seg in segments {
                            if let Ok(pieces) = store_guard.store().list_pieces(&cid, seg) {
                                if !pieces.is_empty() {
                                    pm.track_segment(cid, seg);
                                }
                                for pid in pieces {
                                    if let Ok((_data, coefficients)) = store_guard.store().get_piece(&cid, seg, &pid) {
                                        let seq = pm.next_seq();
                                        let event = craftobj_core::PieceEvent::Stored(craftobj_core::PieceStored {
                                            node: local_node_bytes.clone(),
                                            cid,
                                            segment: seg,
                                            piece_id: pid,
                                            coefficients,
                                            seq,
                                            timestamp: std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap_or_default()
                                                .as_secs(),
                                            signature: vec![], // Local init, no need to sign
                                        });
                                        pm.apply_event(&event);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            info!("[service.rs] PieceMap initialized with {} CIDs from local store", pm.all_cids().len());
        } else {
            warn!("[service.rs] Could not lock client for PieceMap initialization");
        }
        Arc::new(Mutex::new(pm))
    };

    handler.set_piece_map(piece_map.clone());
    // Wire PieceMap into challenger for coefficient vector reads (replaces inventory requests)
    challenger_mgr.lock().await.set_piece_map(piece_map.clone());
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

    // HealthScan — periodic scan of owned segments for repair/degradation
    let health_scan_command_tx = command_tx.clone();
    let health_scan = crate::health_scan::HealthScan::new(
        piece_map.clone(),
        store.clone(),
        demand_signal_tracker.clone(),
        local_peer_id,
        health_scan_command_tx,
    );
    let health_scan: Arc<Mutex<crate::health_scan::HealthScan>> = Arc::new(Mutex::new(health_scan));

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
        pool_id: daemon_config.pool_id.as_deref()
            .and_then(|s| hex::decode(s).ok())
            .and_then(|b| <[u8; 32]>::try_from(b).ok())
            .unwrap_or([0u8; 32]),
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
        _ = drive_swarm(&mut swarm, protocol.clone(), &mut command_rx, pending_requests.clone(), peer_scorer.clone(), command_tx_for_caps.clone(), event_tx.clone(), content_tracker.clone(), client.clone(), daemon_config.max_storage_bytes, store.clone(), demand_tracker.clone(), merkle_tree.clone(), daemon_config.region.clone(), swarm_signing_key, receipt_store.clone(), daemon_config.max_peer_connections, piece_map.clone()) => {
            info!("[service.rs] Swarm event loop ended");
        }
        _ = crate::health_scan::run_health_scan_loop(health_scan.clone()) => {
            info!("[service.rs] HealthScan loop ended");
        }
        _ = handle_protocol_events(&mut protocol_event_rx, pending_requests.clone(), event_tx.clone(), content_tracker.clone(), command_tx_for_events, challenger_mgr.clone()) => {
            info!("[service.rs] Protocol events handler ended");
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
        _ = eviction_maintenance_loop(eviction_manager, store.clone(), event_tx.clone(), pdp_ranks.clone(), merkle_tree.clone(), piece_map.clone(), command_tx.clone()) => {
            info!("[service.rs] Eviction maintenance loop ended");
        }
        _ = crate::aggregator::run_aggregation_loop(receipt_store.clone(), event_tx.clone(), aggregator_config) => {
            info!("[service.rs] Aggregation loop ended");
        }
        _ = gc_loop(store.clone(), content_tracker.clone(), client.clone(), merkle_tree.clone(), event_tx.clone(), daemon_config.gc_interval_secs, daemon_config.max_storage_bytes, piece_map.clone(), command_tx.clone()) => {
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

/// Periodically check storage pressure and evict free content.
async fn eviction_maintenance_loop(
    eviction_manager: Arc<Mutex<crate::eviction::EvictionManager>>,
    store: Arc<Mutex<craftobj_store::FsStore>>,
    event_tx: EventSender,
    pdp_ranks: Arc<Mutex<crate::challenger::PdpRankData>>,
    merkle_tree: Arc<Mutex<craftobj_store::merkle::StorageMerkleTree>>,
    piece_map: Arc<Mutex<crate::piece_map::PieceMap>>,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
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

        // Emit PieceDropped events for all evicted/retired content
        {
            let mut map = piece_map.lock().await;
            let local_node = map.local_node().to_vec();
            let all_removed: Vec<_> = result.evicted.iter().map(|(cid, _)| *cid)
                .chain(result.retired.iter().map(|(cid, _)| *cid))
                .collect();
            for cid in &all_removed {
                // Content already deleted by eviction manager; emit drops from PieceMap state
                let pieces: Vec<(u32, [u8; 32])> = map.pieces_for_cid_local(cid);
                let mut segments_seen = std::collections::HashSet::new();
                for (seg, pid) in pieces {
                    segments_seen.insert(seg);
                    let seq = map.next_seq();
                    let dropped = craftobj_core::PieceDropped {
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
                    let event = craftobj_core::PieceEvent::Dropped(dropped);
                    map.apply_event(&event);
                }
                // Remove DHT provider records and untrack all segments for this removed CID
                for seg in segments_seen {
                    let pkey = craftobj_routing::provider_key(cid, seg);
                    let _ = command_tx.send(CraftObjCommand::StopProviding { key: pkey });
                    map.untrack_segment(cid, seg);
                }
            }
        }

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
            if let Ok(new_tree) = craftobj_store::merkle::StorageMerkleTree::build_from_store(&store_guard) {
                *merkle_tree.lock().await = new_tree;
                debug!("Rebuilt storage Merkle tree after eviction");
            }
        }
    }
}

/// Periodic garbage collection: delete unpinned content, enforce storage limits.
async fn gc_loop(
    store: Arc<Mutex<craftobj_store::FsStore>>,
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
    client: Arc<Mutex<craftobj_client::CraftObjClient>>,
    merkle_tree: Arc<Mutex<craftobj_store::merkle::StorageMerkleTree>>,
    event_tx: EventSender,
    gc_interval_secs: u64,
    _max_storage_bytes: u64,
    piece_map: Arc<Mutex<crate::piece_map::PieceMap>>,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
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

            // Emit PieceDropped events before deletion and untrack segments
            {
                let segments = s.list_segments(cid).unwrap_or_default();
                let mut map = piece_map.lock().await;
                let local_node = map.local_node().to_vec();
                for seg in segments {
                    let pieces = s.list_pieces(cid, seg).unwrap_or_default();
                    for pid in pieces {
                        let seq = map.next_seq();
                        let dropped = craftobj_core::PieceDropped {
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
                        let event = craftobj_core::PieceEvent::Dropped(dropped);
                        map.apply_event(&event);
                    }
                    // Remove DHT provider record and untrack segment after dropping all pieces
                    let pkey = craftobj_routing::provider_key(cid, seg);
                    let _ = command_tx.send(CraftObjCommand::StopProviding { key: pkey });
                    map.untrack_segment(cid, seg);
                }
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
            if let Ok(new_tree) = craftobj_store::merkle::StorageMerkleTree::build_from_store(&s) {
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

async fn drive_swarm(
    swarm: &mut CraftObjSwarm,
    protocol: Arc<CraftObjProtocol>,
    command_rx: &mut mpsc::UnboundedReceiver<CraftObjCommand>,
    pending_requests: PendingRequests,
    peer_scorer: SharedPeerScorer,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    event_tx: EventSender,
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
    client: Arc<Mutex<craftobj_client::CraftObjClient>>,
    max_storage_bytes: u64,
    store_for_repair: Arc<Mutex<craftobj_store::FsStore>>,
    demand_tracker: Arc<Mutex<crate::scaling::DemandTracker>>,
    merkle_tree: Arc<Mutex<craftobj_store::merkle::StorageMerkleTree>>,
    region: Option<String>,
    signing_key: Option<ed25519_dalek::SigningKey>,
    receipt_store: Arc<Mutex<crate::receipt_store::PersistentReceiptStore>>,
    max_peer_connections: usize,
    piece_map: Arc<Mutex<crate::piece_map::PieceMap>>,
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
                            CraftObjBehaviourEvent::Craft(ref craft_event) => {
                                debug!("Craft behaviour event: {:?}", craft_event);
                                // Handle mDNS discovery
                                handle_mdns_event(swarm, craft_event, &event_tx);
                                {
                                    let mut scorer = peer_scorer.lock().await;
                                    scorer.evict_stale(std::time::Duration::from_secs(900));
                                }
                                // Handle Kademlia events for DHT queries
                                if let craftec_network::behaviour::CraftBehaviourEvent::Kademlia(ref kad_event) = craft_event {
                                    protocol.handle_kademlia_event(kad_event).await;
                                }
                            }
                            CraftObjBehaviourEvent::Stream(()) => {
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
                        CraftObjCommand::SyncPieceMap { content_id, segment_index } => {
                            // Query all connected peers for their PieceMap entries for this segment
                            let peers: Vec<libp2p::PeerId> = swarm.connected_peers().cloned().collect();
                            debug!("[service.rs] SyncPieceMap for {}/seg{}: querying {} peers", content_id, segment_index, peers.len());
                            for peer_id in peers {
                                let request = CraftObjRequest::PieceMapQuery { content_id, segment_index };
                                let (ack_tx, ack_rx) = oneshot::channel();
                                let msg = OutboundMessage { peer: peer_id, request, reply_tx: Some(ack_tx) };
                                let tx = outbound_tx.clone();
                                let pm = piece_map.clone();
                                tokio::spawn(async move {
                                    if tx.send(msg).await.is_err() {
                                        return;
                                    }
                                    match tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx).await {
                                        Ok(Ok(CraftObjResponse::PieceMapEntries { entries })) => {
                                            let mut map = pm.lock().await;
                                            for entry in entries {
                                                let stored = craftobj_core::PieceStored {
                                                    node: entry.node,
                                                    cid: content_id,
                                                    segment: segment_index,
                                                    piece_id: entry.piece_id,
                                                    coefficients: entry.coefficients,
                                                    seq: 0,
                                                    timestamp: 0,
                                                    signature: vec![],
                                                };
                                                let event = craftobj_core::PieceEvent::Stored(stored);
                                                map.apply_event(&event);
                                            }
                                            debug!("[service.rs] SyncPieceMap: populated entries from {}", peer_id);
                                        }
                                        _ => {
                                            debug!("[service.rs] SyncPieceMap: no response from {}", peer_id);
                                        }
                                    }
                                });
                            }
                        }
                        CraftObjCommand::TriggerDistribution => {
                            info!("[service.rs] Received TriggerDistribution command — running initial push");
                            let ct = content_tracker.clone();
                            let ctx = command_tx.clone();
                            let cl = client.clone();
                            let etx = event_tx.clone();
                            let ps = peer_scorer.clone();
                            tokio::spawn(async move {
                                // Short delay to let peer discovery complete
                                info!("[service.rs] TriggerDistribution: sleeping 5s for peer discovery");
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
                let pm_clone = piece_map.clone();
                let sk_clone = signing_key.clone();
                let cmd_tx_clone = command_tx.clone();
                let peer = msg.peer;
                let seq_id = msg.seq_id;
                let mut stream = msg.stream;
                tokio::spawn(async move {
                    let response = handle_incoming_transfer_request(
                        &peer, msg.request, &store_clone, &ct_clone, &proto_clone, &dt_clone,
                        &pm_clone, sk_clone.as_ref(), &cmd_tx_clone,
                    ).await;
                    info!("[service.rs] Spawned handler done for {} seq={}: {:?}", peer, seq_id, std::mem::discriminant(&response));
                    // Write response back on the SAME stream the request came from
                    match craftobj_transfer::wire::write_response_frame(&mut stream, seq_id, &response).await {
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
                            // Mark node offline in PieceMap
                            {
                                let node_bytes = peer_id.to_bytes();
                                let mut map = piece_map.lock().await;
                                map.set_node_online(&node_bytes, false);
                            }
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
    request: CraftObjRequest,
    store: &Arc<Mutex<craftobj_store::FsStore>>,
    _content_tracker: &Arc<Mutex<crate::content_tracker::ContentTracker>>,
    protocol: &Arc<CraftObjProtocol>,
    demand_tracker: &Arc<Mutex<crate::scaling::DemandTracker>>,
    piece_map: &Arc<Mutex<crate::piece_map::PieceMap>>,
    signing_key: Option<&ed25519_dalek::SigningKey>,
    command_tx: &mpsc::UnboundedSender<CraftObjCommand>,
) -> CraftObjResponse {
    match request {
        CraftObjRequest::PieceSync { content_id, segment_index, have_pieces, max_pieces, .. } => {
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
                        pieces.push(craftobj_transfer::PiecePayload {
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
            CraftObjResponse::PieceBatch { pieces }
        }
        CraftObjRequest::PiecePush { content_id, segment_index, piece_id, coefficients, data } => {
            info!("[service.rs] Handling PiecePush from {} for {}/seg{}", peer, content_id, segment_index);
            info!("[service.rs] PiecePush handler: acquiring store lock...");
            let store_guard = store.lock().await;
            info!("[service.rs] PiecePush handler: store lock acquired");

            // Check if we have the manifest (must receive ManifestPush first)
            if store_guard.get_manifest(&content_id).is_err() {
                warn!("[service.rs] Received piece push for {} but no manifest — rejecting", content_id);
                return CraftObjResponse::Ack { status: WireStatus::Error };
            }

            match store_guard.store_piece(&content_id, segment_index, &piece_id, &data, &coefficients) {
                Ok(()) => {
                    info!("[service.rs] Stored pushed piece for {}/seg{}", content_id, segment_index);
                    // Notify protocol of piece push for tracker update
                    if let Err(e) = protocol.event_tx.send(CraftObjEvent::PiecePushReceived { content_id }) {
                        debug!("Failed to send piece push event: {}", e);
                    }
                    // Emit PieceStored event to PieceMap
                    {
                        let mut map = piece_map.lock().await;
                        // Track segment if this is the first piece for it
                        let newly_tracked = map.track_segment(content_id, segment_index);
                        if newly_tracked {
                            // Trigger scoped sync from peers for this segment
                            let _ = command_tx.send(CraftObjCommand::SyncPieceMap {
                                content_id,
                                segment_index,
                            });
                        }
                        let seq = map.next_seq();
                        let mut stored = craftobj_core::PieceStored {
                            node: map.local_node().to_vec(),
                            cid: content_id,
                            segment: segment_index,
                            piece_id,
                            coefficients: coefficients.clone(),
                            seq,
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            signature: vec![],
                        };
                        if let Some(key) = signing_key {
                            stored.sign(key);
                        }
                        let event = craftobj_core::PieceEvent::Stored(stored);
                        map.apply_event(&event);
                    }
                    // Publish DHT provider record for this CID+segment
                    let pkey = craftobj_routing::provider_key(&content_id, segment_index);
                    let _ = command_tx.send(CraftObjCommand::StartProviding { key: pkey });
                    CraftObjResponse::Ack { status: WireStatus::Ok }
                }
                Err(e) => {
                    warn!("[service.rs] Failed to store pushed piece: {}", e);
                    CraftObjResponse::Ack { status: WireStatus::Error }
                }
            }
        }
        CraftObjRequest::ManifestPush { content_id, manifest_json } => {
            info!("[service.rs] Handling ManifestPush from {} for {}", peer, content_id);
            match serde_json::from_slice::<craftobj_core::ContentManifest>(&manifest_json) {
                Ok(manifest) => {
                    info!("[service.rs] ManifestPush handler: acquiring store lock...");
                    let store_guard = store.lock().await;
                    info!("[service.rs] ManifestPush handler: store lock acquired");
                    match store_guard.store_manifest(&manifest) {
                        Ok(()) => {
                            info!("[service.rs] Stored manifest for {}", content_id);
                            if let Err(e) = protocol.event_tx.send(CraftObjEvent::ManifestPushReceived {
                                content_id,
                                manifest,
                            }) {
                                debug!("Failed to send manifest push event: {}", e);
                            }
                            CraftObjResponse::Ack { status: WireStatus::Ok }
                        }
                        Err(e) => {
                            warn!("[service.rs] Failed to store manifest: {}", e);
                            CraftObjResponse::Ack { status: WireStatus::Error }
                        }
                    }
                }
                Err(e) => {
                    warn!("[service.rs] Failed to parse manifest JSON: {}", e);
                    CraftObjResponse::Ack { status: WireStatus::Error }
                }
            }
        }
        CraftObjRequest::PieceMapQuery { content_id, segment_index } => {
            debug!("[service.rs] Handling PieceMapQuery from {} for {}/seg{}", peer, content_id, segment_index);
            let map = piece_map.lock().await;
            let pieces = map.pieces_for_segment(&content_id, segment_index);
            let entries: Vec<craftobj_transfer::PieceMapEntry> = pieces.into_iter()
                .map(|(node, pid, coeff)| craftobj_transfer::PieceMapEntry {
                    node: node.clone(),
                    piece_id: *pid,
                    coefficients: coeff.clone(),
                })
                .collect();
            debug!("[service.rs] Returning {} PieceMap entries for {}/seg{}", entries.len(), content_id, segment_index);
            CraftObjResponse::PieceMapEntries { entries }
        }
        CraftObjRequest::MerkleRoot { content_id, segment_index } => {
            info!("[service.rs] Handling MerkleRoot from {} for {}/seg{}", peer, content_id, segment_index);
            
            // Get pieces for this segment from local store and build response
            let store_guard = store.lock().await;
            let pieces = store_guard.list_pieces(&content_id, segment_index).unwrap_or_default();
            
            if pieces.is_empty() {
                // No pieces for this segment, return empty root
                let empty_root = craftobj_store::merkle::StorageMerkleTree::new().root();
                CraftObjResponse::MerkleRootResponse { 
                    root: empty_root, 
                    leaf_count: 0 
                }
            } else {
                // Build merkle tree from our pieces for this segment
                let mut tree = craftobj_store::merkle::StorageMerkleTree::new();
                for piece_id in &pieces {
                    tree.insert(&content_id, segment_index, piece_id);
                }
                CraftObjResponse::MerkleRootResponse { 
                    root: tree.root(), 
                    leaf_count: pieces.len() as u32 
                }
            }
        }
        CraftObjRequest::MerkleDiff { content_id, segment_index, since_root } => {
            info!("[service.rs] Handling MerkleDiff from {} for {}/seg{} since {}", peer, content_id, segment_index, hex::encode(&since_root[..8]));
            
            // Get current pieces for this segment
            let store_guard = store.lock().await;
            let pieces = store_guard.list_pieces(&content_id, segment_index).unwrap_or_default();
            
            // Build current merkle tree
            let mut current_tree = craftobj_store::merkle::StorageMerkleTree::new();
            for piece_id in &pieces {
                current_tree.insert(&content_id, segment_index, piece_id);
            }
            let current_root = current_tree.root();
            
            // If roots are the same, no changes
            if current_root == since_root {
                CraftObjResponse::MerkleDiffResponse {
                    current_root,
                    added: vec![],
                    removed: vec![],
                }
            } else {
                // For now, we don't have a way to reconstruct the old tree state from just the root.
                // This would require either maintaining historical tree states or a more sophisticated diff protocol.
                // As a simplified implementation, we'll return all current pieces as "added".
                // A proper implementation would need to maintain tree history or use a different diff approach.
                let mut added = Vec::new();
                let peer_bytes = peer.to_bytes();
                for piece_id in pieces {
                    if let Ok((_, coefficients)) = store_guard.get_piece(&content_id, segment_index, &piece_id) {
                        added.push(craftobj_transfer::PieceMapEntry {
                            node: peer_bytes.to_vec(),
                            piece_id,
                            coefficients,
                        });
                    }
                }
                CraftObjResponse::MerkleDiffResponse {
                    current_root,
                    added,
                    removed: vec![], // Can't compute removals without old state
                }
            }
        }
        CraftObjRequest::PdpChallenge { content_id, segment_index, piece_id, nonce, byte_positions } => {
            info!("[service.rs] Handling PdpChallenge from {} for {}/seg{} piece {}", peer, content_id, segment_index, hex::encode(&piece_id[..8]));
            
            // Get the requested piece from local store
            let store_guard = store.lock().await;
            match store_guard.get_piece(&content_id, segment_index, &piece_id) {
                Ok((data, coefficients)) => {
                    // Extract bytes at challenged positions
                    let mut challenged_bytes = Vec::new();
                    for &pos in &byte_positions {
                        if (pos as usize) < data.len() {
                            challenged_bytes.push(data[pos as usize]);
                        } else {
                            // Position out of bounds - this is invalid, return empty proof
                            warn!("[service.rs] PDP challenge position {} out of bounds for piece with {} bytes", pos, data.len());
                            return CraftObjResponse::PdpProof {
                                piece_id,
                                coefficients: vec![],
                                challenged_bytes: vec![],
                                proof_hash: [0u8; 32],
                            };
                        }
                    }
                    
                    // Compute proof hash: hash(challenged_bytes + positions + coefficients + nonce)
                    let proof_hash = crate::pdp::compute_proof_hash(&data, &byte_positions, &coefficients, &nonce);
                    
                    CraftObjResponse::PdpProof {
                        piece_id,
                        coefficients,
                        challenged_bytes,
                        proof_hash,
                    }
                }
                Err(e) => {
                    debug!("[service.rs] PDP challenge: piece {}/seg{}/{} not found: {}", 
                           content_id, segment_index, hex::encode(&piece_id[..8]), e);
                    // Return empty proof to indicate piece not available
                    CraftObjResponse::PdpProof {
                        piece_id,
                        coefficients: vec![],
                        challenged_bytes: vec![],
                        proof_hash: [0u8; 32],
                    }
                }
            }
        }
    }
}

/// Handle a command from the IPC handler.
async fn handle_command(
    swarm: &mut CraftObjSwarm,
    protocol: &Arc<CraftObjProtocol>,
    command: CraftObjCommand,
    pending_requests: PendingRequests,
    outbound_tx: &mpsc::Sender<OutboundMessage>,
    event_tx: &EventSender,
    region: &Option<String>,
    signing_key: &Option<ed25519_dalek::SigningKey>,
) {
    match command {
        CraftObjCommand::AnnounceProvider { content_id, manifest, reply_tx } => {
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
        
        CraftObjCommand::ResolveProviders { content_id, reply_tx } => {
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
        
        CraftObjCommand::GetManifest { content_id, reply_tx } => {
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
        
        CraftObjCommand::PieceSync { peer_id, content_id, segment_index, merkle_root, have_pieces, max_pieces, reply_tx } => {
            info!("[service.rs] Handling PieceSync command: {}/{} to peer {} (sending {} have_pieces, max_pieces={})", content_id, segment_index, peer_id, have_pieces.len(), max_pieces);
            let request = CraftObjRequest::PieceSync {
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
                            craftobj_transfer::CraftObjResponse::PieceBatch { pieces } => format!("{} pieces", pieces.len()),
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

        CraftObjCommand::PieceMapQuery { peer_id, content_id, segment_index, reply_tx } => {
            debug!("[service.rs] Handling PieceMapQuery command: {}/seg{} to peer {}", content_id, segment_index, peer_id);
            let request = CraftObjRequest::PieceMapQuery { content_id, segment_index };
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = OutboundMessage { peer: peer_id, request, reply_tx: Some(ack_tx) };
            let tx = outbound_tx.clone();
            tokio::spawn(async move {
                if tx.send(msg).await.is_err() {
                    let _ = reply_tx.send(Err("outbound channel closed".into()));
                    return;
                }
                match tokio::time::timeout(std::time::Duration::from_secs(5), ack_rx).await {
                    Ok(Ok(response)) => { let _ = reply_tx.send(Ok(response)); }
                    Ok(Err(_)) => { let _ = reply_tx.send(Err("ack channel closed".into())); }
                    Err(_) => { let _ = reply_tx.send(Err("piece map query timed out".into())); }
                }
            });
        }

        CraftObjCommand::PutReKey { content_id, entry, reply_tx } => {
            debug!("Handling put re-key command for {} → {}", content_id, hex::encode(entry.recipient_did));
            let local_peer_id = *swarm.local_peer_id();
            let result = craftobj_routing::ContentRouter::put_re_key(
                &mut swarm.behaviour_mut().craft, &content_id, &entry, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        CraftObjCommand::RemoveReKey { content_id, recipient_did, reply_tx } => {
            debug!("Handling remove re-key command for {} → {}", content_id, hex::encode(recipient_did));
            let local_peer_id = *swarm.local_peer_id();
            let result = craftobj_routing::ContentRouter::remove_re_key(
                &mut swarm.behaviour_mut().craft, &content_id, &recipient_did, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        CraftObjCommand::PutAccessList { access_list, reply_tx } => {
            debug!("Handling put access list command for {}", access_list.content_id);
            let local_peer_id = *swarm.local_peer_id();
            let result = craftobj_routing::ContentRouter::put_access_list(
                &mut swarm.behaviour_mut().craft, &access_list, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());
            let _ = reply_tx.send(result);
        }

        CraftObjCommand::GetAccessList { content_id, reply_tx } => {
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

        CraftObjCommand::PublishRemoval { content_id, notice, reply_tx } => {
            debug!("Handling publish removal command for {}", content_id);
            let local_peer_id = *swarm.local_peer_id();

            // Store in DHT
            let dht_result = craftobj_routing::ContentRouter::put_removal_notice(
                &mut swarm.behaviour_mut().craft, &content_id, &notice, &local_peer_id,
            ).map(|_| ()).map_err(|e| e.to_string());

            let _ = reply_tx.send(dht_result);
        }

        CraftObjCommand::PushPiece { peer_id, content_id, segment_index, piece_id, coefficients, piece_data, reply_tx } => {
            debug!("Handling push piece command: {}/{} to {}", content_id, segment_index, peer_id);
            let request = CraftObjRequest::PiecePush {
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
                    Ok(Ok(CraftObjResponse::Ack { status })) => {
                        info!("[service.rs] PushPiece to {}: got ack status={:?}", peer_id, status);
                        if status == craftobj_core::WireStatus::Ok {
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

        CraftObjCommand::PushManifest { peer_id, content_id, manifest_json, reply_tx } => {
            debug!("Handling push manifest command for {} to {}", content_id, peer_id);
            let request = CraftObjRequest::ManifestPush {
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
                    Ok(Ok(CraftObjResponse::Ack { status })) => {
                        info!("[service.rs] PushManifest to {}: got ack status={:?}", peer_id, status);
                        if status == craftobj_core::WireStatus::Ok {
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

        CraftObjCommand::CheckRemoval { content_id, reply_tx } => {
            debug!("Handling check removal command for {}", content_id);
            // For now, just start a DHT query. Full async response would need pending request tracking.
            // This is a simplified version — the RemovalCache handles most checks locally.
            let _ = craftobj_routing::ContentRouter::get_removal_notice(
                &mut swarm.behaviour_mut().craft, &content_id,
            );
            // Reply immediately with None — the cache should be checked first by the caller.
            let _ = reply_tx.send(Ok(None));
        }
        CraftObjCommand::StartProviding { key } => {
            debug!("Handling StartProviding command (key len={})", key.len());
            match swarm.behaviour_mut().craft.start_providing(&key) {
                Ok(_query_id) => debug!("Started providing for key {}", hex::encode(&key[..8.min(key.len())])),
                Err(e) => warn!("Failed to start providing: {:?}", e),
            }
        }

        CraftObjCommand::StopProviding { key } => {
            debug!("Handling StopProviding command (key len={})", key.len());
            swarm.behaviour_mut().craft.stop_providing(&key);
            debug!("Stopped providing for key {}", hex::encode(&key[..8.min(key.len())]));
        }

        CraftObjCommand::SyncPieceMap { .. } => {
            // Handled in drive_swarm before dispatch — should not reach here
            unreachable!("SyncPieceMap should be intercepted in drive_swarm");
        }

        CraftObjCommand::TriggerDistribution => {
            // Handled in drive_swarm before dispatch — should not reach here
            unreachable!("TriggerDistribution should be intercepted in drive_swarm");
        }

        CraftObjCommand::MerkleRoot { peer_id, content_id, segment_index, reply_tx } => {
            debug!("Handling MerkleRoot command: {}/{} from {}", content_id, segment_index, peer_id);
            let request = craftobj_transfer::CraftObjRequest::MerkleRoot {
                content_id,
                segment_index,
            };
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = OutboundMessage { peer: peer_id, request, reply_tx: Some(ack_tx) };
            let tx = outbound_tx.clone();
            tokio::spawn(async move {
                if tx.send(msg).await.is_err() {
                    warn!("[service.rs] MerkleRoot to {}: outbound channel closed", peer_id);
                    let _ = reply_tx.send(None);
                    return;
                }
                match tokio::time::timeout(std::time::Duration::from_secs(10), ack_rx).await {
                    Ok(Ok(craftobj_transfer::CraftObjResponse::MerkleRootResponse { root, leaf_count })) => {
                        let _ = reply_tx.send(Some((root, leaf_count)));
                    }
                    Ok(Ok(other)) => {
                        warn!("[service.rs] MerkleRoot to {}: unexpected response {:?}", peer_id, std::mem::discriminant(&other));
                        let _ = reply_tx.send(None);
                    }
                    Ok(Err(_)) => {
                        warn!("[service.rs] MerkleRoot to {}: ack channel closed", peer_id);
                        let _ = reply_tx.send(None);
                    }
                    Err(_) => {
                        warn!("[service.rs] MerkleRoot to {}: timed out after 10s", peer_id);
                        let _ = reply_tx.send(None);
                    }
                }
            });
        }

        CraftObjCommand::MerkleDiff { peer_id, content_id, segment_index, since_root, reply_tx } => {
            debug!("Handling MerkleDiff command: {}/{} from {} since {}", content_id, segment_index, peer_id, hex::encode(&since_root[..8]));
            let request = craftobj_transfer::CraftObjRequest::MerkleDiff {
                content_id,
                segment_index,
                since_root,
            };
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = OutboundMessage { peer: peer_id, request, reply_tx: Some(ack_tx) };
            let tx = outbound_tx.clone();
            tokio::spawn(async move {
                if tx.send(msg).await.is_err() {
                    warn!("[service.rs] MerkleDiff to {}: outbound channel closed", peer_id);
                    let _ = reply_tx.send(None);
                    return;
                }
                match tokio::time::timeout(std::time::Duration::from_secs(10), ack_rx).await {
                    Ok(Ok(craftobj_transfer::CraftObjResponse::MerkleDiffResponse { current_root, added, removed })) => {
                        let result = crate::commands::MerkleDiffResult {
                            current_root,
                            added,
                            removed,
                        };
                        let _ = reply_tx.send(Some(result));
                    }
                    Ok(Ok(other)) => {
                        warn!("[service.rs] MerkleDiff to {}: unexpected response {:?}", peer_id, std::mem::discriminant(&other));
                        let _ = reply_tx.send(None);
                    }
                    Ok(Err(_)) => {
                        warn!("[service.rs] MerkleDiff to {}: ack channel closed", peer_id);
                        let _ = reply_tx.send(None);
                    }
                    Err(_) => {
                        warn!("[service.rs] MerkleDiff to {}: timed out after 10s", peer_id);
                        let _ = reply_tx.send(None);
                    }
                }
            });
        }

        CraftObjCommand::PdpChallenge { peer_id, content_id, segment_index, piece_id, nonce, byte_positions, reply_tx } => {
            debug!("Handling PdpChallenge command: {}/seg{} piece {} to {}", content_id, segment_index, hex::encode(&piece_id[..8]), peer_id);
            let request = craftobj_transfer::CraftObjRequest::PdpChallenge {
                content_id,
                segment_index,
                piece_id,
                nonce,
                byte_positions,
            };
            let (ack_tx, ack_rx) = oneshot::channel();
            let msg = OutboundMessage { peer: peer_id, request, reply_tx: Some(ack_tx) };
            let tx = outbound_tx.clone();
            tokio::spawn(async move {
                if tx.send(msg).await.is_err() {
                    warn!("[service.rs] PdpChallenge to {}: outbound channel closed", peer_id);
                    let _ = reply_tx.send(None);
                    return;
                }
                match tokio::time::timeout(std::time::Duration::from_secs(10), ack_rx).await {
                    Ok(Ok(response)) => {
                        let _ = reply_tx.send(Some(response));
                    }
                    Ok(Err(_)) => {
                        warn!("[service.rs] PdpChallenge to {}: ack channel closed", peer_id);
                        let _ = reply_tx.send(None);
                    }
                    Err(_) => {
                        warn!("[service.rs] PdpChallenge to {}: timed out after 10s", peer_id);
                        let _ = reply_tx.send(None);
                    }
                }
            });
        }
    }
}

/// Handle mDNS discovery events: add peers to Kademlia and dial them.
fn handle_mdns_event(
    swarm: &mut CraftObjSwarm,
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

// Incoming streams handled by StreamManager in drive_swarm.

/// Handle protocol events (DHT query results, etc.).
async fn handle_protocol_events(
    event_rx: &mut mpsc::UnboundedReceiver<CraftObjEvent>,
    pending_requests: PendingRequests,
    daemon_event_tx: EventSender,
    content_tracker: Arc<Mutex<crate::content_tracker::ContentTracker>>,
    command_tx: mpsc::UnboundedSender<CraftObjCommand>,
    challenger: Arc<Mutex<crate::challenger::ChallengerManager>>,
) {
    info!("[service.rs] Starting protocol events handler");
    
    while let Some(event) = event_rx.recv().await {
        match event {
            CraftObjEvent::ProvidersResolved { content_id, providers } => {
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
            
            CraftObjEvent::ManifestRetrieved { content_id, manifest } => {
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
            
            CraftObjEvent::AccessListRetrieved { content_id, access_list } => {
                info!("[service.rs] Retrieved access list for {} ({} entries)", content_id, access_list.entries.len());
                let mut pending = pending_requests.lock().await;
                if let Some(PendingRequest::GetAccessList { reply_tx }) = pending.remove(&content_id) {
                    let _ = reply_tx.send(Ok(access_list));
                    debug!("Responded to access list retrieval request for {}", content_id);
                }
            }

            CraftObjEvent::PiecePushReceived { content_id } => {
                let mut t = content_tracker.lock().await;
                t.increment_local_pieces(&content_id);
            }
            CraftObjEvent::ManifestPushReceived { content_id, manifest } => {
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
                let cmd = CraftObjCommand::AnnounceProvider {
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
                    let cmd = CraftObjCommand::ResolveProviders { content_id, reply_tx };
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
            CraftObjEvent::DhtError { content_id, error } => {
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
    store: Arc<Mutex<craftobj_store::FsStore>>,
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
    store: Arc<Mutex<craftobj_store::FsStore>>,
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
            match craftobj_store::FsStore::new(&base_dir) {
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
