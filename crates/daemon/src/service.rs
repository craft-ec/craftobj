//! DataCraft daemon service
//!
//! Manages the libp2p swarm, IPC server, and content operations.

use std::collections::HashMap;
use std::sync::Arc;

use craftec_ipc::IpcServer;
use craftec_network::{build_swarm, NetworkConfig};
use datacraft_client::DataCraftClient;
use datacraft_core::{ContentId, ChunkManifest};
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

/// Tracks pending DHT requests from IPC commands.
#[derive(Debug)]
enum PendingRequest {
    ResolveProviders {
        reply_tx: oneshot::Sender<Result<Vec<libp2p::PeerId>, String>>,
    },
    GetManifest {
        reply_tx: oneshot::Sender<Result<ChunkManifest, String>>,
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
    let (mut swarm, local_peer_id) = build_swarm(keypair, network_config).await
        .map_err(|e| format!("Failed to build swarm: {}", e))?;
    info!("DataCraft node started: {}", local_peer_id);

    // Create event channel for protocol communication
    let (protocol_event_tx, mut protocol_event_rx) = mpsc::unbounded_channel::<DataCraftEvent>();
    
    // Create command channel for IPC → swarm communication
    let (command_tx, mut command_rx) = mpsc::unbounded_channel::<DataCraftCommand>();
    
    // Create pending requests tracker for DHT operations
    let pending_requests: PendingRequests = Arc::new(Mutex::new(HashMap::new()));

    // Create and register DataCraft protocol
    let protocol = DataCraftProtocol::new(store.clone(), protocol_event_tx);
    let incoming_streams = protocol.register(&mut swarm)
        .map_err(|e| format!("Failed to register DataCraft protocol: {}", e))?;
    let protocol = Arc::new(protocol);

    // Subscribe to gossipsub topics
    if let Err(e) = swarm
        .behaviour_mut()
        .subscribe_topic(datacraft_core::NODE_STATUS_TOPIC)
    {
        error!("Failed to subscribe to node status: {:?}", e);
    }

    // Start IPC server with enhanced handler
    let ipc_server = IpcServer::new(&socket_path);
    let handler = Arc::new(DataCraftHandler::new(client.clone(), protocol.clone(), command_tx));

    info!("Starting IPC server on {}", socket_path);

    // Run all components concurrently
    tokio::select! {
        result = ipc_server.run(handler) => {
            if let Err(e) = result {
                error!("IPC server error: {}", e);
            }
        }
        _ = drive_swarm(&mut swarm, protocol.clone(), &mut command_rx, pending_requests.clone()) => {
            info!("Swarm event loop ended");
        }
        _ = handle_incoming_streams(incoming_streams, protocol.clone()) => {
            info!("Incoming streams handler ended");
        }
        _ = handle_protocol_events(&mut protocol_event_rx) => {
            info!("Protocol events handler ended");
        }
    }

    Ok(())
}

/// Drive the swarm event loop.
async fn drive_swarm(
    swarm: &mut craftec_network::CraftSwarm,
    protocol: Arc<DataCraftProtocol>,
    command_rx: &mut mpsc::UnboundedReceiver<DataCraftCommand>,
    pending_requests: PendingRequests,
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
                    }
                    debug!("Responded to DHT request with error for {}", content_id);
                }
            }
        }
    }
}
