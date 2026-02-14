//! DataCraft daemon service
//!
//! Manages the libp2p swarm, IPC server, and content operations.

use std::sync::Arc;

use craftec_ipc::IpcServer;
use craftec_network::{build_swarm, NetworkConfig};
use datacraft_client::DataCraftClient;
use libp2p::identity::Keypair;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::handler::DataCraftHandler;

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

/// Run the DataCraft daemon.
///
/// This starts the libp2p swarm and IPC server, then blocks until shutdown.
pub async fn run_daemon(
    keypair: Keypair,
    data_dir: std::path::PathBuf,
    socket_path: String,
    network_config: NetworkConfig,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Build client
    let client = DataCraftClient::new(&data_dir)?;
    let client = Arc::new(Mutex::new(client));

    // Build swarm
    let (mut swarm, local_peer_id) = build_swarm(keypair, network_config).await
        .map_err(|e| format!("Failed to build swarm: {}", e))?;
    info!("DataCraft node started: {}", local_peer_id);

    // Subscribe to status topics
    if let Err(e) = swarm
        .behaviour_mut()
        .subscribe_topic(datacraft_core::NODE_STATUS_TOPIC)
    {
        error!("Failed to subscribe to node status: {:?}", e);
    }

    // Start IPC server
    let ipc_server = IpcServer::new(&socket_path);
    let handler = Arc::new(DataCraftHandler::new(client.clone()));

    info!("Starting IPC server on {}", socket_path);

    // Run swarm event loop and IPC server concurrently
    tokio::select! {
        result = ipc_server.run(handler) => {
            if let Err(e) = result {
                error!("IPC server error: {}", e);
            }
        }
        _ = drive_swarm(&mut swarm) => {
            info!("Swarm event loop ended");
        }
    }

    Ok(())
}

/// Drive the swarm event loop.
async fn drive_swarm(swarm: &mut craftec_network::CraftSwarm) {
    use libp2p::swarm::SwarmEvent;
    use libp2p::futures::StreamExt;

    loop {
        match swarm.select_next_some().await {
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
                tracing::trace!("Behaviour event: {:?}", event);
            }
            _ => {}
        }
    }
}
