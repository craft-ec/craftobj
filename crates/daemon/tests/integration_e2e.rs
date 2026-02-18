//! End-to-End Integration Tests for CraftOBJ
//!
//! This test suite spawns real daemon instances in-process using tokio tasks
//! and tests P2P functionality across multiple nodes.
//!
//! ## Running Tests
//! 
//! Run all tests:
//! ```bash
//! cargo test --test integration_e2e
//! ```
//!
//! Run including ignored slow tests:
//! ```bash
//! cargo test --test integration_e2e -- --ignored
//! ```

use std::time::Duration;

use craftec_ipc::IpcClient;
use craftec_network::NetworkConfig;
use craftobj_daemon::config::DaemonConfig;
use craftobj_daemon::service::run_daemon_with_config;
use libp2p::identity::Keypair;
use libp2p::PeerId;
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, info, warn};

/// Test node that manages a daemon instance
struct TestNode {
    /// Node index for identification
    index: usize,
    /// Peer ID of this node
    peer_id: PeerId,
    /// Temporary directory for data storage
    data_dir: TempDir,
    /// Unix socket path for IPC
    socket_path: String,
    /// WebSocket port for IPC
    ws_port: u16,
    /// Listen port for libp2p (0 = random)
    listen_port: u16,
    /// Shutdown channel sender
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Task handle for the daemon
    daemon_handle: Option<tokio::task::JoinHandle<()>>,
    /// IPC client for communicating with the daemon
    ipc_client: Option<IpcClient>,
}

impl TestNode {
    /// Spawn a new daemon instance with unique ports and data directory
    async fn spawn(index: usize, boot_peers: Vec<String>) -> Result<Self, String> {
        let keypair = Keypair::generate_ed25519();
        let peer_id = PeerId::from(keypair.public());
        
        // Create unique temporary directory
        let data_dir = TempDir::new_in("/tmp")
            .map_err(|e| format!("Failed to create temp dir: {}", e))?;
        
        // Generate unique socket path
        let socket_path = format!("/tmp/craftobj-test-{}-{}.sock", index, rand::random::<u32>());
        
        // Use random high ports to avoid conflicts
        let ws_port = 0; // OS assigns random port
        let listen_port = 10000 + (rand::random::<u16>() % 50000); // random but known
        
        info!("Spawning test node {} with peer_id {} at {}", 
              index, peer_id, data_dir.path().display());

        // Create daemon config with test parameters
        let mut daemon_config = DaemonConfig::default();
        daemon_config.listen_port = listen_port;
        daemon_config.ws_port = ws_port;
        daemon_config.socket_path = Some(socket_path.clone());
        daemon_config.boot_peers = boot_peers;
        daemon_config.capabilities = vec!["client".to_string(), "storage".to_string()];
        
        // Set shorter intervals for tests
        daemon_config.reannounce_interval_secs = 30;
        daemon_config.reannounce_threshold_secs = 60;
        daemon_config.challenger_interval_secs = Some(60);
        daemon_config.aggregation_epoch_secs = Some(120);
        
        // Save config to data dir
        daemon_config.save(data_dir.path())
            .map_err(|e| format!("Failed to save config: {}", e))?;
        
        // Set up network config
        let network_config = NetworkConfig {
            listen_addrs: vec![format!("/ip4/127.0.0.1/tcp/{}", listen_port).parse()
                .map_err(|e| format!("Failed to parse listen address: {}", e))?],
            bootstrap_peers: vec![], // Will be handled via daemon_config.boot_peers
            protocol_prefix: "craftobj".to_string(),
        };
        
        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        
        // Spawn daemon in background task
        let daemon_keypair = keypair.clone();
        let daemon_data_dir = data_dir.path().to_path_buf();
        let daemon_socket_path = socket_path.clone();
        let daemon_ws_port = ws_port;
        
        let daemon_handle = tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx => {
                    info!("Test node {} daemon shutdown requested", index);
                }
                result = async {
                    run_daemon_with_config(
                        daemon_keypair,
                        daemon_data_dir,
                        daemon_socket_path,
                        network_config,
                        daemon_ws_port,
                        None,
                        None
                    ).await.map_err(|e| e.to_string())
                } => {
                    match result {
                        Ok(()) => info!("Test node {} daemon completed successfully", index),
                        Err(e) => warn!("Test node {} daemon error: {}", index, e),
                    }
                }
            };
            info!("Test node {} daemon task completed", index);
        });
        
        let mut test_node = TestNode {
            index,
            peer_id,
            data_dir,
            socket_path: socket_path.clone(),
            ws_port,
            listen_port,
            shutdown_tx: Some(shutdown_tx),
            daemon_handle: Some(daemon_handle),
            ipc_client: None,
        };
        
        // Wait for daemon to start and establish IPC connection
        test_node.connect_ipc().await?;
        
        Ok(test_node)
    }
    
    /// Establish IPC connection to the daemon
    async fn connect_ipc(&mut self) -> Result<(), String> {
        let start = Instant::now();
        let timeout_duration = Duration::from_secs(30);
        
        let client = IpcClient::new(&self.socket_path);
        
        while start.elapsed() < timeout_duration {
            if client.is_daemon_running().await {
                info!("Test node {} IPC connected", self.index);
                self.ipc_client = Some(client);
                return Ok(());
            } else {
                debug!("Test node {} daemon not yet running", self.index);
                sleep(Duration::from_millis(500)).await;
            }
        }
        
        Err(format!("Failed to connect IPC for test node {} within {}s", 
                   self.index, timeout_duration.as_secs()))
    }
    
    /// Send an IPC request to this node's daemon
    async fn rpc(&self, method: &str, params: Option<Value>) -> Result<Value, String> {
        let client = self.ipc_client.as_ref()
            .ok_or("No IPC client connected")?;
        
        client.send_request(method, params).await
            .map_err(|e| format!("IPC request failed: {}", e))
    }
    
    /// Get node status including listen addresses and peer info
    async fn status(&self) -> Result<Value, String> {
        self.rpc("status", None).await
    }
    
    /// Publish content to this node (writes to temp file, then publishes via path)
    async fn publish(&self, content: &[u8]) -> Result<String, String> {
        // Write content to a temp file in the data dir
        let file_path = self.data_dir.path().join("test_publish_input");
        std::fs::write(&file_path, content)
            .map_err(|e| format!("Failed to write test file: {}", e))?;
        let params = json!({
            "path": file_path.to_string_lossy(),
            "encrypted": false
        });
        
        let response = self.rpc("publish", Some(params)).await?;
        let cid = response["cid"].as_str()
            .ok_or("No CID in publish response")?
            .to_string();
        
        info!("Test node {} published content, CID: {}", self.index, cid);
        Ok(cid)
    }
    
    /// List content stored on this node
    async fn list(&self) -> Result<Value, String> {
        self.rpc("list", None).await
    }
    
    /// Get list of connected peers (from peer_scorer / gossipsub)
    async fn peers(&self) -> Result<Vec<Value>, String> {
        let response = self.rpc("peers", None).await?;
        let peers = response["peers"].as_array()
            .ok_or("No peers array in response")?
            .clone();
        Ok(peers)
    }

    /// Get list of connected peers at swarm level (raw libp2p connections)
    async fn connected_peers(&self) -> Result<Vec<String>, String> {
        let response = self.rpc("connected_peers", None).await?;
        let peers = response["peers"].as_array()
            .ok_or("No peers array in connected_peers response")?;
        Ok(peers.iter().filter_map(|v| v.as_str().map(String::from)).collect())
    }
    
    /// Get the listen addresses of this node
    async fn get_listen_addrs(&self) -> Result<Vec<String>, String> {
        Ok(vec![format!("/ip4/127.0.0.1/tcp/{}", self.listen_port)])
    }
    
    /// Get the full multiaddr with peer ID for use as boot peer
    async fn get_boot_peer_addr(&self) -> Result<String, String> {
        Ok(format!("/ip4/127.0.0.1/tcp/{}/p2p/{}", self.listen_port, self.peer_id))
    }
    
    /// Shutdown the daemon
    async fn shutdown(mut self) -> Result<(), String> {
        info!("Shutting down test node {}", self.index);
        
        // Close IPC client
        self.ipc_client = None;
        
        // Send shutdown signal
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        
        // Wait for daemon to finish
        if let Some(handle) = self.daemon_handle.take() {
            let _ = timeout(Duration::from_secs(10), handle).await;
        }
        
        // Clean up socket file
        if std::path::Path::new(&self.socket_path).exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
        
        info!("Test node {} shutdown complete", self.index);
        Ok(())
    }
}

/// Wait for nodes to connect to each other
async fn wait_for_connection(node_a: &TestNode, node_b: &TestNode, timeout_secs: u64) -> Result<(), String> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);
    
    info!("Waiting for connection between node {} and node {}", node_a.index, node_b.index);
    
    while start.elapsed() < timeout_duration {
        if let (Ok(peers_a), Ok(peers_b)) = (node_a.connected_peers().await, node_b.connected_peers().await) {
            let b_id = node_b.peer_id.to_string();
            let a_id = node_a.peer_id.to_string();
            let a_connected = peers_a.iter().any(|id| id == &b_id);
            let b_connected = peers_b.iter().any(|id| id == &a_id);
            
            if a_connected && b_connected {
                info!("Connection established between node {} and node {}", node_a.index, node_b.index);
                return Ok(());
            }
        }
        
        debug!("Connection not yet established, retrying... ({}s elapsed)", start.elapsed().as_secs());
        sleep(Duration::from_secs(2)).await;
    }
    
    Err(format!("Nodes {} and {} failed to connect within {}s", 
               node_a.index, node_b.index, timeout_secs))
}

/// Initialize tracing for tests
fn init_test_tracing() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    
    INIT.call_once(|| {
        let filter = std::env::var("RUST_LOG")
            .unwrap_or_else(|_| "info,craftobj=debug,libp2p=warn".to_string());
        
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .init();
    });
}

// Simple base64 encoding/decoding
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    
    let mut encoded = String::new();
    let mut i = 0;
    
    while i < data.len() {
        let b1 = data[i];
        let b2 = if i + 1 < data.len() { data[i + 1] } else { 0 };
        let b3 = if i + 2 < data.len() { data[i + 2] } else { 0 };
        
        let bitmap = ((b1 as u32) << 16) | ((b2 as u32) << 8) | (b3 as u32);
        
        encoded.push(CHARS[((bitmap >> 18) & 63) as usize] as char);
        encoded.push(CHARS[((bitmap >> 12) & 63) as usize] as char);
        encoded.push(if i + 1 < data.len() { CHARS[((bitmap >> 6) & 63) as usize] as char } else { '=' });
        encoded.push(if i + 2 < data.len() { CHARS[(bitmap & 63) as usize] as char } else { '=' });
        
        i += 3;
    }
    
    encoded
}

// ═══════════════════════════════════════════════════════════════════════════════
// Test Cases
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_single_node_publish_list() -> Result<(), String> {
    init_test_tracing();
    info!("=== Running test_single_node_publish_list ===");
    
    let timeout_duration = Duration::from_secs(60);
    timeout(timeout_duration, async {
        // Spawn a single node
        let node = TestNode::spawn(0, vec![]).await?;
        
        // Wait a moment for node to initialize
        sleep(Duration::from_secs(2)).await;
        
        // Publish a small file
        let test_content = b"Hello, CraftOBJ E2E test!";
        let cid = node.publish(test_content).await?;
        
        // Verify it appears in the list
        let list_response = node.list().await?;
        let contents = list_response.as_array()
            .ok_or("No array in list response")?;
        
        let found = contents.iter().any(|c| {
            c["content_id"].as_str().map_or(false, |id| id == cid)
        });
        if !found {
            return Err("Published content not found in list".to_string());
        }
        
        // Verify status shows stored bytes > 0
        let status = node.status().await?;
        let stored_bytes = status["stored_bytes"].as_u64().unwrap_or(0);
        if stored_bytes == 0 {
            return Err(format!("Status should show stored bytes > 0, got: {}", stored_bytes));
        }
        
        info!("✓ Single node publish/list test passed - CID: {}, stored: {} bytes", 
              cid, stored_bytes);
        
        node.shutdown().await?;
        Ok(())
    }).await.map_err(|_| "Test timed out".to_string())?
}

#[tokio::test]
async fn test_two_nodes_connect() -> Result<(), String> {
    init_test_tracing();
    info!("=== Running test_two_nodes_connect ===");
    
    let timeout_duration = Duration::from_secs(90);
    timeout(timeout_duration, async {
        // Spawn Node A
        let node_a = TestNode::spawn(0, vec![]).await?;
        
        // Wait for Node A to be ready and get its boot peer address
        sleep(Duration::from_secs(3)).await;
        let boot_addr = node_a.get_boot_peer_addr().await?;
        info!("Node A boot peer address: {}", boot_addr);
        
        // Spawn Node B with Node A as boot peer
        let node_b = TestNode::spawn(1, vec![boot_addr]).await?;
        
        // Wait for connection to establish
        wait_for_connection(&node_a, &node_b, 30).await?;
        
        // Verify both nodes see each other
        let peers_a = node_a.connected_peers().await?;
        let peers_b = node_b.connected_peers().await?;
        
        if peers_a.is_empty() {
            return Err("Node A should have peers".to_string());
        }
        if peers_b.is_empty() {
            return Err("Node B should have peers".to_string());
        }
        
        info!("✓ Two nodes connection test passed - {} peers on A, {} peers on B", 
              peers_a.len(), peers_b.len());
        
        // Shutdown nodes
        node_a.shutdown().await?;
        node_b.shutdown().await?;
        
        Ok(())
    }).await.map_err(|_| "Test timed out".to_string())?
}

#[tokio::test]
async fn test_publish_and_basic_functionality() -> Result<(), String> {
    init_test_tracing();
    info!("=== Running test_publish_and_basic_functionality ===");
    
    let timeout_duration = Duration::from_secs(120);
    timeout(timeout_duration, async {
        // Spawn Node A
        let node_a = TestNode::spawn(0, vec![]).await?;
        sleep(Duration::from_secs(3)).await;
        
        // Get boot peer address and spawn Node B
        let boot_addr = node_a.get_boot_peer_addr().await?;
        let node_b = TestNode::spawn(1, vec![boot_addr]).await?;
        
        // Wait for connection
        wait_for_connection(&node_a, &node_b, 30).await?;
        
        // Node A publishes a file
        let original_content = b"CraftOBJ E2E test content for P2P transfer verification";
        let cid = node_a.publish(original_content).await?;
        
        // Wait for some time for any distribution that might happen automatically
        sleep(Duration::from_secs(10)).await;
        
        // Verify both nodes can see the content exists (at least on node A)
        let list_a = node_a.list().await?;
        let contents_a = list_a.as_array()
            .ok_or("No array in list response from node A")?;
        
        let found_on_a = contents_a.iter().any(|c| {
            c["content_id"].as_str().map_or(false, |id| id == cid)
        });
        
        if !found_on_a {
            return Err("Published content not found on node A".to_string());
        }
        
        info!("✓ Publish and basic functionality test passed - content published and visible");
        
        // Note: Direct fetch between nodes may not work without provider resolution
        // TODO: Implement fetch via provider resolution when that functionality is available
        
        node_a.shutdown().await?;
        node_b.shutdown().await?;
        Ok(())
    }).await.map_err(|_| "Test timed out".to_string())?
}

#[tokio::test]
async fn test_node_spawn_shutdown() -> Result<(), String> {
    init_test_tracing();
    info!("=== Running test_node_spawn_shutdown ===");
    
    let timeout_duration = Duration::from_secs(30);
    timeout(timeout_duration, async {
        let node = TestNode::spawn(99, vec![]).await?;
        
        // Verify node is responsive
        let status = node.status().await?;
        if !status.is_object() {
            return Err("Status should return a JSON object".to_string());
        }
        
        // Clean shutdown
        node.shutdown().await?;
        
        info!("✓ Node spawn/shutdown test passed");
        Ok(())
    }).await.map_err(|_| "Test timed out".to_string())?
}

// TODO: Implement more advanced tests when the following functionality is available:
// - Provider resolution and cross-node fetch
// - PEX (Peer Exchange) discovery 
// - Health scan and Merkle pull
// - PDP (Proof of Data Possession) challenges
//
// For now, these tests would require:
// 1. Automatic content distribution between nodes
// 2. Provider resolution via DHT
// 3. Cross-node piece fetching
// 4. PEX implementation with configurable intervals
// 5. Health scan functionality accessible via IPC
// 6. PDP challenge system with receipt generation
//
// The current test suite focuses on:
// ✓ Basic node spawning and shutdown
// ✓ Node-to-node connectivity via libp2p
// ✓ Content publishing and local storage
// ✓ IPC communication with daemon

#[tokio::test]
#[ignore = "Requires full P2P implementation"]
async fn test_advanced_p2p_features() {
    // This test would include:
    // - PEX discovery between 3+ nodes
    // - Content fetch from remote providers
    // - Health scan and merkle pull
    // - PDP challenges and receipt verification
    
    // Placeholder for future implementation
    info!("TODO: Implement advanced P2P feature tests");
}