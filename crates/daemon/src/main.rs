//! DataCraft Daemon entry point
//!
//! Usage:
//!   datacraft-daemon [OPTIONS]
//!
//! Options:
//!   --listen <ADDR>     Listen address (default: /ip4/0.0.0.0/tcp/0)
//!   --data-dir <PATH>   Data directory (default: platform-specific)
//!   --socket <PATH>     IPC socket path (default: /tmp/datacraft.sock)
//!   --log-level <LEVEL> Log level: trace, debug, info, warn, error (default: info)

use datacraft_daemon::service;
use craftec_network::NetworkConfig;
use libp2p::identity::Keypair;
use tracing::info;

fn parse_args() -> (String, std::path::PathBuf, String, String, u16, Option<std::path::PathBuf>) {
    let args: Vec<String> = std::env::args().collect();
    let mut listen = String::new();
    let mut data_dir: Option<std::path::PathBuf> = None;
    let mut socket: Option<String> = None;
    let mut log_level = "info".to_string();
    let mut ws_port: u16 = 9091;
    let mut config_path: Option<std::path::PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" => {
                i += 1;
                if i < args.len() { listen = args[i].clone(); }
            }
            "--data-dir" => {
                i += 1;
                if i < args.len() { data_dir = Some(std::path::PathBuf::from(&args[i])); }
            }
            "--socket" => {
                i += 1;
                if i < args.len() { socket = Some(args[i].clone()); }
            }
            "--log-level" => {
                i += 1;
                if i < args.len() { log_level = args[i].clone(); }
            }
            "--ws-port" => {
                i += 1;
                if i < args.len() { ws_port = args[i].parse().unwrap_or(9091); }
            }
            "--config" => {
                i += 1;
                if i < args.len() { config_path = Some(std::path::PathBuf::from(&args[i])); }
            }
            "--help" | "-h" => {
                eprintln!("DataCraft Daemon");
                eprintln!();
                eprintln!("Usage: datacraft-daemon [OPTIONS]");
                eprintln!();
                eprintln!("Options:");
                eprintln!("  --listen <ADDR>      Listen multiaddr (default: /ip4/0.0.0.0/tcp/0)");
                eprintln!("  --data-dir <PATH>    Data directory");
                eprintln!("  --socket <PATH>      IPC socket path (default: /tmp/datacraft.sock)");
                eprintln!("  --log-level <LEVEL>  Log level (default: info)");
                eprintln!("  --ws-port <PORT>     WebSocket server port (default: 9091, 0 to disable)");
                eprintln!("  --config <PATH>      Config file path (default: <data-dir>/config.json)");
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let data_dir = data_dir.unwrap_or_else(service::default_data_dir);
    let socket = socket.unwrap_or_else(service::default_socket_path);

    (listen, data_dir, socket, log_level, ws_port, config_path)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (listen, data_dir, socket_path, log_level, ws_port, config_path) = parse_args();

    // Initialize logging
    let level = match log_level.as_str() {
        "trace" => craftec_logging::LogLevel::Trace,
        "debug" => craftec_logging::LogLevel::Debug,
        "warn" => craftec_logging::LogLevel::Warn,
        "error" => craftec_logging::LogLevel::Error,
        _ => craftec_logging::LogLevel::Info,
    };
    craftec_logging::init(level);

    // Ensure data directory exists
    std::fs::create_dir_all(&data_dir)?;

    // Load or generate persistent node identity
    let key_path = data_dir.join("node.key");
    let node_signing_key = craftec_keystore::load_or_generate_keypair(&key_path)
        .map_err(|e| format!("Failed to load/generate node keypair: {}", e))?;

    // Derive libp2p keypair from the same ed25519 secret
    let secret_bytes = node_signing_key.secret_key_bytes();
    let mut ed_secret = secret_bytes.to_vec();
    let ed_libp2p = libp2p::identity::ed25519::SecretKey::try_from_bytes(&mut ed_secret)
        .expect("valid ed25519 secret");
    let keypair = Keypair::from(libp2p::identity::ed25519::Keypair::from(ed_libp2p));
    let peer_id = keypair.public().to_peer_id();

    let node_pubkey_hex = hex::encode(node_signing_key.public_key_bytes());

    eprintln!("╔══════════════════════════════════════════════════════════════╗");
    eprintln!("║  DataCraft Daemon                                          ║");
    eprintln!("╠══════════════════════════════════════════════════════════════╣");
    eprintln!("║  Peer ID:    {}  ║", &peer_id.to_string()[..46]);
    eprintln!("║  Pubkey:     {:<47}║", &node_pubkey_hex[..46]);
    eprintln!("║  Data dir:   {:<47}║", data_dir.display());
    eprintln!("║  IPC socket: {:<47}║", &socket_path);
    if ws_port > 0 {
        eprintln!("║  WS server:  {:<47}║", format!("ws://0.0.0.0:{}/ws", ws_port));
    } else {
        eprintln!("║  WS server:  {:<47}║", "disabled");
    }
    eprintln!("╚══════════════════════════════════════════════════════════════╝");

    let mut network_config = NetworkConfig {
        protocol_prefix: "datacraft".to_string(),
        ..Default::default()
    };

    if !listen.is_empty() {
        network_config.listen_addrs = vec![listen.parse()?];
    } else {
        // Use listen_port from config if available, otherwise random port
        let actual_config_path = config_path
            .as_ref()
            .cloned()
            .unwrap_or_else(|| data_dir.join("config.json"));
        if actual_config_path.exists() {
            if let Ok(raw) = std::fs::read_to_string(&actual_config_path) {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&raw) {
                    if let Some(port) = json.get("listen_port").and_then(|v| v.as_u64()) {
                        let addr = format!("/ip4/0.0.0.0/tcp/{}", port);
                        info!("Using listen_port {} from config", port);
                        network_config.listen_addrs = vec![addr.parse()?];
                    }
                }
            }
        }
    }

    info!("DataCraft daemon starting with peer ID {}", peer_id);

    // Convert craftec SigningKeypair to ed25519_dalek SigningKey
    let dalek_key = ed25519_dalek::SigningKey::from_bytes(&node_signing_key.secret_key_bytes());

    service::run_daemon_with_config(keypair, data_dir, socket_path, network_config, ws_port, config_path, Some(dalek_key)).await
}
