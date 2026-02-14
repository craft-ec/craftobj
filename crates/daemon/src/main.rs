//! DataCraft Daemon entry point

use datacraft_daemon::service;
use craftec_network::NetworkConfig;
use libp2p::identity::Keypair;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    craftec_logging::init(craftec_logging::LogLevel::Info);

    info!("DataCraft daemon starting...");

    let keypair = Keypair::generate_ed25519();
    let data_dir = service::default_data_dir();
    let socket_path = service::default_socket_path();

    let network_config = NetworkConfig {
        protocol_prefix: "datacraft".to_string(),
        ..Default::default()
    };

    service::run_daemon(keypair, data_dir, socket_path, network_config).await
}
