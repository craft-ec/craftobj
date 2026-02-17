//! DataCraft wrapper behaviour
//!
//! Combines CraftBehaviour (generic networking) with request_response
//! for DataCraft-specific piece transfer.

use craftec_network::CraftBehaviour;
use craftec_network::behaviour::CraftBehaviourEvent;
use craftec_network::NetworkConfig;
use datacraft_transfer::{DataCraftCodec, DataCraftRequest, DataCraftResponse};
use libp2p::identity::Keypair;
use libp2p::{noise, request_response, tcp, yamux, PeerId, StreamProtocol, SwarmBuilder};
use libp2p::swarm::NetworkBehaviour;
use std::time::Duration;
use tracing::info;

/// Type alias for the DataCraft swarm.
pub type DataCraftSwarm = libp2p::Swarm<DataCraftBehaviour>;

/// Combined behaviour for DataCraft nodes.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "DataCraftBehaviourEvent")]
pub struct DataCraftBehaviour {
    /// Generic Craftec networking (Kademlia, Gossipsub, mDNS, etc.)
    pub craft: CraftBehaviour,
    /// DataCraft piece transfer protocol (request-response)
    pub transfer: request_response::Behaviour<DataCraftCodec>,
}

/// Events emitted by DataCraftBehaviour.
#[derive(Debug)]
pub enum DataCraftBehaviourEvent {
    Craft(CraftBehaviourEvent),
    Transfer(request_response::Event<DataCraftRequest, DataCraftResponse>),
}

impl From<CraftBehaviourEvent> for DataCraftBehaviourEvent {
    fn from(e: CraftBehaviourEvent) -> Self {
        DataCraftBehaviourEvent::Craft(e)
    }
}

impl From<request_response::Event<DataCraftRequest, DataCraftResponse>> for DataCraftBehaviourEvent {
    fn from(e: request_response::Event<DataCraftRequest, DataCraftResponse>) -> Self {
        DataCraftBehaviourEvent::Transfer(e)
    }
}

#[allow(deprecated)]
fn yamux_config() -> yamux::Config {
    let mut cfg = yamux::Config::default();
    cfg.set_max_num_streams(4096);
    cfg.set_receive_window_size(1024 * 1024);
    cfg.set_max_buffer_size(1024 * 1024);
    cfg
}

/// Build a DataCraft swarm with CraftBehaviour + request_response transfer.
pub async fn build_datacraft_swarm(
    keypair: Keypair,
    config: NetworkConfig,
) -> Result<(DataCraftSwarm, PeerId), Box<dyn std::error::Error + Send + Sync>> {
    let local_peer_id = PeerId::from(keypair.public());
    info!("Local peer ID: {}", local_peer_id);

    let protocol_prefix = config.protocol_prefix.clone();
    let transfer_protocol = StreamProtocol::new(datacraft_core::TRANSFER_PROTOCOL);

    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            yamux_config,
        )?
        .with_relay_client(noise::Config::new, yamux_config)?
        .with_behaviour(|key, relay_behaviour| {
            let peer_id = PeerId::from(key.public());
            let craft = CraftBehaviour::build(&protocol_prefix, peer_id, key, relay_behaviour)?;

            let transfer_cfg = request_response::Config::default()
                .with_request_timeout(Duration::from_secs(30));
            let transfer = request_response::Behaviour::with_codec(
                DataCraftCodec,
                [(transfer_protocol.clone(), request_response::ProtocolSupport::Full)],
                transfer_cfg,
            );

            Ok(DataCraftBehaviour { craft, transfer })
        })?
        .with_swarm_config(|c| {
            c.with_idle_connection_timeout(Duration::from_secs(300))
        })
        .build();

    // Start listening
    for addr in config.listen_addrs {
        swarm.listen_on(addr)?;
    }

    // Add bootstrap peers to Kademlia
    for (peer_id, addr) in config.bootstrap_peers {
        swarm.behaviour_mut().craft.add_address(&peer_id, addr);
    }

    Ok((swarm, local_peer_id))
}
