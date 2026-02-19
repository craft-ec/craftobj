//! CraftObj wrapper behaviour
//!
//! Combines CraftBehaviour (generic networking) with libp2p_stream
//! for CraftObj-specific piece transfer via persistent streams.

use craftec_network::CraftBehaviour;
use craftec_network::behaviour::CraftBehaviourEvent;
use craftec_network::NetworkConfig;
use libp2p::identity::Keypair;
use libp2p::{noise, tcp, yamux, PeerId, SwarmBuilder};
use libp2p::swarm::NetworkBehaviour;
use std::time::Duration;
use tracing::info;

/// Type alias for the CraftObj swarm.
pub type CraftObjSwarm = libp2p::Swarm<CraftObjBehaviour>;

/// Combined behaviour for CraftObj nodes.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "CraftObjBehaviourEvent")]
pub struct CraftObjBehaviour {
    /// Generic Craftec networking (Kademlia, mDNS, etc.)
    pub craft: CraftBehaviour,
    /// Persistent stream transport for piece transfer
    pub stream: libp2p_stream::Behaviour,
}

/// Events emitted by CraftObjBehaviour.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CraftObjBehaviourEvent {
    Craft(CraftBehaviourEvent),
    // libp2p_stream::Behaviour produces no events (streams are accepted via Control)
    #[allow(dead_code)]
    Stream(()),
}

impl From<CraftBehaviourEvent> for CraftObjBehaviourEvent {
    fn from(e: CraftBehaviourEvent) -> Self {
        CraftObjBehaviourEvent::Craft(e)
    }
}

impl From<()> for CraftObjBehaviourEvent {
    fn from(_: ()) -> Self {
        CraftObjBehaviourEvent::Stream(())
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

/// Build a CraftObj swarm with CraftBehaviour + libp2p_stream.
pub async fn build_craftobj_swarm(
    keypair: Keypair,
    config: NetworkConfig,
) -> Result<(CraftObjSwarm, PeerId), Box<dyn std::error::Error + Send + Sync>> {
    let local_peer_id = PeerId::from(keypair.public());
    info!("Local peer ID: {}", local_peer_id);

    let protocol_prefix = config.protocol_prefix.clone();
    let enable_mdns = config.enable_mdns;

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
            let craft = CraftBehaviour::build_with_options(&protocol_prefix, peer_id, key, relay_behaviour, enable_mdns)?;
            let stream = libp2p_stream::Behaviour::new();

            Ok(CraftObjBehaviour { craft, stream })
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
