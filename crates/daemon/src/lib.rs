//! CraftObj Daemon
//!
//! Background service that runs the CraftObj node:
//! - libp2p swarm event loop
//! - IPC server for desktop/CLI clients
//! - Content routing and transfer

pub mod aggregator;
pub mod api_key;
pub mod challenger;
pub mod channel_store;
pub mod config;
pub mod content_tracker;
pub mod events;
pub mod eviction;

pub mod disk_monitor;

pub mod commands;
pub mod errors;
pub mod health_scan;
pub mod handler;
pub mod health;
pub mod pdp;
pub mod peer_reconnect;
pub mod piece_transfer;
pub mod peer_scorer;
pub mod pex;
pub mod pex_wire;
pub mod protocol;
pub mod reannounce;
pub mod push_target;
pub mod receipt_store;
pub mod scaling;
pub mod removal_cache;
pub mod service;
pub mod settlement;
pub mod stream_manager;
pub mod behaviour;
pub mod settlement_cycle;
pub mod ws_server;
