//! DataCraft Daemon
//!
//! Background service that runs the DataCraft node:
//! - libp2p swarm event loop
//! - IPC server for desktop/CLI clients
//! - Content routing and transfer

pub mod api_key;
pub mod challenger;
pub mod channel_store;
pub mod config;
pub mod content_tracker;

pub mod commands;
pub mod handler;
pub mod health;
pub mod pdp;
pub mod peer_scorer;
pub mod protocol;
pub mod reannounce;
pub mod receipt_store;
pub mod removal_cache;
pub mod service;
pub mod settlement;
pub mod ws_server;
