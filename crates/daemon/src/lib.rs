//! DataCraft Daemon
//!
//! Background service that runs the DataCraft node:
//! - libp2p swarm event loop
//! - IPC server for desktop/CLI clients
//! - Content routing and transfer

pub mod channel_store;
pub mod commands;
pub mod handler;
pub mod health;
pub mod pdp;
pub mod protocol;
pub mod receipt_store;
pub mod service;
