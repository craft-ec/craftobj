//! DataCraft Daemon
//!
//! Background service that runs the DataCraft node:
//! - libp2p swarm event loop
//! - IPC server for desktop/CLI clients
//! - Content routing and transfer

pub mod commands;
pub mod handler;
pub mod protocol;
pub mod service;
