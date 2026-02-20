//! Daemon configuration
//!
//! Configurable timing, capabilities, network, and behaviour parameters.
//! Loaded from a JSON file with env var overrides as escape hatch.

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, info, warn};

/// Current schema version. Bump when adding/removing/renaming fields.
pub const SCHEMA_VERSION: u32 = 2;

/// Daemon configuration — the single source of truth for a daemon instance.
///
/// Persisted to `{data_dir}/config.json`. CraftStudio writes this file;
/// the daemon reads it on startup.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    /// Schema version for migration support.
    pub schema_version: u32,

    // ── Capabilities ────────────────────────────────────────
    /// Node capabilities (e.g. ["client", "storage", "aggregator"]).
    /// This is the primary source — NOT env vars.
    pub capabilities: Vec<String>,

    // ── Network ─────────────────────────────────────────────
    /// libp2p listen port (0 = random). CLI --listen overrides this.
    pub listen_port: u16,
    /// WebSocket server port for IPC (0 = disabled).
    pub ws_port: u16,
    /// Unix socket path for IPC.
    pub socket_path: Option<String>,

    // ── Timing ──────────────────────────────────────────────
    /// Content reannounce check interval in seconds (default: 600)
    pub reannounce_interval_secs: u64,
    /// Content staleness threshold before re-announcing in seconds (default: 1200)
    pub reannounce_threshold_secs: u64,
    /// Challenger check interval in seconds (default: 300)
    pub challenger_interval_secs: Option<u64>,
    /// Aggregation epoch interval in seconds (default: 600 = 10 minutes).
    /// Set to None to use the default.
    pub aggregation_epoch_secs: Option<u64>,
    /// PEX (Peer Exchange) interval in seconds (default: 60).
    /// Lower values speed up peer discovery at the cost of more traffic.
    pub pex_interval_secs: u64,
    /// How often to refresh peer capabilities in seconds (default: 600).
    /// Keeps last_announcement fresh so stale eviction doesn't drop live peers.
    pub capability_refresh_interval_secs: u64,
    /// How often to run evict_stale on the peer scorer in seconds (default: 300).
    pub evict_stale_interval_secs: u64,
    /// How long a peer can be silent before triggering a capability re-request in seconds (default: 300).
    pub peer_heartbeat_timeout_secs: u64,

    // ── Storage ─────────────────────────────────────────────
    /// Maximum storage in bytes (0 = unlimited).
    pub max_storage_bytes: u64,
    /// Garbage collection sweep interval in seconds (default: 3600 = 1 hour).
    /// Set to 0 to disable GC.
    pub gc_interval_secs: u64,

    /// Maximum number of simultaneous peer connections (default: 50).
    /// New inbound connections are rejected when this limit is reached.
    pub max_peer_connections: usize,

    // ── Settlement ──────────────────────────────────────────
    /// Creator pool ID (32-byte hex). Used by aggregator for settlement receipts.
    /// Leave empty/None for nodes that don't participate in settlement.
    pub pool_id: Option<String>,

    // ── Concurrency & Timeouts ───────────────────────────────
    /// Maximum concurrent piece transfers (default: 64).
    pub max_concurrent_transfers: usize,
    /// Per-piece transfer timeout in seconds (default: 30).
    pub piece_timeout_secs: u64,
    /// Stream open timeout in seconds (default: 10).
    pub stream_open_timeout_secs: u64,
    /// Content health check interval in seconds (default: 300).
    pub health_check_interval_secs: u64,
    /// HealthScan periodic scan interval in seconds (default: 300).
    /// Controls how often the HealthScan loop runs to detect repair/degradation needs.
    pub health_scan_interval_secs: u64,
    /// HealthScan tier target multiplier (default: 1.5). Content with more than
    /// k × tier_target pieces on the network is considered over-replicated.
    pub health_scan_tier_target: Option<f64>,
    /// Minimum fetches in the demand window to consider content "hot" (default: 10).
    pub demand_threshold: u32,

    // ── Bootstrap ─────────────────────────────────────────────
    /// Bootstrap peer multiaddrs (e.g. ["/ip4/1.2.3.4/tcp/9000/p2p/12D3KooW..."]).
    /// Nodes dial these on startup for reliable discovery beyond mDNS.
    #[serde(default)]
    pub boot_peers: Vec<String>,

    // ── Geographic ──────────────────────────────────────────
    /// Geographic region of this node (e.g. "us-east", "eu-west").
    /// Included in capability announcements for geographic-aware routing.
    pub region: Option<String>,

    /// Unknown fields — preserved for forward compatibility.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            capabilities: vec!["client".to_string(), "storage".to_string()],
            listen_port: 0,
            ws_port: 9091,
            socket_path: None,
            reannounce_interval_secs: 600,
            reannounce_threshold_secs: 1200,
            challenger_interval_secs: None,
            aggregation_epoch_secs: None,
            pex_interval_secs: 60,
            capability_refresh_interval_secs: 600,
            evict_stale_interval_secs: 300,
            peer_heartbeat_timeout_secs: 300,
            max_concurrent_transfers: 64,
            piece_timeout_secs: 30,
            stream_open_timeout_secs: 10,
            health_check_interval_secs: 300,
            health_scan_interval_secs: 300,
            health_scan_tier_target: None,
            demand_threshold: 10,
            max_storage_bytes: 0,
            gc_interval_secs: 3600,
            max_peer_connections: 50,
            pool_id: None,
            boot_peers: Vec::new(),
            region: None,
            extra: serde_json::Map::new(),
        }
    }
}

impl DaemonConfig {
    /// Load config from a specific file path, falling back to defaults.
    /// Missing fields are filled from defaults (backward compat).
    /// Unknown fields are preserved (forward compat).
    /// Corrupt files → warning + defaults + overwrite.
    pub fn load_from(path: &Path) -> Self {
        let mut config = match std::fs::read_to_string(path) {
            Ok(data) => {
                match serde_json::from_str::<DaemonConfig>(&data) {
                    Ok(mut c) => {
                        info!("Loaded daemon config from {:?} (schema v{})", path, c.schema_version);
                        c.migrate();
                        c
                    }
                    Err(e) => {
                        warn!("Corrupt config {:?}: {} — using defaults", path, e);
                        let default = Self::default();
                        // Try to overwrite the corrupt file
                        if let Err(e2) = default.save_to(path) {
                            warn!("Failed to overwrite corrupt config: {}", e2);
                        }
                        default
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!("No config file at {:?}, using defaults", path);
                Self::default()
            }
            Err(e) => {
                warn!("Failed to read config {:?}: {} — using defaults", path, e);
                Self::default()
            }
        };

        config.apply_env_overrides();
        config
    }

    /// Load config from `{data_dir}/config.json`.
    pub fn load(data_dir: &Path) -> Self {
        Self::load_from(&data_dir.join("config.json"))
    }

    /// Save config to a specific file path. Creates parent dirs as needed.
    pub fn save_to(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(std::io::Error::other)?;
        // Atomic write: write to .tmp then rename
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Save config to `{data_dir}/config.json`.
    pub fn save(&self, data_dir: &Path) -> std::io::Result<()> {
        self.save_to(&data_dir.join("config.json"))
    }

    /// Migrate from older schema versions.
    fn migrate(&mut self) {
        if self.schema_version < 2 {
            // v1 → v2: capabilities, network fields added.
            // If capabilities is empty (old config didn't have it), set default.
            if self.capabilities.is_empty() {
                self.capabilities = vec!["client".to_string(), "storage".to_string()];
            }
            info!("Migrated config from v{} to v{}", self.schema_version, SCHEMA_VERSION);
        }
        self.schema_version = SCHEMA_VERSION;
    }

    /// Apply environment variable overrides (developer escape hatches only).
    fn apply_env_overrides(&mut self) {
        if let Ok(val) = std::env::var("CRAFTEC_CAPABILITIES") {
            let caps: Vec<String> = val.split(',')
                .map(|s| s.trim().to_lowercase())
                .filter(|s| !s.is_empty())
                .collect();
            if !caps.is_empty() {
                debug!("CRAFTEC_CAPABILITIES override: {:?}", caps);
                self.capabilities = caps;
            }
        }
        if let Ok(val) = std::env::var("CRAFTOBJ_REANNOUNCE_INTERVAL") {
            if let Ok(secs) = val.parse::<u64>() {
                debug!("CRAFTOBJ_REANNOUNCE_INTERVAL={}", secs);
                self.reannounce_interval_secs = secs;
            }
        }
        if let Ok(val) = std::env::var("CRAFTOBJ_REANNOUNCE_THRESHOLD") {
            if let Ok(secs) = val.parse::<u64>() {
                debug!("CRAFTOBJ_REANNOUNCE_THRESHOLD={}", secs);
                self.reannounce_threshold_secs = secs;
            }
        }
    }

    /// Merge partial JSON values into this config (for set-config IPC).
    pub fn merge(&mut self, partial: &Value) {
        if let Some(v) = partial.get("capabilities").and_then(|v| v.as_array()) {
            self.capabilities = v.iter()
                .filter_map(|s| s.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(v) = partial.get("listen_port").and_then(|v| v.as_u64()) {
            self.listen_port = v as u16;
        }
        if let Some(v) = partial.get("ws_port").and_then(|v| v.as_u64()) {
            self.ws_port = v as u16;
        }
        if let Some(v) = partial.get("socket_path") {
            if v.is_null() {
                self.socket_path = None;
            } else if let Some(s) = v.as_str() {
                self.socket_path = Some(s.to_string());
            }
        }
        if let Some(v) = partial.get("reannounce_interval_secs").and_then(|v| v.as_u64()) {
            self.reannounce_interval_secs = v;
        }
        if let Some(v) = partial.get("reannounce_threshold_secs").and_then(|v| v.as_u64()) {
            self.reannounce_threshold_secs = v;
        }
        if let Some(v) = partial.get("challenger_interval_secs") {
            if v.is_null() {
                self.challenger_interval_secs = None;
            } else if let Some(secs) = v.as_u64() {
                self.challenger_interval_secs = Some(secs);
            }
        }
        if let Some(v) = partial.get("max_storage_bytes").and_then(|v| v.as_u64()) {
            self.max_storage_bytes = v;
        }
        if let Some(v) = partial.get("max_peer_connections").and_then(|v| v.as_u64()) {
            self.max_peer_connections = v as usize;
        }
    }

    /// Fields that require a daemon restart to take effect.
    pub fn restart_required_fields() -> &'static [&'static str] {
        &["capabilities", "listen_port", "ws_port", "socket_path"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = DaemonConfig::default();
        assert_eq!(config.schema_version, SCHEMA_VERSION);
        assert_eq!(config.capabilities, vec!["client", "storage"]);
        assert_eq!(config.reannounce_interval_secs, 600);
        assert_eq!(config.reannounce_threshold_secs, 1200);
        assert!(config.challenger_interval_secs.is_none());
        assert_eq!(config.ws_port, 9091);
        assert_eq!(config.listen_port, 0);
    }

    #[test]
    fn test_load_missing_file() {
        let dir = std::env::temp_dir().join("daemon-config-test-missing");
        let config = DaemonConfig::load(&dir);
        assert_eq!(config.reannounce_interval_secs, 600);
    }

    #[test]
    fn test_save_and_load() {
        let dir = std::env::temp_dir().join(format!(
            "daemon-config-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = DaemonConfig::default();
        config.reannounce_interval_secs = 42;
        config.capabilities = vec!["client".to_string()];
        config.save(&dir).unwrap();

        let loaded = DaemonConfig::load(&dir);
        assert_eq!(loaded.reannounce_interval_secs, 42);
        assert_eq!(loaded.capabilities, vec!["client"]);
        assert_eq!(loaded.schema_version, SCHEMA_VERSION);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_merge() {
        let mut config = DaemonConfig::default();
        let partial = serde_json::json!({
            "reannounce_interval_secs": 999,
            "challenger_interval_secs": 120,
            "capabilities": ["client", "aggregator"],
        });
        config.merge(&partial);
        assert_eq!(config.reannounce_interval_secs, 999);
        assert_eq!(config.challenger_interval_secs, Some(120));
        assert_eq!(config.capabilities, vec!["client", "aggregator"]);
        // Unchanged
        assert_eq!(config.reannounce_threshold_secs, 1200);
    }

    #[test]
    fn test_unknown_fields_preserved() {
        // Includes deprecated `capability_announce_interval_secs` (eliminated per PIECE_TRACKING_DESIGN.md).
        // It is silently stored in `extra` and round-tripped — we test that unknown fields survive.
        let json = r#"{
            "schema_version": 2,
            "capabilities": ["client"],
            "capability_announce_interval_secs": 300,
            "reannounce_interval_secs": 600,
            "reannounce_threshold_secs": 1200,
            "future_field": "hello",
            "another_future": 42
        }"#;
        let dir = std::env::temp_dir().join(format!(
            "daemon-config-test-extra-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("config.json"), json).unwrap();

        let config = DaemonConfig::load(&dir);
        assert_eq!(config.extra.get("future_field").unwrap().as_str().unwrap(), "hello");
        assert_eq!(config.extra.get("another_future").unwrap().as_u64().unwrap(), 42);

        // Save and reload — extra fields should survive
        config.save(&dir).unwrap();
        let reloaded = DaemonConfig::load(&dir);
        assert_eq!(reloaded.extra.get("future_field").unwrap().as_str().unwrap(), "hello");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_v1_migration() {
        // Simulate a v1 config: no capabilities, no schema_version.
        // `capability_announce_interval_secs` was a v1 field (eliminated in current design);
        // it should be silently absorbed into `extra` without breaking load.
        let json = r#"{
            "capability_announce_interval_secs": 300,
            "reannounce_interval_secs": 600,
            "reannounce_threshold_secs": 1200
        }"#;
        let dir = std::env::temp_dir().join(format!(
            "daemon-config-test-migrate-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("config.json"), json).unwrap();

        let config = DaemonConfig::load(&dir);
        // Should have migrated to v2 with default capabilities
        assert_eq!(config.schema_version, SCHEMA_VERSION);
        assert_eq!(config.capabilities, vec!["client", "storage"]);

        std::fs::remove_dir_all(&dir).ok();
    }
}
