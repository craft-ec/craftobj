//! Daemon configuration
//!
//! Configurable timing and behaviour parameters for the DataCraft daemon.
//! Loaded from a JSON file with env var overrides.

use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Daemon configuration with timing parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DaemonConfig {
    /// Capability announcement interval in seconds (default: 300)
    pub capability_announce_interval_secs: u64,
    /// Content reannounce check interval in seconds (default: 600)
    pub reannounce_interval_secs: u64,
    /// Content staleness threshold before re-announcing in seconds (default: 1200)
    pub reannounce_threshold_secs: u64,
    /// Challenger check interval in seconds (default: 300)
    pub challenger_interval_secs: Option<u64>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            capability_announce_interval_secs: 300,
            reannounce_interval_secs: 600,
            reannounce_threshold_secs: 1200,
            challenger_interval_secs: None,
        }
    }
}

impl DaemonConfig {
    /// Load config from a specific file path, falling back to defaults.
    /// Env vars always take priority.
    pub fn load_from(path: &Path) -> Self {
        let mut config = match std::fs::read_to_string(path) {
            Ok(data) => {
                match serde_json::from_str::<DaemonConfig>(&data) {
                    Ok(c) => {
                        info!("Loaded daemon config from {:?}", path);
                        c
                    }
                    Err(e) => {
                        warn!("Failed to parse config {:?}: {}, using defaults", path, e);
                        Self::default()
                    }
                }
            }
            Err(_) => {
                debug!("No config file at {:?}, using defaults", path);
                Self::default()
            }
        };

        config.apply_env_overrides();
        config
    }

    /// Load config from `{data_dir}/config.json`, with env var overrides.
    pub fn load(data_dir: &Path) -> Self {
        Self::load_from(&data_dir.join("config.json"))
    }

    /// Save config to a specific file path.
    pub fn save_to(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(std::io::Error::other)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, json)
    }

    /// Save config to `{data_dir}/config.json`.
    pub fn save(&self, data_dir: &Path) -> std::io::Result<()> {
        self.save_to(&data_dir.join("config.json"))
    }

    /// Apply environment variable overrides (env vars take priority).
    fn apply_env_overrides(&mut self) {
        if let Ok(val) = std::env::var("DATACRAFT_CAPABILITY_ANNOUNCE_INTERVAL") {
            if let Ok(secs) = val.parse::<u64>() {
                debug!("DATACRAFT_CAPABILITY_ANNOUNCE_INTERVAL={}", secs);
                self.capability_announce_interval_secs = secs;
            }
        }
        if let Ok(val) = std::env::var("DATACRAFT_REANNOUNCE_INTERVAL") {
            if let Ok(secs) = val.parse::<u64>() {
                debug!("DATACRAFT_REANNOUNCE_INTERVAL={}", secs);
                self.reannounce_interval_secs = secs;
            }
        }
        if let Ok(val) = std::env::var("DATACRAFT_REANNOUNCE_THRESHOLD") {
            if let Ok(secs) = val.parse::<u64>() {
                debug!("DATACRAFT_REANNOUNCE_THRESHOLD={}", secs);
                self.reannounce_threshold_secs = secs;
            }
        }
    }

    /// Merge partial JSON values into this config (for set-config IPC).
    pub fn merge(&mut self, partial: &serde_json::Value) {
        if let Some(v) = partial.get("capability_announce_interval_secs").and_then(|v| v.as_u64()) {
            self.capability_announce_interval_secs = v;
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let config = DaemonConfig::default();
        assert_eq!(config.capability_announce_interval_secs, 300);
        assert_eq!(config.reannounce_interval_secs, 600);
        assert_eq!(config.reannounce_threshold_secs, 1200);
        assert!(config.challenger_interval_secs.is_none());
    }

    #[test]
    fn test_load_missing_file() {
        let dir = std::env::temp_dir().join("daemon-config-test-missing");
        let config = DaemonConfig::load(&dir);
        assert_eq!(config.capability_announce_interval_secs, 300);
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
        config.save(&dir).unwrap();

        let loaded = DaemonConfig::load(&dir);
        assert_eq!(loaded.reannounce_interval_secs, 42);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_merge() {
        let mut config = DaemonConfig::default();
        let partial = serde_json::json!({
            "reannounce_interval_secs": 999,
            "challenger_interval_secs": 120,
        });
        config.merge(&partial);
        assert_eq!(config.reannounce_interval_secs, 999);
        assert_eq!(config.challenger_interval_secs, Some(120));
        // Unchanged
        assert_eq!(config.capability_announce_interval_secs, 300);
    }
}
