//! API key management for the WebSocket server.
//!
//! On first start, generates a random 32-byte hex key and saves it to
//! `<data-dir>/api_key`. On subsequent starts, reads the existing key.
//! The key is also written to `~/.craftobj/api_key` for CraftStudio/CLI access.

use std::fs;
use std::path::{Path, PathBuf};

use rand::Rng;
use tracing::{debug, info};

/// Load or generate the API key from the data directory.
///
/// Returns the hex-encoded 32-byte key.
pub fn load_or_generate(data_dir: &Path) -> std::io::Result<String> {
    let key_path = data_dir.join("api_key");

    let key = if key_path.exists() {
        let raw = fs::read_to_string(&key_path)?;
        let trimmed = raw.trim().to_string();
        debug!("Loaded existing API key from {}", key_path.display());
        trimmed
    } else {
        let mut bytes = [0u8; 32];
        rand::thread_rng().fill(&mut bytes);
        let key = hex::encode(bytes);
        fs::write(&key_path, &key)?;
        info!("Generated new API key at {}", key_path.display());
        key
    };

    // Also write to ~/.craftobj/api_key for CraftStudio/CLI
    if let Some(well_known) = well_known_path() {
        if let Some(parent) = well_known.parent() {
            let _ = fs::create_dir_all(parent);
        }
        // Only write if different (or doesn't exist)
        let needs_write = fs::read_to_string(&well_known)
            .map(|existing| existing.trim() != key)
            .unwrap_or(true);
        if needs_write {
            fs::write(&well_known, &key)?;
            debug!("Wrote API key to {}", well_known.display());
        }
    }

    Ok(key)
}

/// Well-known path: `~/.craftobj/api_key`
fn well_known_path() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(|h| PathBuf::from(h).join(".craftobj").join("api_key"))
}

/// Read the API key from the well-known path (`~/.craftobj/api_key`).
///
/// Used by the CLI to authenticate with the daemon.
pub fn read_from_well_known() -> Option<String> {
    well_known_path()
        .and_then(|p| fs::read_to_string(p).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn generates_and_reloads_key() {
        let dir = tempfile::tempdir().unwrap();
        let key1 = load_or_generate(dir.path()).unwrap();
        assert_eq!(key1.len(), 64); // 32 bytes hex
        let key2 = load_or_generate(dir.path()).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn reads_existing_key() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("api_key"), "abcd1234").unwrap();
        let key = load_or_generate(dir.path()).unwrap();
        assert_eq!(key, "abcd1234");
    }
}
