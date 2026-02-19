//! Disk space monitoring
//!
//! Periodically checks available disk space and emits warnings/refusals
//! when thresholds are exceeded.

use std::path::Path;

use tracing::{info, warn};

/// Disk space information.
#[derive(Debug, Clone)]
pub struct DiskSpace {
    pub used_bytes: u64,
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub percent_used: f64,
}

/// Warning threshold (90%).
pub const WARN_THRESHOLD: f64 = 0.90;
/// Refuse threshold (95%).
pub const REFUSE_THRESHOLD: f64 = 0.95;
/// Critical threshold for startup refusal (99%).
pub const CRITICAL_THRESHOLD: f64 = 0.99;

/// Get disk space for the filesystem containing the given path.
pub fn get_disk_space(path: &Path) -> Option<DiskSpace> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::mem::MaybeUninit;

        let c_path = CString::new(path.to_string_lossy().as_bytes()).ok()?;
        let mut stat = MaybeUninit::<libc::statvfs>::uninit();
        let ret = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };
        if ret != 0 {
            return None;
        }
        let stat = unsafe { stat.assume_init() };
        let total = stat.f_blocks as u64 * stat.f_frsize;
        let available = stat.f_bavail as u64 * stat.f_frsize;
        let used = total.saturating_sub(stat.f_bfree as u64 * stat.f_frsize);
        let percent = if total > 0 { used as f64 / total as f64 } else { 0.0 };
        Some(DiskSpace {
            used_bytes: used,
            total_bytes: total,
            available_bytes: available,
            percent_used: percent,
        })
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

/// Check if disk space is sufficient to accept new pieces.
pub fn should_accept_pieces(path: &Path) -> bool {
    match get_disk_space(path) {
        Some(ds) => ds.percent_used < REFUSE_THRESHOLD,
        None => true, // Can't check — allow
    }
}

/// Check startup disk space. Returns Err if critically low.
pub fn check_startup(path: &Path) -> Result<DiskSpace, String> {
    match get_disk_space(path) {
        Some(ds) => {
            if ds.percent_used >= CRITICAL_THRESHOLD {
                Err(format!(
                    "Disk critically full: {:.1}% used ({} / {} bytes)",
                    ds.percent_used * 100.0,
                    ds.used_bytes,
                    ds.total_bytes,
                ))
            } else {
                if ds.percent_used >= WARN_THRESHOLD {
                    warn!(
                        "Disk space warning: {:.1}% used ({} / {} bytes)",
                        ds.percent_used * 100.0,
                        ds.used_bytes,
                        ds.total_bytes,
                    );
                } else {
                    info!(
                        "Disk space OK: {:.1}% used, {} available",
                        ds.percent_used * 100.0,
                        ds.available_bytes,
                    );
                }
                Ok(ds)
            }
        }
        None => {
            warn!("Could not determine disk space");
            Ok(DiskSpace {
                used_bytes: 0,
                total_bytes: 0,
                available_bytes: u64::MAX,
                percent_used: 0.0,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_disk_space() {
        // Should work on the temp directory
        let ds = get_disk_space(std::env::temp_dir().as_path());
        #[cfg(unix)]
        {
            let ds = ds.unwrap();
            assert!(ds.total_bytes > 0);
            assert!(ds.percent_used >= 0.0 && ds.percent_used <= 1.0);
        }
    }

    #[test]
    fn test_should_accept_pieces() {
        assert!(should_accept_pieces(std::env::temp_dir().as_path()));
    }
}
