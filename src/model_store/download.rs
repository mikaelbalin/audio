//! Downloading model files with progress and integrity verification.
//!
//! A model is downloaded file-by-file into its own subdirectory. Each file is
//! written to a `.partial` path first and renamed into place only after its
//! SHA-256 matches (when known), so a half-downloaded or corrupted file is
//! never mistaken for a complete one.

use std::path::Path;

use sha2::{Digest, Sha256};

use super::catalog::{ModelEntry, ModelFile, file_url};
use super::paths;

use std::io::{Read, Write};

/// Download progress, in bytes already received of `total`.
#[derive(Debug, Clone, Copy)]
pub struct Progress {
    pub received: u64,
    pub total: u64,
}

impl Progress {
    /// Progress as a fraction in `0.0..=1.0`.
    pub fn fraction(self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.received as f64 / self.total as f64
        }
    }
}

/// Downloads every file of `entry`, invoking `on_progress` after each chunk of
/// bytes received. Returns once all files are verified and in place.
pub fn download_model<F>(entry: &ModelEntry, mut on_progress: F) -> Result<(), String>
where
    F: FnMut(Progress),
{
    let dir = paths::model_dir(&entry.id);
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;

    let total = entry.total_size();
    let mut received: u64 = 0;

    for file in &entry.files {
        let dest = dir.join(&file.name);
        if dest.exists() {
            // Already present: count its bytes and skip re-downloading.
            received += file.size;
            on_progress(Progress { received, total });
            continue;
        }
        received += download_file(entry, file, &dest, &mut |delta| {
            on_progress(Progress {
                received: received + delta,
                total,
            })
        })?;
    }

    Ok(())
}

/// Downloads a single file to `dest`, verifying its SHA-256 when `file.sha256`
/// is present. The `on_chunk` callback receives the number of new bytes.
fn download_file<F>(
    entry: &ModelEntry,
    file: &ModelFile,
    dest: &Path,
    mut on_chunk: F,
) -> Result<u64, String>
where
    F: FnMut(u64),
{
    let url = file_url(entry, file);
    let partial = dest.with_extension("partial");

    let response = reqwest::blocking::get(&url)
        .map_err(|e| format!("download {url}: {e}"))?
        .error_for_status()
        .map_err(|e| format!("download {url}: {e}"))?;

    let mut hasher = Sha256::new();
    let mut out = std::fs::File::create(&partial).map_err(|e| format!("create: {e}"))?;
    let mut written: u64 = 0;

    let mut reader = response;
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        out.write_all(&buf[..n])
            .map_err(|e| format!("write: {e}"))?;
        written += n as u64;
        on_chunk(n as u64);
    }
    out.flush().map_err(|e| format!("flush: {e}"))?;

    // Verify integrity before renaming into place.
    if let Some(expected) = &file.sha256 {
        let actual = format!("{:x}", hasher.finalize());
        if actual != *expected {
            let _ = std::fs::remove_file(&partial);
            return Err(format!(
                "sha256 mismatch for {}: expected {expected}, got {actual}",
                file.name
            ));
        }
    }

    std::fs::rename(&partial, dest).map_err(|e| format!("rename: {e}"))?;
    Ok(written)
}

/// Removes a model's directory and all its files.
pub fn delete_model(id: &str) -> Result<(), String> {
    let dir = paths::model_dir(id);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).map_err(|e| format!("delete: {e}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_fraction() {
        assert_eq!(
            Progress {
                received: 0,
                total: 100
            }
            .fraction(),
            0.0
        );
        assert_eq!(
            Progress {
                received: 50,
                total: 100
            }
            .fraction(),
            0.5
        );
        assert_eq!(
            Progress {
                received: 100,
                total: 100
            }
            .fraction(),
            1.0
        );
        assert_eq!(
            Progress {
                received: 0,
                total: 0
            }
            .fraction(),
            0.0
        );
    }
}
