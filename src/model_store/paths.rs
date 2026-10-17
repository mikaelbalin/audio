//! Resolution of the shared model directory.
//!
//! All consumers of the `audio` crate (Tauri apps and the standalone
//! `audio-server` alike) resolve models to the same on-disk location, so a
//! model downloaded by one app is reused by the others. The directory is:
//!
//! - `<data_dir>/kaelib/audio/models` (via the `directories` crate, XDG-aware),
//! - or `AUDIO_MODELS_DIR`, if that environment variable is set (override for
//!   tests and portable/embedded deployments).

use std::path::PathBuf;

/// Environment variable that overrides the resolved models directory.
pub const MODELS_DIR_ENV: &str = "AUDIO_MODELS_DIR";

/// Organisation / application qualifiers used by `directories`.
const ORG: &str = "kaelib";
const APP: &str = "audio";

/// Returns the directory where models live, creating nothing.
///
/// `AUDIO_MODELS_DIR` takes precedence; otherwise the XDG data dir
/// (`~/.local/share/kaelib/audio/models` on Linux) is used.
pub fn models_dir() -> PathBuf {
    if let Some(dir) = std::env::var_os(MODELS_DIR_ENV) {
        return PathBuf::from(dir);
    }

    directories::ProjectDirs::from("", ORG, APP)
        .map(|dirs| dirs.data_dir().join("models"))
        .unwrap_or_else(|| PathBuf::from("models"))
}

/// Returns the on-disk path for one model's files (its own subdirectory).
pub fn model_dir(model_id: &str) -> PathBuf {
    models_dir().join(model_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_override_wins() {
        // SAFETY: single-threaded test; we restore the previous value after.
        let key = MODELS_DIR_ENV;
        let prev = std::env::var_os(key);
        unsafe { std::env::set_var(key, "/tmp/audio-test-models") };
        assert_eq!(models_dir(), PathBuf::from("/tmp/audio-test-models"));
        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
    }

    #[test]
    fn model_dir_is_a_subdirectory_of_models_dir() {
        let dir = model_dir("parakeet-tdt-v3");
        assert!(dir.ends_with("parakeet-tdt-v3"));
    }
}
