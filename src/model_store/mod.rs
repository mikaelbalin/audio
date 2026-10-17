//! Shared model storage: catalog, download, and on-disk layout.
//!
//! This module is the single place that knows *where* models live and *how* to
//! fetch and verify them. Both Tauri apps and the standalone `audio-server`
//! reuse it, so a model downloaded once is shared by every consumer.

pub mod catalog;
pub mod download;
pub mod paths;

pub use catalog::{ModelEntry, ModelFile, all_models, file_url, find};
pub use download::{Progress, delete_model, download_model};
pub use paths::{model_dir, models_dir};
