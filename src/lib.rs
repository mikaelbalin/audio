//! `audio` — unified audio capture and transcription layer.
//!
//! This crate is the single source of truth for audio work across
//! `speech-transcription` and `chatbot`. It is organized into small,
//! focused modules:
//!
//! - [`protocol`] — shared event/command types (the contract).
//! - capture, providers, session, tauri — added incrementally as the
//!   crate grows.

pub mod capture;
pub mod protocol;
pub mod providers;
pub mod session;
