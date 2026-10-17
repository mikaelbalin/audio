//! Local transcription provider (Whisper-style models via transcribe-cpp).
//!
//! TODO: implement on top of `transcribe-cpp` (VAD + incremental transcription).
//! For now this only exists so the provider architecture is in place.

use crate::protocol::{AudioChunk, ServerMessage};

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::{SessionGuard, TranscriptionProvider};

/// Local transcription (runs in-process, no network).
///
/// This is the provider that will back the "work entirely offline /
/// no Mistral API" path. It is intentionally a stub today and will be wired
/// up in a dedicated step.
pub struct LocalProvider;

impl LocalProvider {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LocalProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl TranscriptionProvider for LocalProvider {
    fn stream(
        &self,
        _chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
        _on_event: Box<dyn FnMut(&ServerMessage) + Send>,
        _paused: Arc<AtomicBool>,
    ) -> Result<SessionGuard, String> {
        Err("local provider: not yet implemented".to_string())
    }
}
