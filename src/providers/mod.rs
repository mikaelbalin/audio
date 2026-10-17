//! Transcription providers: turn a stream of [`AudioChunk`]s into events.
//!
//! Each implementation knows nothing about **how** audio was captured — it
//! receives a pull-stream of PCM chunks and yields [`ServerMessage`]s. This
//! keeps capture and transcription fully decoupled, so backends can be
//! swapped (cloud Mistral, local models, etc.) without touching capture.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::protocol::{AudioChunk, ProviderKind, ServerMessage};

mod local;
mod mistral;

pub use local::LocalProvider;
pub use mistral::{MistralConfig, MistralProvider};

/// Configuration common to every provider.
pub trait TranscriptionProvider {
    /// Runs a session: consumes chunks from `chunks`, emits events to `on_event`.
    ///
    /// This is intentionally pull-based and blocking-free from the caller's
    /// point of view (implementations drive their own async runtime). The
    /// `on_event` callback is invoked as events arrive; return `false` from it
    /// to stop the session early, or drop the returned [`SessionGuard`] to
    /// request shutdown.
    fn stream(
        &self,
        chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
        on_event: Box<dyn FnMut(&ServerMessage) + Send>,
    ) -> Result<SessionGuard, String>;
}

/// A handle to a running session. Dropping it requests graceful shutdown and
/// waits for the provider's worker thread to exit.
pub struct SessionGuard {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl SessionGuard {
    /// Signals the session to stop and waits for the worker thread to exit.
    pub fn finish(mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Instantiates a provider from the protocol-level kind.
pub fn build(
    kind: ProviderKind,
    cfg: ProviderConfig,
) -> Result<Box<dyn TranscriptionProvider>, String> {
    match kind {
        ProviderKind::Mistral => Ok(Box::new(MistralProvider::new(cfg.mistral))),
        ProviderKind::Local => Err("local provider: not yet implemented".to_string()),
    }
}

/// Configuration passed to a provider at construction time.
#[derive(Debug, Clone, Default)]
pub struct ProviderConfig {
    pub mistral: MistralConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_provider_is_not_yet_implemented() {
        let result = build(ProviderKind::Local, ProviderConfig::default());
        assert!(result.is_err());
    }
}
