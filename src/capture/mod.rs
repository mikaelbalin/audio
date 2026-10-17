//! Audio capture (Linux only).
//!
//! Produces a stream of [`AudioChunk`]s for a given source (microphone or
//! system output). Platform-specific mechanics live in sibling modules;
//! this module defines the shared interface and a small factory.

use crate::protocol::{AudioChunk, AudioSource, SourceKind};

mod microphone;
mod system;

use microphone::Microphone;
use system::SystemAudio;

/// Anything that yields successive raw PCM chunks until it is stopped or
/// the stream ends. Implementations are blocking by design — the owning
/// session runs them on a dedicated thread.
pub trait AudioStream: Send {
    /// Returns the next chunk, or `None` once the source is exhausted /
    /// stopped. Implementations should not block indefinitely once a stop
    /// has been requested.
    fn next_chunk(&mut self) -> Option<AudioChunk>;
}

/// Lists all capturable sources (both microphones and system monitors).
pub fn list_sources() -> Result<Vec<AudioSource>, String> {
    let mut sources = microphone::list()?;
    sources.extend(system::list()?);
    Ok(sources)
}

/// Starts capturing from the given source id and returns the stream.
pub fn start(source_id: &str, sample_rate: u32) -> Result<Box<dyn AudioStream>, String> {
    let sources = list_sources()?;
    let source = sources
        .iter()
        .find(|s| s.id == source_id)
        .ok_or_else(|| format!("Unknown audio source: {source_id}"))?;

    match source.kind {
        SourceKind::Microphone => Ok(Box::new(Microphone::start(sample_rate, source_id)?)),
        SourceKind::System => Ok(Box::new(SystemAudio::start(sample_rate, source_id)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_source_id_is_an_error() {
        // We cannot guarantee any devices exist in the test environment, so
        // only assert the error path with a guaranteed-unknown id.
        let result = start("__definitely_not_a_source__", 16000);
        assert!(result.is_err());
    }
}
