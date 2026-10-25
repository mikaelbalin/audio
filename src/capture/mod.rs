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
///
/// `source_id` semantics:
/// - `""` (empty) → the default microphone,
/// - an id that exists in [`list_sources`] → that exact source,
/// - anything else → error, with available sources listed in the message.
///
/// Listing alternatives in the error makes it easy for the UI to surface a
/// "device unavailable — pick another" hint instead of a silent fallback that
/// the user might not notice.
pub fn start(source_id: &str, sample_rate: u32) -> Result<Box<dyn AudioStream>, String> {
    let sources = list_sources()?;

    let resolved_id: String = if source_id.is_empty() {
        // Empty = default microphone.
        sources
            .iter()
            .find(|s| s.kind == SourceKind::Microphone && s.is_default)
            .map(|s| s.id.clone())
            .ok_or_else(|| "No default microphone available".to_string())?
    } else {
        match sources.iter().find(|s| s.id == source_id) {
            Some(s) => s.id.clone(),
            None => {
                let available: Vec<String> = sources
                    .iter()
                    .map(|s| format!("{} ({:?})", s.id, s.kind))
                    .collect();
                return Err(format!(
                    "Audio source unavailable: {source_id}. Available sources: {}",
                    available.join(", ")
                ));
            }
        }
    };

    // Re-resolve the resolved id to know its kind.
    let kind = sources
        .iter()
        .find(|s| s.id == resolved_id)
        .map(|s| s.kind)
        .ok_or_else(|| format!("Source disappeared while resolving: {resolved_id}"))?;

    match kind {
        SourceKind::Microphone => Ok(Box::new(Microphone::start(sample_rate, &resolved_id)?)),
        SourceKind::System => Ok(Box::new(SystemAudio::start(sample_rate, &resolved_id)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_source_id_includes_available_alternatives() {
        // This test needs `pactl` (PulseAudio/PipeWire) to enumerate sources.
        // On CI runners and other headless environments it is not installed,
        // so `list_sources()` fails before we can reach the "unavailable"
        // branch. Only assert when the environment can actually list sources.
        if list_sources().is_err() {
            return;
        }

        let result = start("__definitely_not_a_source__", 16000);
        let err = result.err().expect("unknown source must error");
        assert!(err.contains("unavailable"));
        // Even with no devices on the box, the message should at least
        // contain a bracketed "available sources" section (possibly empty).
        assert!(err.contains("Available sources:"));
    }
}
