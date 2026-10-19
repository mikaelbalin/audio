//! Local transcription provider (streaming, via transcribe-cpp / ggml).
//!
//! Runs a GGUF model entirely in-process, so no network or API key is needed.
//! Every model in the catalog is streaming-capable (Parakeet Unified, Nemotron
//! Streaming, Voxtral Realtime), so there is a single code path: feed
//! `pcm_s16le` chunks to a [`transcribe_cpp::Stream`] and emit `Delta` events as
//! the model's committed/tentative text grows.
//!
//! The model emits punctuation and capitalization natively (no separate PnC
//! pass), and transcribe-cpp's committed/tentative split means the transcript
//! never duplicates or "jumps" — we send only the *newly committed* suffix on
//! each change.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use transcribe_cpp::{Model, RunOptions, StreamOptions};

use crate::model_store;
use crate::protocol::{AudioChunk, LocalConfig, ServerMessage};

use super::{SessionGuard, TranscriptionProvider};

/// Local transcription (runs in-process, no network).
pub struct LocalProvider {
    config: LocalConfig,
}

impl LocalProvider {
    pub fn new(config: LocalConfig) -> Self {
        Self { config }
    }
}

impl Default for LocalProvider {
    fn default() -> Self {
        Self::new(LocalConfig::default())
    }
}

/// Converts `pcm_s16le` bytes (little-endian 16-bit PCM) into normalized `f32`
/// samples, the format transcribe-cpp consumes. Mono 16 kHz is assumed.
fn s16le_to_f32(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(2)
        .map(|pair| {
            let sample = i16::from_le_bytes([pair[0], pair[1]]);
            sample as f32 / 32768.0
        })
        .collect()
}

/// Resolves the GGUF file path for a catalog id and verifies it exists.
fn resolve_model_file(model_id: &str) -> Result<std::path::PathBuf, String> {
    let entry = model_store::find(model_id)
        .ok_or_else(|| format!("local provider: unknown model '{model_id}'"))?;
    if entry.files.is_empty() {
        return Err(format!("local provider: model '{model_id}' has no files"));
    }
    let dir = model_store::model_dir(model_id);
    let path = dir.join(&entry.files[0].name);
    if !path.exists() {
        return Err(format!(
            "local provider: model file '{}' is not downloaded",
            path.display()
        ));
    }
    Ok(path)
}

impl TranscriptionProvider for LocalProvider {
    fn stream(
        &self,
        mut chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
        mut on_event: Box<dyn FnMut(&ServerMessage) + Send>,
        paused: Arc<AtomicBool>,
    ) -> Result<SessionGuard, String> {
        let model_id = self.config.model_id.trim();
        if model_id.is_empty() {
            return Err("local provider: model_id is empty".to_string());
        }

        let model_path = resolve_model_file(model_id)?;
        let language = self.config.language.clone();

        // Load the model up front (before spawning the worker) so a missing or
        // corrupt file fails the `start` call synchronously rather than inside
        // the worker thread.
        let model = Model::load(&model_path).map_err(|e| {
            format!(
                "local provider: failed to load '{}': {e}",
                model_path.display()
            )
        })?;
        let mut session = model
            .session()
            .map_err(|e| format!("local provider: failed to open session: {e}"))?;

        // Build run/stream options once. `language` is an ISO hint or empty
        // (auto-detect).
        let mut run_options = RunOptions::default();
        if !language.is_empty() {
            run_options.language = Some(language.clone());
        }
        let stream_options = StreamOptions::default();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_for_worker = Arc::clone(&stop_flag);

        // `Session` is `Send`; move it into the worker and begin the `Stream`
        // there so the borrow stays scoped to the thread (the stream borrows
        // `session`, which must live for the whole worker lifetime).
        let handle = std::thread::spawn(move || {
            let mut stream = match session.stream(&run_options, &stream_options) {
                Ok(s) => s,
                Err(e) => {
                    on_event(&ServerMessage::Error {
                        message: format!("local provider: failed to begin stream: {e}"),
                    });
                    return;
                }
            };

            // The committed text already emitted; we send only the suffix that
            // grows beyond it each time the stream updates.
            let mut emitted: String = String::new();

            while !stop_flag_for_worker.load(Ordering::SeqCst) {
                let Some(chunk) = chunks() else { break };
                if paused.load(Ordering::SeqCst) {
                    continue;
                }

                let samples = s16le_to_f32(&chunk);
                if samples.is_empty() {
                    continue;
                }

                match stream.feed(&samples) {
                    Ok(update) => {
                        if update.committed_changed || update.tentative_changed {
                            emit_delta(&mut stream, &mut emitted, &mut on_event);
                        }
                    }
                    Err(e) => {
                        on_event(&ServerMessage::Error {
                            message: format!("local provider: stream feed failed: {e}"),
                        });
                        break;
                    }
                }
            }

            // Flush buffered audio and emit any remaining text.
            match stream.finalize() {
                Ok(_) => emit_delta(&mut stream, &mut emitted, &mut on_event),
                Err(e) => {
                    on_event(&ServerMessage::Error {
                        message: format!("local provider: stream finalize failed: {e}"),
                    });
                }
            }

            on_event(&ServerMessage::Done);
        });

        Ok(SessionGuard {
            stop: stop_flag,
            handle: Some(handle),
        })
    }
}

/// Emits a `Delta` for the text that has grown since the last emission.
///
/// We send the *committed* prefix (append-only, never revised) so the UI never
/// shows duplicated or "jumping" text. If the committed prefix hasn't grown,
/// the tentative suffix is emitted instead so partial hypotheses still surface
/// live; when the model later commits it, the committed delta is the difference.
fn emit_delta(
    stream: &mut transcribe_cpp::Stream<'_>,
    emitted: &mut String,
    on_event: &mut Box<dyn FnMut(&ServerMessage) + Send>,
) {
    let text = stream.text();
    // Prefer the append-only committed text; fall back to full display when the
    // committed prefix is empty (e.g. very early in an utterance).
    let target = if text.committed.is_empty() {
        text.tentative.clone()
    } else {
        text.committed.clone()
    };

    // Only send the suffix beyond what we've already emitted.
    if target.len() > emitted.len() && target.starts_with(emitted.as_str()) {
        let delta = target[emitted.len()..].to_string();
        if !delta.is_empty() {
            on_event(&ServerMessage::Delta { text: delta });
        }
        *emitted = target;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s16le_round_trips_through_i16() {
        let bytes = [0x00, 0x40, 0x00, 0xC0]; // 16384, -16384
        let out = s16le_to_f32(&bytes);
        assert_eq!(out.len(), 2);
        assert!((out[0] - 0.5).abs() < 1e-6);
        assert!((out[1] + 0.5).abs() < 1e-6);
    }

    #[test]
    fn s16le_zero_is_zero_f32() {
        let bytes = [0x00, 0x00];
        assert_eq!(s16le_to_f32(&bytes), vec![0.0]);
    }

    #[test]
    fn empty_model_id_is_rejected() {
        let provider = LocalProvider::new(LocalConfig::default());
        let result = provider.stream(
            Box::new(|| None::<Vec<u8>>),
            Box::new(|_| {}),
            Arc::new(AtomicBool::new(false)),
        );
        assert!(result.is_err());
    }

    #[test]
    fn unknown_model_is_reported() {
        let provider = LocalProvider::new(LocalConfig {
            model_id: "does-not-exist".to_string(),
            language: String::new(),
        });
        let result = provider.stream(
            Box::new(|| None::<Vec<u8>>),
            Box::new(|_| {}),
            Arc::new(AtomicBool::new(false)),
        );
        assert!(result.is_err());
    }
}
