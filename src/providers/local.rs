//! Local transcription provider (offline and streaming, via sherpa-onnx).
//!
//! Runs NeMo transducer models entirely in-process, so no network or API key is
//! needed. Two modes are supported, selected by the catalog id in
//! [`LocalConfig::model_id`]:
//!
//! - **Offline** (Parakeet TDT v3): buffers audio and transcribes in one pass at
//!   stop time. Text appears only after the session stops.
//! - **Streaming** (Nemotron 3.5 ASR): feeds audio chunk-by-chunk to an
//!   [`OnlineRecognizer`] and emits `Delta` events continuously as text is
//!   produced, so the transcript updates live during speech.
//!
//! Both modes read `pcm_s16le` chunks, convert them to normalized `f32`, and
//! honour the shared pause flag (draining but not sending while muted).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::model_store;
use crate::protocol::{AudioChunk, LocalConfig, ServerMessage};

use super::{SessionGuard, TranscriptionProvider};

/// Catalog id of the streaming (Nemotron) model.
const STREAMING_MODEL_ID: &str = "nemotron-3.5-asr-streaming";

/// The model files a local session expects inside `model_dir`.
const MODEL_FILES: [&str; 4] = [
    "encoder.int8.onnx",
    "decoder.int8.onnx",
    "joiner.int8.onnx",
    "tokens.txt",
];

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
/// samples, the format sherpa-onnx consumes. Mono 16 kHz is assumed.
fn s16le_to_f32(bytes: &[u8]) -> Vec<f32> {
    bytes
        .chunks_exact(2)
        .map(|pair| {
            let sample = i16::from_le_bytes([pair[0], pair[1]]);
            sample as f32 / 32768.0
        })
        .collect()
}

impl TranscriptionProvider for LocalProvider {
    fn stream(
        &self,
        chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
        on_event: Box<dyn FnMut(&ServerMessage) + Send>,
        paused: Arc<AtomicBool>,
    ) -> Result<SessionGuard, String> {
        let model_id = self.config.model_id.trim();
        if model_id.is_empty() {
            return Err("local provider: model_id is empty".to_string());
        }

        if model_id == STREAMING_MODEL_ID {
            self.stream_streaming(model_id, chunks, on_event, paused)
        } else {
            self.stream_offline(model_id, chunks, on_event, paused)
        }
    }
}

impl LocalProvider {
    /// Offline path: buffer everything, transcribe once at stop time.
    fn stream_offline(
        &self,
        model_id: &str,
        mut chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
        mut on_event: Box<dyn FnMut(&ServerMessage) + Send>,
        paused: Arc<AtomicBool>,
    ) -> Result<SessionGuard, String> {
        let model_dir = model_store::model_dir(model_id);
        let recognizer = build_offline_recognizer(&model_dir)?;

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_for_worker = Arc::clone(&stop_flag);

        let handle = std::thread::spawn(move || {
            let mut buffer: Vec<f32> = Vec::new();

            while !stop_flag_for_worker.load(Ordering::SeqCst) {
                let Some(chunk) = chunks() else { break };
                if paused.load(Ordering::SeqCst) {
                    continue;
                }
                buffer.extend_from_slice(&s16le_to_f32(&chunk));
            }

            if buffer.is_empty() {
                on_event(&ServerMessage::Done);
                return;
            }

            match transcribe_offline(&recognizer, &buffer) {
                Ok(text) => {
                    let trimmed = text.trim();
                    if !trimmed.is_empty() {
                        on_event(&ServerMessage::Delta {
                            text: trimmed.to_string(),
                        });
                    }
                    on_event(&ServerMessage::Done);
                }
                Err(e) => {
                    on_event(&ServerMessage::Error { message: e });
                }
            }
        });

        Ok(SessionGuard {
            stop: stop_flag,
            handle: Some(handle),
        })
    }

    /// Streaming path: feed chunks to an `OnlineRecognizer`, emitting `Delta`
    /// events as the recognizer produces text.
    fn stream_streaming(
        &self,
        model_id: &str,
        mut chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
        mut on_event: Box<dyn FnMut(&ServerMessage) + Send>,
        paused: Arc<AtomicBool>,
    ) -> Result<SessionGuard, String> {
        let model_dir = model_store::model_dir(model_id);
        let recognizer = build_streaming_recognizer(&model_dir)?;

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_for_worker = Arc::clone(&stop_flag);

        let handle = std::thread::spawn(move || {
            let stream = recognizer.create_stream();
            // The text already emitted for the current utterance. `get_result`
            // returns the full hypothesis each time, so we send only the suffix
            // that hasn't been emitted yet to avoid duplicating the transcript.
            let mut emitted: String = String::new();

            while !stop_flag_for_worker.load(Ordering::SeqCst) {
                let Some(chunk) = chunks() else { break };
                if paused.load(Ordering::SeqCst) {
                    continue;
                }

                let samples = s16le_to_f32(&chunk);
                stream.accept_waveform(16000, &samples);

                while recognizer.is_ready(&stream) {
                    recognizer.decode(&stream);

                    if let Some(result) = recognizer.get_result(&stream) {
                        let text = result.text;
                        if !text.is_empty() {
                            let delta = if text.starts_with(&emitted) {
                                // New text extends the previous hypothesis: emit
                                // only the suffix.
                                text[emitted.len()..].to_string()
                            } else {
                                // Hypothesis was revised or reset: emit the whole
                                // thing (best-effort; rare with greedy search).
                                text.clone()
                            };
                            if !delta.is_empty() {
                                on_event(&ServerMessage::Delta { text: delta });
                            }
                            emitted = text;
                        }
                    }

                    if recognizer.is_endpoint(&stream) {
                        recognizer.reset(&stream);
                        emitted.clear();
                    }
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

/// Builds an [`sherpa_onnx::OfflineRecognizer`] for the Parakeet TDT model.
fn build_offline_recognizer(
    model_dir: &std::path::Path,
) -> Result<sherpa_onnx::OfflineRecognizer, String> {
    ensure_model_files(model_dir)?;

    let mut config = sherpa_onnx::OfflineRecognizerConfig::default();
    config.model_config.transducer = sherpa_onnx::OfflineTransducerModelConfig {
        encoder: Some(
            model_dir
                .join("encoder.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
        decoder: Some(
            model_dir
                .join("decoder.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
        joiner: Some(
            model_dir
                .join("joiner.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
    };
    config.model_config.tokens = Some(model_dir.join("tokens.txt").to_string_lossy().into_owned());
    config.model_config.model_type = Some("nemo_transducer".into());

    sherpa_onnx::OfflineRecognizer::create(&config).ok_or_else(|| {
        format!(
            "local provider: failed to load model from '{}'",
            model_dir.display()
        )
    })
}

/// Builds an [`sherpa_onnx::OnlineRecognizer`] for the Nemotron streaming model.
fn build_streaming_recognizer(
    model_dir: &std::path::Path,
) -> Result<sherpa_onnx::OnlineRecognizer, String> {
    ensure_model_files(model_dir)?;

    let mut config = sherpa_onnx::OnlineRecognizerConfig::default();
    config.model_config.transducer = sherpa_onnx::OnlineTransducerModelConfig {
        encoder: Some(
            model_dir
                .join("encoder.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
        decoder: Some(
            model_dir
                .join("decoder.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
        joiner: Some(
            model_dir
                .join("joiner.int8.onnx")
                .to_string_lossy()
                .into_owned(),
        ),
    };
    config.model_config.tokens = Some(model_dir.join("tokens.txt").to_string_lossy().into_owned());
    config.model_config.model_type = Some("nemotron".into());
    config.enable_endpoint = true;
    config.decoding_method = Some("greedy_search".into());

    sherpa_onnx::OnlineRecognizer::create(&config).ok_or_else(|| {
        format!(
            "local provider: failed to load streaming model from '{}'",
            model_dir.display()
        )
    })
}

/// Verifies all four model files exist, returning a descriptive error otherwise.
fn ensure_model_files(model_dir: &std::path::Path) -> Result<(), String> {
    let mut missing = Vec::new();
    for file in MODEL_FILES {
        if !model_dir.join(file).exists() {
            missing.push(file);
        }
    }
    if !missing.is_empty() {
        return Err(format!(
            "local provider: model dir '{}' is missing: {}",
            model_dir.display(),
            missing.join(", ")
        ));
    }
    Ok(())
}

/// Runs the offline recognizer over a buffered, normalized `f32` waveform.
fn transcribe_offline(
    recognizer: &sherpa_onnx::OfflineRecognizer,
    samples: &[f32],
) -> Result<String, String> {
    let stream = recognizer.create_stream();
    stream.accept_waveform(16000, samples);
    recognizer.decode(&stream);
    stream
        .get_result()
        .map(|r| r.text)
        .ok_or_else(|| "local provider: transcription failed".to_string())
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
    fn missing_model_files_are_reported() {
        let dir = std::path::Path::new("/definitely/not/a/real/model/dir");
        assert!(ensure_model_files(dir).is_err());
    }
}
