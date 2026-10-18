//! Local transcription provider (offline, via sherpa-onnx).
//!
//! Runs a Parakeet-style NeMo transducer model entirely in-process, so no
//! network or API key is needed. The model is identified by its catalog id (see
//! [`crate::model_store`]) and its on-disk directory is resolved from the shared
//! model store; it must contain the four sherpa-onnx files for
//! `parakeet-tdt-0.6b-v3`:
//!
//! ```text
//! encoder.int8.onnx
//! decoder.int8.onnx
//! joiner.int8.onnx
//! tokens.txt
//! ```
//!
//! This first version buffers audio and transcribes in one pass at stop time —
//! live streaming preview (VAD + incremental decoding) is a follow-up.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

/// The model files a local session expects inside `model_dir`.
const MODEL_FILES: [&str; 4] = [
    "encoder.int8.onnx",
    "decoder.int8.onnx",
    "joiner.int8.onnx",
    "tokens.txt",
];

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
        mut chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
        mut on_event: Box<dyn FnMut(&ServerMessage) + Send>,
        paused: Arc<AtomicBool>,
    ) -> Result<SessionGuard, String> {
        let model_id = self.config.model_id.trim();
        if model_id.is_empty() {
            return Err("local provider: model_id is empty".to_string());
        }

        // Resolve the on-disk directory from the shared model store.
        let model_dir = model_store::model_dir(model_id);

        // Build the sherpa-onnx recognizer up front so a bad path fails the
        // `start` command immediately instead of after the first chunk.
        let recognizer = build_recognizer(&model_dir)?;

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_for_worker = Arc::clone(&stop_flag);

        let handle = std::thread::spawn(move || {
            let mut buffer: Vec<f32> = Vec::new();

            // Pull chunks until the session is stopped or capture ends. While
            // muted we still drain chunks (so the capture pipe never fills up
            // and muted speech is never replayed), but we discard them instead
            // of buffering.
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

            match transcribe(&recognizer, &buffer) {
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
}

/// Builds an [`sherpa_onnx::OfflineRecognizer`] for the Parakeet TDT model in
/// `model_dir`. Returns a descriptive error if any file is missing.
fn build_recognizer(model_dir: &std::path::Path) -> Result<sherpa_onnx::OfflineRecognizer, String> {
    let dir = model_dir;

    let mut missing = Vec::new();
    for file in MODEL_FILES {
        if !dir.join(file).exists() {
            missing.push(file);
        }
    }
    if !missing.is_empty() {
        return Err(format!(
            "local provider: model dir '{}' is missing: {}",
            dir.display(),
            missing.join(", ")
        ));
    }

    let mut config = sherpa_onnx::OfflineRecognizerConfig::default();
    config.model_config.transducer = sherpa_onnx::OfflineTransducerModelConfig {
        encoder: Some(dir.join("encoder.int8.onnx").to_string_lossy().into_owned()),
        decoder: Some(dir.join("decoder.int8.onnx").to_string_lossy().into_owned()),
        joiner: Some(dir.join("joiner.int8.onnx").to_string_lossy().into_owned()),
    };
    config.model_config.tokens = Some(dir.join("tokens.txt").to_string_lossy().into_owned());
    config.model_config.model_type = Some("nemo_transducer".into());

    sherpa_onnx::OfflineRecognizer::create(&config).ok_or_else(|| {
        format!(
            "local provider: failed to load model from '{}'",
            dir.display()
        )
    })
}

/// Runs the recognizer over a buffered, normalized `f32` waveform.
fn transcribe(
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
        // A full-scale positive and a negative sample.
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
        match build_recognizer(dir) {
            Ok(_) => panic!("expected an error for a missing model directory"),
            Err(msg) => assert!(msg.contains("missing"), "unexpected error: {msg}"),
        }
    }
}
