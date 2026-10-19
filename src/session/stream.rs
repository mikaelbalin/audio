//! Internal stream loop: capture → provider → events.
//!
//! The capture blocks on `parec`'s stdout (~40 ms per chunk), which is fine:
//! we run the whole capture → transcribe → emit cycle synchronously on the
//! session's own thread, exactly like the pre-crate implementation did. There
//! are no intermediate channels or polling sleeps, so latency stays low.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::capture;
use crate::protocol::{AudioChunk, ServerMessage};
use crate::providers::{ProviderConfig, build};

/// Options for one transcription session.
#[allow(clippy::type_complexity)]
pub struct StreamOptions {
    pub source_id: String,
    pub sample_rate: u32,
    pub on_event: Arc<dyn Fn(&ServerMessage) + Send + Sync>,
    /// Optional live volume callback (0.0..=1.0), fired once per chunk.
    pub on_level: Option<Arc<dyn Fn(f64) + Send + Sync>>,
    /// Shared pause flag: when set, the provider stops sending audio but keeps
    /// the session alive for an instant resume.
    pub paused: Arc<AtomicBool>,
}

/// Runs the full capture → transcribe → emit cycle.
pub fn run(
    options: StreamOptions,
    provider_kind: crate::protocol::ProviderKind,
    provider_config: ProviderConfig,
    stop_flag: &Arc<AtomicBool>,
) -> Result<(), String> {
    // Start capture.
    let mut stream = capture::start(&options.source_id, options.sample_rate)?;

    // Build the provider.
    let provider = build(provider_kind, provider_config)?;

    // Pull chunks straight from the capture and hand them to the provider.
    // `next_chunk` blocks only ~40 ms, so checking `stop_flag` between chunks
    // is enough to honour a stop within one chunk's worth of audio.
    let on_level = options.on_level.clone();
    let stop_for_source = Arc::clone(stop_flag);
    let mut chunk_count: usize = 0;
    let chunk_source: Box<dyn FnMut() -> Option<AudioChunk> + Send> = Box::new(move || {
        if stop_for_source.load(Ordering::SeqCst) {
            return None;
        }
        let chunk = stream.next_chunk()?;
        chunk_count += 1;
        if chunk_count == 1 || chunk_count.is_multiple_of(200) {
            eprintln!(
                "[audio:stream] captured chunk #{chunk_count} len={}",
                chunk.len()
            );
        }
        if let Some(level) = &on_level {
            level(super::level::rms_level(&chunk));
        }
        Some(chunk)
    });

    let on_event = Arc::clone(&options.on_event);
    let stop_for_events = Arc::clone(stop_flag);
    let session = provider.stream(
        chunk_source,
        Box::new(move |event| {
            // Only log lifecycle events; `Delta` fires hundreds of times per
            // second and would flood the log.
            match event {
                ServerMessage::Ready => eprintln!("[audio:stream] provider ready"),
                ServerMessage::Done => eprintln!("[audio:stream] provider done"),
                ServerMessage::Error { message } => {
                    eprintln!("[audio:stream] provider error: {message}")
                }
                ServerMessage::Stopped
                | ServerMessage::Delta { .. }
                | ServerMessage::Sources { .. }
                | ServerMessage::Models { .. }
                | ServerMessage::ModelProgress { .. }
                | ServerMessage::ModelDone { .. }
                | ServerMessage::ModelError { .. } => {}
            }
            if matches!(event, ServerMessage::Stopped) {
                stop_for_events.store(true, Ordering::SeqCst);
            }
            on_event(event);
        }),
        options.paused.clone(),
    )?;

    // Tell the UI we are live.
    (options.on_event)(&ServerMessage::Ready);
    eprintln!("[audio:stream] Ready (source={})", options.source_id);

    // Keep the session alive until a stop is requested. The provider runs on
    // its own thread; holding the guard prevents premature shutdown.
    while !stop_flag.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    session.finish();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_options_is_constructible() {
        let opts = StreamOptions {
            source_id: "noop".to_string(),
            sample_rate: 16000,
            on_event: Arc::new(|_| {}),
            on_level: None,
            paused: Arc::new(AtomicBool::new(false)),
        };
        assert_eq!(opts.sample_rate, 16000);
    }
}
