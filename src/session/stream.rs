//! Internal stream loop: capture → provider → events.
//!
//! The capture blocks on `parec`'s stdout (~40 ms per chunk), which is fine:
//! we run the whole capture → transcribe → emit cycle synchronously on the
//! session's own thread, exactly like the pre-crate implementation did. There
//! are no intermediate channels or polling sleeps, so latency stays low.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;

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
    /// Optional external chunk source (e.g. browser-captured audio pushed over
    /// a WebSocket). When `Some`, capture is bypassed and chunks are pulled
    /// from this receiver instead of a local `parec` source.
    pub external_chunks: Option<Receiver<AudioChunk>>,
}

/// Runs the full capture → transcribe → emit cycle.
pub fn run(
    options: StreamOptions,
    provider_kind: crate::protocol::ProviderKind,
    provider_config: ProviderConfig,
    stop_flag: &Arc<AtomicBool>,
) -> Result<(), String> {
    // Build the provider.
    let provider = build(provider_kind, provider_config)?;

    let on_level = options.on_level.clone();
    let stop_for_source = Arc::clone(stop_flag);

    // The chunk source is either the local capture (`parec`) or an external
    // stream of chunks pushed by the client (browser Web Audio capture).
    let chunk_source: Box<dyn FnMut() -> Option<AudioChunk> + Send> = match options.external_chunks
    {
        Some(rx) => {
            let rx = rx;
            Box::new(move || {
                if stop_for_source.load(Ordering::SeqCst) {
                    return None;
                }
                // Block until a chunk arrives or the sender is dropped.
                match rx.recv() {
                    Ok(chunk) => {
                        if let Some(level) = &on_level {
                            level(super::level::rms_level(&chunk));
                        }
                        Some(chunk)
                    }
                    Err(_) => None,
                }
            })
        }
        None => {
            // Start capture locally.
            let mut stream = capture::start(&options.source_id, options.sample_rate)?;
            Box::new(move || {
                if stop_for_source.load(Ordering::SeqCst) {
                    return None;
                }
                let chunk = stream.next_chunk()?;
                if let Some(level) = &on_level {
                    level(super::level::rms_level(&chunk));
                }
                Some(chunk)
            })
        }
    };

    let on_event = Arc::clone(&options.on_event);
    let stop_for_events = Arc::clone(stop_flag);
    let session = provider.stream(
        chunk_source,
        Box::new(move |event| {
            match event {
                ServerMessage::Error { message } => {
                    eprintln!("[audio:stream] provider error: {message}")
                }
                ServerMessage::Ready
                | ServerMessage::Done
                | ServerMessage::Stopped
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
            external_chunks: None,
        };
        assert_eq!(opts.sample_rate, 16000);
    }
}
