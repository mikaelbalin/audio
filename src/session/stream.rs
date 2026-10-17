//! Internal stream loop: capture → provider → events.
//!
//! The capture itself blocks on `parec`'s stdout. To make `stop` effective
//! quickly, the raw capture runs on its own thread and forwards chunks down a
//! channel; the provider-side loop polls that channel with a short timeout,
//! so a stop request is honoured within ~50 ms even mid-read.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};

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
}

/// Runs the full capture → transcribe → emit cycle.
pub fn run(
    options: StreamOptions,
    provider_kind: crate::protocol::ProviderKind,
    provider_config: ProviderConfig,
    stop_flag: &Arc<AtomicBool>,
) -> Result<(), String> {
    // 1. Raw capture on its own thread; sends chunks into a channel.
    let (tx, rx) = mpsc::channel::<Option<AudioChunk>>();
    {
        let mut stream = capture::start(&options.source_id, options.sample_rate)?;
        std::thread::spawn(move || {
            loop {
                match stream.next_chunk() {
                    Some(c) => {
                        if tx.send(Some(c)).is_err() {
                            break; // receiver dropped
                        }
                    }
                    None => {
                        let _ = tx.send(None);
                        break;
                    }
                }
            }
        });
    }

    // 2. Provider-side pull source polls the channel with a short timeout so
    //    a stop signal is honoured without waiting for the next capture read.
    let on_level = options.on_level.clone();
    let chunk_source: Box<dyn FnMut() -> Option<AudioChunk> + Send> = {
        let stop_flag = Arc::clone(stop_flag);
        Box::new(move || {
            loop {
                if stop_flag.load(Ordering::SeqCst) {
                    return None;
                }
                match rx.recv_timeout(std::time::Duration::from_millis(50)) {
                    Ok(Some(c)) => {
                        if let Some(level) = &on_level {
                            level(super::level::rms_level(&c));
                        }
                        return Some(c);
                    }
                    Ok(None) => return None,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => return None,
                }
            }
        })
    };

    // 3. Provider gets chunks and pushes events to the UI.
    let provider = build(provider_kind, provider_config)?;
    let on_event = Arc::clone(&options.on_event);
    let stop_for_events = Arc::clone(stop_flag);
    let session = provider.stream(
        chunk_source,
        Box::new(move |event| {
            if matches!(event, ServerMessage::Stopped) {
                stop_for_events.store(true, Ordering::SeqCst);
            }
            on_event(event);
        }),
    )?;

    // 4. Tell the UI we are live.
    (options.on_event)(&ServerMessage::Ready);

    // 5. Keep the session alive until a stop is requested. Holding `session`
    //    (a SessionGuard with its worker JoinHandle) prevents the provider
    //    thread from being dropped/joined prematurely.
    while !stop_flag.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    // 6. Graceful stop: request the provider to finish and join its thread.
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
        };
        assert_eq!(opts.sample_rate, 16000);
    }
}
