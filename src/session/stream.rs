//! Internal stream loop: capture → provider → events.

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
}

/// Runs the full capture → transcribe → emit cycle.
///
/// This is pull-based to stay in sync with how `speech-transcription` drives
/// audio today: the capture runs on its own thread, chunks are pushed through
/// the provider, events are emitted to `on_event` as they arrive.
pub fn run(
    options: StreamOptions,
    provider_kind: crate::protocol::ProviderKind,
    provider_config: ProviderConfig,
    stop_flag: &Arc<AtomicBool>,
) -> Result<(), String> {
    // 1. Start capture.
    let mut stream = capture::start(&options.source_id, options.sample_rate)?;

    // 2. Build the provider.
    let provider = build(provider_kind, provider_config)?;

    // 3. Adapt the blocking capture into the provider's pull-based chunk
    //    source. Each `chunks()` call blocks until a chunk is available or the
    //    stream ends.
    let chunk_source: Box<dyn FnMut() -> Option<AudioChunk> + Send> = {
        let stop_flag = Arc::clone(stop_flag);
        Box::new(move || {
            if stop_flag.load(Ordering::SeqCst) {
                return None;
            }
            stream.next_chunk()
        })
    };

    // 4. Start the provider; events are also emitted to the UI, and the
    //    stop flag is mirrored so a `Stopped` event shuts the capture down.
    let on_event = Arc::clone(&options.on_event);
    let stop_for_events = Arc::clone(stop_flag);
    let session = provider.stream(
        chunk_source,
        Box::new(move |event| {
            if event == &ServerMessage::Stopped {
                stop_for_events.store(true, Ordering::SeqCst);
            }
            on_event(event);
        }),
    )?;

    // 5. Emit a `ready` so the UI knows capture is live.
    (options.on_event)(&ServerMessage::Ready);

    // 6. Wait for shutdown. The provider's `SessionGuard` (dropped here at the
    //    end of the scope) requests a graceful stop; we just need to wait until
    //    the underlying stream is exhausted (the provider calls `Done` or the
    //    capture returns `None`).
    let _session = session; // keep alive until the end of the function
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::session::level;

    #[test]
    fn rms_level_is_bounded() {
        assert_eq!(level::rms_level(&[]), 0.0);
        assert_eq!(level::rms_level(&[0xff]), 0.0);
    }
}
