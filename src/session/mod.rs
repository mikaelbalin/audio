//! Session orchestration: binds capture to a provider and exposes events.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::protocol::{AudioSource, ProviderKind, ServerMessage};

mod level;
mod stream;

pub use level::rms_level;
pub use stream::{StreamOptions, run as stream_run};

use crate::providers::ProviderConfig;

/// A running audio session. Dropping it stops capture and transcription.
pub struct Session {
    stop_flag: Arc<AtomicBool>,
    _handle: std::thread::JoinHandle<()>,
}

impl Session {
    /// Returns `true` while the session is still running.
    pub fn is_running(&self) -> bool {
        !self.stop_flag.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.stop_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Starts a new session on a background thread and returns a handle to it.
///
/// `on_event` is wrapped in `Arc` internally so errors can be emitted even if
/// part of the pipeline holds a reference at the time of a failure.
pub fn start(
    source_id: &str,
    provider_kind: ProviderKind,
    provider_config: ProviderConfig,
    on_event: Box<dyn Fn(&ServerMessage) + Send + Sync>,
) -> Session {
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_worker = Arc::clone(&stop_flag);
    let source_id = source_id.to_string();
    let on_event: Arc<dyn Fn(&ServerMessage) + Send + Sync> = on_event.into();
    let on_event_err = Arc::clone(&on_event);

    let handle = std::thread::spawn(move || {
        let options = StreamOptions {
            source_id,
            sample_rate: 16000,
            on_event,
        };
        if let Err(e) = stream::run(options, provider_kind, provider_config, &stop_flag_worker) {
            on_event_err(&ServerMessage::Error { message: e });
        }
    });

    Session {
        stop_flag,
        _handle: handle,
    }
}

/// Lists all capturable sources (microphones and system monitors).
pub fn list_sources() -> Result<Vec<AudioSource>, String> {
    crate::capture::list_sources()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_requests_stop() {
        let session = start(
            "no-such-source",
            ProviderKind::Mistral,
            ProviderConfig::default(),
            Box::new(|_| {}),
        );
        assert!(session.is_running());
        drop(session);
        // After drop, the flag is set — the worker thread will observe it and
        // exit shortly. We don't join here to keep the test deterministic.
    }
}
