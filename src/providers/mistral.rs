//! Cloud transcription via the Mistral realtime WebSocket API.
//!
//! Streams `pcm_s16le` chunks to `wss://api.mistral.ai/v1/audio/transcriptions/realtime`
//! and turns the server events into [`ServerMessage`]s.

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::protocol::{AudioChunk, MistralConfig, ServerMessage};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{SessionGuard, TranscriptionProvider};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Installs `ring` as the process-level CryptoProvider for rustls.
///
/// Rustls (used by tokio-tungstenite for wss) needs one crypto backend chosen
/// at process level *before* any TLS handshake. Idempotent: subsequent calls
/// return `Ok` without re-installing.
fn ensure_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// A connected realtime session: an audio writer + an event reader.
struct MistralSession {
    writer: futures_util::stream::SplitSink<Ws, Message>,
    reader: futures_util::stream::SplitStream<Ws>,
}

impl MistralSession {
    async fn connect(cfg: &MistralConfig) -> Result<Self, String> {
        ensure_crypto_provider();

        let url = format!(
            "{base}/v1/audio/transcriptions/realtime?model={model}",
            base = cfg.base_url,
            model = cfg.model,
        );
        let mut request = url
            .into_client_request()
            .map_err(|e| format!("Invalid URL: {e}"))?;
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", cfg.api_key)
                .parse()
                .map_err(|e| format!("Invalid auth header: {e}"))?,
        );

        let (ws, _) = connect_async(request)
            .await
            .map_err(|e| format!("Failed to connect: {e}"))?;

        let (writer, reader) = ws.split();
        Ok(Self { writer, reader })
    }
}

/// Cloud provider.
pub struct MistralProvider {
    cfg: MistralConfig,
}

impl MistralProvider {
    pub fn new(cfg: MistralConfig) -> Self {
        Self { cfg }
    }
}

impl TranscriptionProvider for MistralProvider {
    fn stream(
        &self,
        chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
        mut on_event: Box<dyn FnMut(&ServerMessage) + Send>,
    ) -> Result<SessionGuard, String> {
        if self.cfg.api_key.is_empty() {
            return Err("Mistral API key is empty".to_string());
        }

        let cfg = self.cfg.clone();
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_for_session = Arc::clone(&stop_flag);

        let handle = std::thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    on_event(&ServerMessage::Error {
                        message: format!("tokio runtime: {e}"),
                    });
                    return;
                }
            };

            rt.block_on(async move {
                match run(&cfg, chunks, on_event, &stop_flag_for_session).await {
                    Ok(()) => {}
                    Err(_e) => {}
                }
            });
        });

        Ok(SessionGuard {
            stop: stop_flag,
            handle: Some(handle),
        })
    }
}

async fn run(
    cfg: &MistralConfig,
    chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
    on_event: Box<dyn FnMut(&ServerMessage) + Send>,
    stop: &AtomicBool,
) -> Result<(), String> {
    let session = MistralSession::connect(cfg).await?;

    let mut writer = session.writer;
    let mut reader = session.reader;

    // The event callback must be shared between the spawned reader task and
    // the final `Done`/`Error` emission, so it lives in an Arc<Mutex>.
    let on_event = Arc::new(std::sync::Mutex::new(on_event));
    let on_event_reader = Arc::clone(&on_event);

    // Reader runs on its OWN tokio task so it doesn't block audio sends.
    let reader_task = tokio::spawn(async move {
        loop {
            match reader.next().await {
                Some(Ok(Message::Text(t))) => {
                    if let Some(ev) = parse_event(t.as_bytes()) {
                        on_event_reader.lock().unwrap()(&ev);
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    on_event_reader.lock().unwrap()(&ServerMessage::Error {
                        message: format!("WebSocket error: {e}"),
                    });
                    break;
                }
            }
        }
    });

    // Audio loop: send chunks synchronously in this task.
    let mut chunks = chunks;
    while !stop.load(Ordering::SeqCst) {
        let Some(chunk) = chunks() else { break };
        writer
            .send(Message::Binary(chunk.to_vec()))
            .await
            .map_err(|e| format!("Failed to send audio: {e}"))?;
    }
    writer
        .close()
        .await
        .map_err(|e| format!("Failed to close: {e}"))?;

    // Wait for the reader task to drain remaining events.
    let _ = reader_task.await;

    // Emit the final Done after the stream has fully closed.
    on_event.lock().unwrap()(&ServerMessage::Done);
    Ok(())
}

/// Parses one JSON event from the realtime stream into a [`ServerMessage`].
fn parse_event(bytes: &[u8]) -> Option<ServerMessage> {
    let text = std::str::from_utf8(bytes).ok()?;
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    let ty = v.get("type")?.as_str()?;

    match ty {
        "transcription.text.delta" => {
            v.get("text")
                .and_then(|t| t.as_str())
                .map(|t| ServerMessage::Delta {
                    text: t.to_string(),
                })
        }
        "transcription.done" => Some(ServerMessage::Done),
        "error" => {
            let message = v
                .pointer("/error/message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error")
                .to_string();
            Some(ServerMessage::Error { message })
        }
        // session.created and anything else is informational
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_delta() {
        let ev = parse_event(br#"{"type":"transcription.text.delta","text":"hi "}"#);
        assert_eq!(ev, Some(ServerMessage::Delta { text: "hi ".into() }));
    }

    #[test]
    fn parse_done_and_error() {
        assert_eq!(
            parse_event(br#"{"type":"transcription.done"}"#),
            Some(ServerMessage::Done)
        );
        assert_eq!(
            parse_event(br#"{"type":"error","error":{"message":"auth"}}"#),
            Some(ServerMessage::Error {
                message: "auth".into()
            })
        );
    }

    #[test]
    fn unknown_events_are_ignored() {
        assert_eq!(parse_event(br#"{"type":"session.created"}"#), None);
        assert_eq!(parse_event(b"not json"), None);
    }

    #[test]
    fn empty_api_key_is_rejected_early() {
        let provider = MistralProvider::new(MistralConfig::default());
        let stream = provider.stream(Box::new(|| None::<Vec<u8>>), Box::new(|_| {}));
        assert!(stream.is_err());
    }
}
