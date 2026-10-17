//! Cloud transcription via the Mistral realtime WebSocket API.
//!
//! Streams `pcm_s16le` chunks to `wss://api.mistral.ai/v1/audio/transcriptions/realtime`
//! and turns the server events into [`ServerMessage`]s.

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::protocol::{AudioChunk, ServerMessage};

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::{SessionGuard, TranscriptionProvider};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Connection parameters for the Mistral realtime endpoint.
#[derive(Debug, Clone)]
pub struct MistralConfig {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
    /// Sample rate the incoming PCM was captured at (must match capture).
    pub sample_rate: u32,
}

impl Default for MistralConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            base_url: "wss://api.mistral.ai".to_string(),
            model: "voxtral-mini-transcribe-realtime-2602".to_string(),
            sample_rate: 16000,
        }
    }
}

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
        mut chunks: Box<dyn FnMut() -> Option<AudioChunk> + Send>,
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
                match run(&cfg, &mut chunks, &mut on_event, &stop_flag_for_session).await {
                    Ok(()) => on_event(&ServerMessage::Done),
                    Err(e) => on_event(&ServerMessage::Error { message: e }),
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
    chunks: &mut (dyn FnMut() -> Option<AudioChunk> + Send),
    on_event: &mut (dyn FnMut(&ServerMessage) + Send),
    stop: &AtomicBool,
) -> Result<(), String> {
    let session = MistralSession::connect(cfg).await?;

    // Split the connection so audio writes and event reads proceed in
    // parallel (full-duplex). Without this, reading events would wait until
    // the audio loop finished — and the audio loop only finishes on stop,
    // so `delta`/`done` events would never surface.
    let mut writer = session.writer;
    let mut reader = session.reader;

    // Read events concurrently with audio sends. `join!` runs both futures
    // in the same async task, so `on_event` (a `&mut` borrow) is valid for
    // the whole scope and does not need `'static`.
    let event_fut = async {
        loop {
            match reader.next().await {
                Some(Ok(Message::Text(t))) => {
                    if let Some(ev) = parse_event(t.as_bytes()) {
                        on_event(&ev);
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    on_event(&ServerMessage::Error {
                        message: format!("WebSocket error: {e}"),
                    });
                    break;
                }
            }
        }
    };

    let audio_fut = async {
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
        Ok(()) as Result<(), String>
    };

    let (_, audio_result) = tokio::join!(event_fut, audio_fut);
    audio_result
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
