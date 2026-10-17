//! Handling of a single WebSocket connection.
//!
//! One connection owns at most one running session. Incoming text frames are
//! parsed as [`ClientMessage`]; session events are serialized as
//! [`ServerMessage`] and pushed back over the same socket. `session::start`
//! runs the whole pipeline on its own thread, so this async handler only
//! forwards already-computed events via a channel.

use std::sync::Arc;
use std::sync::Mutex;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::protocol::{ClientMessage, ServerMessage};
use crate::providers::ProviderConfig;
use crate::session::{self, Session};

/// Capacity of the per-connection outbound event channel. Events are small
/// text frames; a few dozen of headroom absorbs a fast delta burst without
/// unbounded growth if the socket is momentarily slow.
const EVENT_CHANNEL: usize = 64;

/// Drives one WebSocket connection until it closes or errors.
pub async fn handle(tcp: TcpStream) -> Result<(), String> {
    let ws = tokio_tungstenite::accept_async(tcp)
        .await
        .map_err(|e| format!("handshake failed: {e}"))?;
    let (mut sink, mut stream) = ws.split();

    // Session events cross from the session's blocking thread into this async
    // task through a channel, so the single writer owns the socket.
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(EVENT_CHANNEL);

    // The current session, shared so the read loop can stop/start it.
    let session: Arc<Mutex<Option<Session>>> = Arc::new(Mutex::new(None));

    // Writer task: drain the channel and send text frames.
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let text = super::encode(&msg);
            if sink.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
    });

    // Read loop: interpret client commands.
    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(|e| format!("read failed: {e}"))?;
        let Message::Text(text) = frame else {
            continue; // ignore binary/ping/pong
        };
        match super::decode(&text) {
            Ok(cmd) => handle_command(cmd, &session, &tx),
            Err(e) => send(&tx, ServerMessage::Error { message: e }),
        }
    }

    // Connection closed: stop any running session and the writer.
    session.lock().unwrap().take();
    drop(tx);
    let _ = writer.await;
    Ok(())
}

/// Applies one parsed command against the connection's session slot.
fn handle_command(
    cmd: ClientMessage,
    session: &Arc<Mutex<Option<Session>>>,
    tx: &mpsc::Sender<ServerMessage>,
) {
    match cmd {
        ClientMessage::ListSources => match session::list_sources() {
            Ok(sources) => send(tx, ServerMessage::Sources { sources }),
            Err(e) => send(tx, ServerMessage::Error { message: e }),
        },
        ClientMessage::Start {
            source_id,
            provider,
            config,
        } => start_session(session, tx, source_id, provider, config),
        ClientMessage::Stop => stop_session(session, tx),
    }
}

/// Starts a session, replacing any existing one for this connection.
fn start_session(
    session: &Arc<Mutex<Option<Session>>>,
    tx: &mpsc::Sender<ServerMessage>,
    source_id: String,
    provider: crate::protocol::ProviderKind,
    config: crate::protocol::MistralConfig,
) {
    // Stop a previous session on this connection, if any.
    session.lock().unwrap().take();

    let tx_events = tx.clone();
    let on_event = Box::new(move |msg: &ServerMessage| {
        let _ = tx_events.try_send(msg.clone());
    });

    let cfg = ProviderConfig { mistral: config };
    let new_session = session::start(&source_id, provider, cfg, on_event, None);
    *session.lock().unwrap() = Some(new_session);
}

/// Stops the current session, if any, and reports it.
fn stop_session(session: &Arc<Mutex<Option<Session>>>, tx: &mpsc::Sender<ServerMessage>) {
    if session.lock().unwrap().take().is_some() {
        send(tx, ServerMessage::Stopped);
    }
}

/// Best-effort enqueue of an outbound event.
fn send(tx: &mpsc::Sender<ServerMessage>, msg: ServerMessage) {
    let _ = tx.try_send(msg);
}
