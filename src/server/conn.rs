//! Handling of a single WebSocket connection.
//!
//! One connection owns at most one running session. Incoming text frames are
//! parsed as [`ClientMessage`]; session events are serialized as
//! [`ServerMessage`] and pushed back over the same socket. `session::start`
//! runs the whole pipeline on its own thread, so this async handler only
//! forwards already-computed events via a channel.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc as tmpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::protocol::{AudioChunk, BROWSER_SOURCE_ID, ClientMessage, ServerMessage};
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
    let (tx, mut rx) = tmpsc::channel::<ServerMessage>(EVENT_CHANNEL);

    // The current session, shared so the read loop can stop/start it.
    let session: Arc<Mutex<Option<Session>>> = Arc::new(Mutex::new(None));

    // Sender for browser-captured audio chunks. `None` until a browser-source
    // session starts; binary frames are pushed through it to the session's
    // blocking worker via a sync mpsc channel.
    let chunk_tx: Arc<Mutex<Option<mpsc::Sender<AudioChunk>>>> = Arc::new(Mutex::new(None));

    // Writer task: drain the channel and send text frames.
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let text = super::encode(&msg);
            if sink.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
    });

    // Read loop: interpret client commands and browser audio frames.
    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(|e| format!("read failed: {e}"))?;
        match frame {
            Message::Text(text) => match super::decode(&text) {
                Ok(cmd) => handle_command(cmd, &session, &tx, &chunk_tx),
                Err(e) => send(&tx, ServerMessage::Error { message: e }),
            },
            // Binary frames carry raw PCM chunks from the browser (Web Audio
            // capture). Forward them to the active browser-source session.
            Message::Binary(bytes) => {
                if let Some(sender) = chunk_tx.lock().unwrap().as_ref() {
                    let _ = sender.send(bytes.to_vec());
                }
            }
            _ => {} // ignore ping/pong/close
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
    tx: &tmpsc::Sender<ServerMessage>,
    chunk_tx: &Arc<Mutex<Option<mpsc::Sender<AudioChunk>>>>,
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
            local,
        } => start_session(session, tx, chunk_tx, source_id, provider, config, local),
        ClientMessage::Stop => stop_session(session, tx),
        ClientMessage::Pause => set_paused(session, true),
        ClientMessage::Resume => set_paused(session, false),
        ClientMessage::ListModels => list_models(tx),
        ClientMessage::DownloadModel { id } => download_model(tx, id),
        ClientMessage::DeleteModel { id } => delete_model(tx, id),
    }
}

/// Mutes/unmutes the active session, if one is running.
fn set_paused(session: &Arc<Mutex<Option<Session>>>, paused: bool) {
    if let Some(s) = session.lock().unwrap().as_ref() {
        s.set_paused(paused);
    }
}

/// Starts a session, replacing any existing one for this connection.
fn start_session(
    session: &Arc<Mutex<Option<Session>>>,
    tx: &tmpsc::Sender<ServerMessage>,
    chunk_tx: &Arc<Mutex<Option<mpsc::Sender<AudioChunk>>>>,
    source_id: String,
    provider: crate::protocol::ProviderKind,
    config: crate::protocol::MistralConfig,
    local: crate::protocol::LocalConfig,
) {
    // Stop a previous session on this connection, if any, and drop any stale
    // browser chunk sender so the previous session's worker unblocks.
    session.lock().unwrap().take();
    *chunk_tx.lock().unwrap() = None;

    let tx_events = tx.clone();
    let on_event = Box::new(move |msg: &ServerMessage| {
        let _ = tx_events.try_send(msg.clone());
    });

    let cfg = ProviderConfig {
        mistral: config,
        local,
    };

    // Browser-captured audio: bypass local capture and wire binary frames into
    // the session via a sync mpsc channel.
    let external_chunks = if source_id == BROWSER_SOURCE_ID {
        let (sender, receiver) = mpsc::channel::<AudioChunk>();
        *chunk_tx.lock().unwrap() = Some(sender);
        Some(receiver)
    } else {
        None
    };

    let new_session = session::start(&source_id, provider, cfg, on_event, None, external_chunks);
    *session.lock().unwrap() = Some(new_session);
}

/// Stops the current session, if any, and reports it.
fn stop_session(session: &Arc<Mutex<Option<Session>>>, tx: &tmpsc::Sender<ServerMessage>) {
    if session.lock().unwrap().take().is_some() {
        send(tx, ServerMessage::Stopped);
    }
}

/// Sends the full catalog with download status.
fn list_models(tx: &tmpsc::Sender<ServerMessage>) {
    let models = crate::model_store::all_statuses();
    send(tx, ServerMessage::Models { models });
}

/// Starts downloading a model on a blocking thread; streams progress events,
/// then `ModelDone`/`ModelError`, and finally a fresh catalog.
fn download_model(tx: &tmpsc::Sender<ServerMessage>, id: String) {
    let entry = match crate::model_store::find(&id) {
        Some(e) => e,
        None => {
            send(
                tx,
                ServerMessage::ModelError {
                    id,
                    message: "unknown model".to_string(),
                },
            );
            return;
        }
    };
    let model_id = entry.id.clone();
    let tx_progress = tx.clone();

    // Run the blocking download off the async executor's worker threads.
    tokio::task::spawn_blocking(move || {
        let tx_progress_inner = tx_progress.clone();
        let progress_id = model_id.clone();
        let result = crate::model_store::download_model(&entry, move |p| {
            let _ = tx_progress_inner.try_send(ServerMessage::ModelProgress {
                id: progress_id.clone(),
                received: p.received,
                total: p.total,
            });
        });
        match result {
            Ok(()) => {
                let _ = tx_progress.try_send(ServerMessage::ModelDone { id: model_id });
            }
            Err(message) => {
                let _ = tx_progress.try_send(ServerMessage::ModelError {
                    id: model_id,
                    message,
                });
            }
        }
        // Refresh the catalog for any listeners.
        let models = crate::model_store::all_statuses();
        let _ = tx_progress.try_send(ServerMessage::Models { models });
    });
}

/// Deletes a model and reports the fresh catalog (or an error).
fn delete_model(tx: &tmpsc::Sender<ServerMessage>, id: String) {
    if let Err(message) = crate::model_store::delete_model(&id) {
        send(tx, ServerMessage::ModelError { id, message });
        return;
    }
    let models = crate::model_store::all_statuses();
    send(tx, ServerMessage::Models { models });
}

/// Best-effort enqueue of an outbound event.
fn send(tx: &tmpsc::Sender<ServerMessage>, msg: ServerMessage) {
    let _ = tx.try_send(msg);
}
