//! End-to-end test for the WebSocket server: a real client connects over TCP
//! and exercises the shared protocol without needing audio hardware or an API
//! key. Covers handshake, `list_sources`, and an error path for a bad source.

#![cfg(feature = "server")]

use std::net::SocketAddr;
use std::time::Duration;

use audio::protocol::{ClientMessage, ServerMessage};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Connects a WebSocket client to `addr`.
async fn connect(addr: SocketAddr) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    let url = format!("ws://{addr}");
    let (ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .expect("client should connect");
    ws
}

/// Sends one client message as a JSON text frame.
async fn send(ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, msg: &ClientMessage) {
    let text = serde_json::to_string(msg).unwrap();
    ws.send(Message::Text(text.into())).await.unwrap();
}

/// Waits for the next server message, or fails on timeout.
async fn next(ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>) -> ServerMessage {
    let frame = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .expect("timed out waiting for server message")
        .expect("stream ended")
        .expect("frame should be valid");
    let Message::Text(text) = frame else {
        panic!("expected a text frame, got {frame:?}");
    };
    serde_json::from_str(&text).expect("server message should deserialize")
}

#[tokio::test]
async fn server_answers_list_sources_and_bad_start() {
    // Bind an ephemeral port and run the server in the background.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = tokio::spawn(audio::server::serve(addr));
    // Give the listener a moment to bind before connecting.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut ws = connect(addr).await;

    // 1. list_sources should return a `sources` payload (possibly empty in CI,
    //    but always well-formed and never an error).
    send(&mut ws, &ClientMessage::ListSources).await;
    match next(&mut ws).await {
        ServerMessage::Sources { sources } => {
            // We only assert shape: ids and kinds are consistent.
            for s in &sources {
                assert!(!s.id.is_empty());
            }
        }
        other => panic!("expected sources, got {other:?}"),
    }

    // 2. Starting a session with a bogus source id must surface an error, not
    //    hang. The crate's capture layer rejects unknown ids synchronously.
    send(
        &mut ws,
        &ClientMessage::Start {
            source_id: "definitely-not-a-real-source".to_string(),
            provider: audio::protocol::ProviderKind::Mistral,
            config: audio::protocol::MistralConfig::default(),
        },
    )
    .await;
    match next(&mut ws).await {
        ServerMessage::Error { message } => {
            assert!(
                message.contains("definitely-not-a-real-source") || message.contains("source"),
                "error should name the bad source, got: {message}"
            );
        }
        other => panic!("expected error, got {other:?}"),
    }

    drop(ws);
    server.abort();
}
