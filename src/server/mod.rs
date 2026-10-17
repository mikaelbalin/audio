//! WebSocket server exposing the `audio` crate over the wire.
//!
//! This lets the browser build of `chatbot` reuse the exact same crate and
//! protocol as the Tauri build, without any Node server. Each connection can
//! run one transcription session at a time; the protocol is the shared
//! [`crate::protocol::ClientMessage`] / [`crate::protocol::ServerMessage`].

use std::net::SocketAddr;

use tokio::net::TcpListener;

use crate::protocol::ClientMessage;
use crate::protocol::ServerMessage;

mod conn;

/// Listens on `addr` and serves WebSocket transcription sessions forever.
pub async fn serve(addr: SocketAddr) -> Result<(), String> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| format!("failed to bind {addr}: {e}"))?;
    eprintln!("[audio:server] listening on ws://{addr}");

    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                eprintln!("[audio:server] accept failed: {e}");
                continue;
            }
        };
        eprintln!("[audio:server] connection from {peer}");
        tokio::spawn(async move {
            if let Err(e) = conn::handle(tcp).await {
                eprintln!("[audio:server] connection error ({peer}): {e}");
            }
        });
    }
}

/// Serializes one server message into a JSON text frame.
fn encode(msg: &ServerMessage) -> String {
    // Serialization of our own types cannot fail; if it ever does, surface it
    // as an error message so the client sees something instead of silence.
    serde_json::to_string(msg)
        .unwrap_or_else(|e| format!(r#"{{"type":"error","message":"encode: {e}"}}"#))
}

/// Parses one incoming text frame into a client message.
fn decode(text: &str) -> Result<ClientMessage, String> {
    serde_json::from_str(text).map_err(|e| format!("invalid message: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::LocalConfig;
    use crate::protocol::MistralConfig;
    use crate::protocol::ProviderKind;

    #[test]
    fn encode_includes_type_tag() {
        let s = encode(&ServerMessage::Ready);
        assert_eq!(s, r#"{"type":"ready"}"#);
    }

    #[test]
    fn decode_list_sources() {
        let msg = decode(r#"{"type":"list_sources"}"#).unwrap();
        assert_eq!(msg, ClientMessage::ListSources);
    }

    #[test]
    fn decode_start_roundtrip() {
        let msg = ClientMessage::Start {
            source_id: "mic".to_string(),
            provider: ProviderKind::Mistral,
            config: MistralConfig::default(),
            local: LocalConfig::default(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back = decode(&json).unwrap();
        assert_eq!(back, msg);
    }
}
