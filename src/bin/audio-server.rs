//! `audio-server` — WebSocket daemon wrapping the `audio` crate.
//!
//! Run it on any machine whose microphone/system audio you want to capture;
//! clients connect over `ws://<host>:<port>` and drive sessions with the
//! shared `ClientMessage`/`ServerMessage` protocol. All settings (including
//! the Mistral API key) arrive per-session from the client, so the daemon
//! itself holds no credentials or `.env` file.

use std::net::SocketAddr;

/// Default listen address/port, matching the previous Node server.
const DEFAULT_ADDR: &str = "127.0.0.1:8765";

/// Parses `--addr <addr>` / `--port <n>`; falls back to [`DEFAULT_ADDR`].
fn parse_addr() -> SocketAddr {
    let args: Vec<String> = std::env::args().collect();
    let mut addr = DEFAULT_ADDR.to_string();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" if i + 1 < args.len() => {
                addr = args[i + 1].clone();
                i += 1;
            }
            "--port" if i + 1 < args.len() => {
                addr = format!("127.0.0.1:{}", args[i + 1]);
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    addr.parse()
        .unwrap_or_else(|e| panic!("invalid listen address '{addr}': {e}"))
}

#[tokio::main]
async fn main() {
    let addr = parse_addr();
    if let Err(e) = audio::server::serve(addr).await {
        eprintln!("[audio-server] fatal: {e}");
        std::process::exit(1);
    }
}
