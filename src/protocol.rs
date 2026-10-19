//! Unified protocol types shared by every consumer of this crate.

use serde::{Deserialize, Serialize};

/// A chunk of raw PCM audio. `pcm_s16le`, mono, 16 kHz.
pub type AudioChunk = Vec<u8>;

/// A kind of audio source that can be captured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    /// Microphone input.
    Microphone,
    /// System output (a PulseAudio/PipeWire monitor source).
    System,
}

/// A capture source the user can pick in the UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AudioSource {
    /// Stable identifier (implementation-defined, e.g. a PulseAudio source name).
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// What kind of source this is.
    pub kind: SourceKind,
    /// Whether this is the system default of its kind.
    pub is_default: bool,
}

/// Connection parameters for the Mistral realtime endpoint.
///
/// This is part of the shared contract: the UI builds it from its settings and
/// sends it with every `start`, so neither the Tauri core nor the WebSocket
/// server ever stores credentials on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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

/// Parameters for the local (offline) provider.
///
/// Identifies a model by its catalog id (see `model_store::catalog`); the
/// on-disk directory is resolved by the crate, not supplied by the UI. The
/// language hint is optional — an empty string means the model auto-detects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LocalConfig {
    /// Catalog id of the model to run (e.g. "nemotron-3.5-asr-streaming-0.6b").
    pub model_id: String,
    /// Language hint (e.g. "ru" or "en"); an empty string means auto-detect.
    pub language: String,
}

/// Which transcription backend to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Cloud transcription via the Mistral realtime API.
    Mistral,
    /// Local transcription (transcribe-cpp / Whisper-style models).
    Local,
}

/// A command the frontend sends to start a session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartRequest {
    /// Which source to capture from.
    pub source_id: String,
    /// Which backend to transcribe with.
    pub provider: ProviderKind,
}

/// Messages sent from the client to the server (Tauri command or WebSocket).
///
/// The discriminant lives in the `type` field, matching the existing frontend
/// protocol. `Start` carries the full provider configuration so the server is
/// stateless and never stores credentials.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Start a new transcription session with the given source and config.
    Start {
        source_id: String,
        provider: ProviderKind,
        config: MistralConfig,
        /// Local-model parameters; ignored by the Mistral provider. Defaults so
        /// older clients that don't send it still deserialize.
        #[serde(default)]
        local: LocalConfig,
    },
    /// Stop the current session.
    Stop,
    /// Mute without tearing the session down (provider sends nothing).
    Pause,
    /// Unmute, resuming instantly on the same connection.
    Resume,
    /// Request the list of available capture sources.
    ListSources,
}

/// Events emitted from a running session back to the frontend.
///
/// The discriminant lives in the `type` field (tagged enum), matching the
/// wire format used by the existing Node server and the frontend hook.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Session is ready and capturing.
    Ready,
    /// A partial transcription result.
    Delta { text: String },
    /// An utterance finished transcribing.
    Done,
    /// An error occurred; the session ended.
    Error { message: String },
    /// Sent in response to `ClientMessage::ListSources`.
    Sources { sources: Vec<AudioSource> },
    /// The session was stopped.
    Stopped,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_message_wire_format_matches_existing_protocol() {
        let delta = ServerMessage::Delta {
            text: "hello ".to_string(),
        };
        let json = serde_json::to_string(&delta).unwrap();
        assert_eq!(json, r#"{"type":"delta","text":"hello "}"#);

        let done = ServerMessage::Done;
        let json = serde_json::to_string(&done).unwrap();
        assert_eq!(json, r#"{"type":"done"}"#);
    }

    #[test]
    fn roundtrips_start_request() {
        let req = StartRequest {
            source_id: "alsa_output.pci.monitor".to_string(),
            provider: ProviderKind::Local,
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: StartRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back, req);
    }

    #[test]
    fn deserializes_server_message_by_discriminant() {
        let raw = r#"{"type":"error","message":"boom"}"#;
        let msg: ServerMessage = serde_json::from_str(raw).unwrap();
        assert_eq!(
            msg,
            ServerMessage::Error {
                message: "boom".to_string(),
            }
        );
    }

    #[test]
    fn client_message_start_carries_full_config() {
        let raw = r#"{
            "type": "start",
            "source_id": "alsa_input.usb",
            "provider": "mistral",
            "config": {
                "apiKey": "key-123",
                "baseUrl": "wss://api.mistral.ai",
                "model": "voxtral-mini-transcribe-realtime-2602",
                "sampleRate": 16000
            }
        }"#;
        let msg: ClientMessage = serde_json::from_str(raw).unwrap();
        match msg {
            ClientMessage::Start {
                source_id,
                provider,
                config,
                local,
            } => {
                assert_eq!(source_id, "alsa_input.usb");
                assert_eq!(provider, ProviderKind::Mistral);
                assert_eq!(config.api_key, "key-123");
                assert_eq!(config.sample_rate, 16000);
                // Absent `local` defaults to empty, preserving wire compatibility.
                assert_eq!(local, LocalConfig::default());
            }
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn serializes_sources_response() {
        let msg = ServerMessage::Sources {
            sources: vec![AudioSource {
                id: "mic".to_string(),
                name: "Default mic".to_string(),
                kind: SourceKind::Microphone,
                is_default: true,
            }],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(
            json.contains(r#""type":"sources""#),
            "sources message must carry the type tag, got: {json}"
        );
    }

    /// The exact wire format the WebSocket frontend must send. Pinning it in a
    /// test prevents drift if the serde attributes on `ClientMessage` change.
    #[test]
    fn start_message_wire_format() {
        let msg = ClientMessage::Start {
            source_id: "mic-1".to_string(),
            provider: ProviderKind::Mistral,
            config: MistralConfig::default(),
            local: LocalConfig::default(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(
            json,
            r#"{"type":"start","source_id":"mic-1","provider":"mistral","config":{"apiKey":"","baseUrl":"wss://api.mistral.ai","model":"voxtral-mini-transcribe-realtime-2602","sampleRate":16000},"local":{"modelId":"","language":""}}"#
        );
    }
}
