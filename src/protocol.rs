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
}
