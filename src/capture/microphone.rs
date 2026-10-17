//! Microphone capture via the `parec` CLI (PulseAudio/PipeWire, Linux).

use std::io::Read;
use std::process::{Child, Command, Stdio};

use super::AudioStream;
use crate::protocol::{AudioChunk, AudioSource, SourceKind};

/// Bytes read from `parec` stdout per call. At 16 kHz mono s16le this is
/// ~40 ms of audio per chunk.
const READ_BUF_SIZE: usize = 640;

/// Captures the default/selected microphone and yields `pcm_s16le` chunks.
pub struct Microphone {
    child: Child,
    reader: std::process::ChildStdout,
}

impl Microphone {
    /// Spawns `parec` for the given source name (`""` = default source).
    pub fn start(sample_rate: u32, source_id: &str) -> Result<Self, String> {
        let mut cmd = Command::new("parec");
        cmd.args([
            "--format=s16le",
            &format!("--rate={sample_rate}"),
            "--channels=1",
            "--latency-msec=100",
        ]);
        if !source_id.is_empty() {
            cmd.arg(format!("--device={source_id}"));
        }

        let mut child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| format!("Failed to spawn parec: {e}"))?;

        let reader = child
            .stdout
            .take()
            .ok_or_else(|| "parec stdout not piped".to_string())?;

        Ok(Self { child, reader })
    }
}

impl AudioStream for Microphone {
    fn next_chunk(&mut self) -> Option<AudioChunk> {
        let mut buf = vec![0u8; READ_BUF_SIZE];
        match self.reader.read(&mut buf) {
            Ok(0) | Err(_) => None,
            Ok(n) => {
                buf.truncate(n);
                Some(buf)
            }
        }
    }
}

impl Drop for Microphone {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Lists microphone sources (non-monitor PulseAudio sources).
pub fn list() -> Result<Vec<AudioSource>, String> {
    let output = Command::new("pactl")
        .args(["list", "sources"])
        .output()
        .map_err(|e| format!("Failed to run pactl: {e}"))?;
    let text = String::from_utf8_lossy(&output.stdout);

    let default = default_source();
    let mut sources = Vec::new();
    let mut name: Option<String> = None;
    let mut description: Option<String> = None;

    let flush = |name: &Option<String>,
                 description: &Option<String>,
                 sources: &mut Vec<AudioSource>,
                 default: &Option<String>| {
        let Some(id) = name.clone() else { return };
        // Skip system monitors — those are exposed by `system.rs`.
        if id.ends_with(".monitor") {
            return;
        }
        let label = description
            .clone()
            .filter(|d| !d.trim().is_empty())
            .unwrap_or_else(|| id.clone());
        sources.push(AudioSource {
            id: id.clone(),
            name: label,
            kind: SourceKind::Microphone,
            is_default: default.as_deref() == Some(id.as_str()),
        });
    };

    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("Name:") {
            flush(&name, &description, &mut sources, &default);
            name = Some(rest.trim().to_string());
            description = None;
        } else if let Some(rest) = line.strip_prefix("Description:") {
            description = Some(rest.trim().to_string());
        }
    }
    flush(&name, &description, &mut sources, &default);

    // If no source was matched as default, mark the first one.
    if !sources.iter().any(|s| s.is_default)
        && let Some(first) = sources.first_mut()
    {
        first.is_default = true;
    }

    Ok(sources)
}

/// The PulseAudio default source name, if any.
fn default_source() -> Option<String> {
    Command::new("pactl")
        .args(["get-default-source"])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn microphone_ids_are_not_monitors() {
        // Whatever the environment, `list()` must never expose `.monitor`
        // sources — those belong to `system::list()`.
        if let Ok(sources) = list() {
            assert!(sources.iter().all(|s| !s.id.ends_with(".monitor")));
            assert!(sources.iter().all(|s| s.kind == SourceKind::Microphone));
        }
    }
}
