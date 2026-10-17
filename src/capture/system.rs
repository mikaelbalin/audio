//! System audio capture via PulseAudio/PipeWire monitor sources (Linux).

use std::process::Command;

use super::AudioStream;
use super::microphone::Microphone;
use crate::protocol::{AudioSource, SourceKind};

/// Captures system output by recording from a `.monitor` source.
///
/// Reuses the same `parec` mechanics as [`Microphone`] — the only difference
/// is the device id passed to `--device`.
pub struct SystemAudio {
    inner: Microphone,
}

impl SystemAudio {
    /// Starts capturing the given monitor source (must end with `.monitor`).
    pub fn start(sample_rate: u32, source_id: &str) -> Result<Self, String> {
        if !source_id.ends_with(".monitor") {
            return Err(format!(
                "System capture expects a monitor source, got: {source_id}"
            ));
        }
        Ok(Self {
            inner: Microphone::start(sample_rate, source_id)?,
        })
    }
}

impl AudioStream for SystemAudio {
    fn next_chunk(&mut self) -> Option<crate::protocol::AudioChunk> {
        self.inner.next_chunk()
    }
}

/// Lists system output sources (`.monitor` entries only).
pub fn list() -> Result<Vec<AudioSource>, String> {
    let output = Command::new("pactl")
        .args(["list", "sources"])
        .output()
        .map_err(|e| format!("Failed to run pactl: {e}"))?;
    let text = String::from_utf8_lossy(&output.stdout);

    let default = default_monitor();
    let mut sources = Vec::new();
    let mut name: Option<String> = None;
    let mut description: Option<String> = None;

    let flush = |name: &Option<String>,
                 description: &Option<String>,
                 sources: &mut Vec<AudioSource>,
                 default: &Option<String>| {
        let Some(id) = name.clone() else { return };
        if !id.ends_with(".monitor") {
            return;
        }
        let label = description
            .clone()
            .filter(|d| !d.trim().is_empty())
            .unwrap_or_else(|| id.clone());
        sources.push(AudioSource {
            id: id.clone(),
            name: label,
            kind: SourceKind::System,
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

    Ok(sources)
}

/// The default source's monitor name (the most sensible system default).
fn default_monitor() -> Option<String> {
    Command::new("pactl")
        .args(["get-default-source"])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .map(|source| format!("{source}.monitor"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_ids_are_monitors() {
        // Whatever the environment, `list()` must only expose monitor ids.
        if let Ok(sources) = list() {
            assert!(sources.iter().all(|s| s.id.ends_with(".monitor")));
            assert!(sources.iter().all(|s| s.kind == SourceKind::System));
        }
    }

    #[test]
    fn non_monitor_source_id_is_rejected() {
        let result = SystemAudio::start(16000, "alsa_input.pci-0000.0");
        assert!(result.is_err());
    }
}
