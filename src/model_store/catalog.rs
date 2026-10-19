//! Static catalog of downloadable local models.
//!
//! Each entry describes where to fetch a model's files and how to verify them.
//! The catalog is intentionally data-only and additive: adding a model is just
//! adding an entry, no new code.

use serde::{Deserialize, Serialize};

/// One file that makes up a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelFile {
    /// File name on disk (also the last path segment of `url`).
    pub name: String,
    /// Size in bytes (used for progress and pre-flight checks).
    pub size: u64,
    /// Expected SHA-256, verified after download. `None` skips verification
    /// (used for files not tracked as LFS where only a git blob id is exposed).
    pub sha256: Option<String>,
}

/// A downloadable model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelEntry {
    /// Stable identifier (also the on-disk subdirectory name).
    pub id: String,
    /// Human-readable display name.
    pub name: String,
    /// Hugging Face repository, e.g. `csukuangfj/parakeet-...`.
    pub repo: String,
    /// Pinned revision so a re-download is byte-identical and reproducible.
    pub revision: String,
    /// The files to fetch, in download order.
    pub files: Vec<ModelFile>,
}

impl ModelEntry {
    /// Total size of all files, in bytes.
    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    /// Whether every file of this model is present on disk.
    pub fn is_downloaded(&self) -> bool {
        self.files
            .iter()
            .all(|f| super::paths::model_dir(&self.id).join(&f.name).exists())
    }
}

/// A model together with its on-disk download status, serialized for the UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelStatus {
    #[serde(flatten)]
    pub entry: ModelEntry,
    pub is_downloaded: bool,
}

impl ModelStatus {
    pub fn of(entry: ModelEntry) -> Self {
        let is_downloaded = entry.is_downloaded();
        Self {
            entry,
            is_downloaded,
        }
    }
}

/// Resolves a file to its Hugging Face `resolve` URL.
pub fn file_url(entry: &ModelEntry, file: &ModelFile) -> String {
    format!(
        "https://huggingface.co/{}/resolve/{}/{}",
        entry.repo, entry.revision, file.name
    )
}

/// Parakeet Unified EN 0.6B — unified offline+streaming RNNT, English only,
/// native punctuation & capitalization (Q8_0 GGUF).
fn parakeet_unified_en() -> ModelEntry {
    ModelEntry {
        id: "parakeet-unified-en-0.6b".to_string(),
        name: "Parakeet Unified EN 0.6B".to_string(),
        repo: "handy-computer/parakeet-unified-en-0.6b-gguf".to_string(),
        revision: "7e948f21b7bdbac698d3318db9d350f1096f3b6c".to_string(),
        files: vec![ModelFile {
            name: "parakeet-unified-en-0.6b-Q8_0.gguf".to_string(),
            size: 731357568,
            sha256: Some(
                "4b50b6dd862bf6e346929aaf4f5eaacec003bfa3f56462d6c874b41ef2f38795".to_string(),
            ),
        }],
    }
}

/// Nemotron 3.5 ASR Streaming 0.6B — cache-aware streaming, 28 languages
/// including Russian, native punctuation & capitalization (Q8_0 GGUF).
fn nemotron_3_5_asr_streaming() -> ModelEntry {
    ModelEntry {
        id: "nemotron-3.5-asr-streaming-0.6b".to_string(),
        name: "Nemotron Streaming 3.5".to_string(),
        repo: "handy-computer/nemotron-3.5-asr-streaming-0.6b-gguf".to_string(),
        revision: "6d44e540bc31b0de1dbe174a3cea87f53a7f22fb".to_string(),
        files: vec![ModelFile {
            name: "nemotron-3.5-asr-streaming-0.6b-Q8_0.gguf".to_string(),
            size: 751094240,
            sha256: Some(
                "b94545b313b3223fda7b2857a52681da813935c2127643d1e9ff0c23d988089c".to_string(),
            ),
        }],
    }
}

/// Voxtral Mini 4B Realtime — causal audio encoder + LLM, 13 languages
/// including Russian, native streaming with punctuation (Q5_K_M GGUF).
fn voxtral_mini_4b_realtime() -> ModelEntry {
    ModelEntry {
        id: "voxtral-mini-4b-realtime".to_string(),
        name: "Voxtral Mini 4B Realtime".to_string(),
        repo: "handy-computer/Voxtral-Mini-4B-Realtime-2602-gguf".to_string(),
        revision: "b3e1c979e3775cbd0a49a65878a0ec7f06789ed7".to_string(),
        files: vec![ModelFile {
            name: "Voxtral-Mini-4B-Realtime-2602-Q5_K_M.gguf".to_string(),
            size: 3281439008,
            sha256: Some(
                "e20a7582c5cf8159454c909b7f57b184b287cb3d7c3ec85744727a2cec51b08f".to_string(),
            ),
        }],
    }
}

/// All models currently offered for download.
pub fn all_models() -> Vec<ModelEntry> {
    vec![
        parakeet_unified_en(),
        nemotron_3_5_asr_streaming(),
        voxtral_mini_4b_realtime(),
    ]
}

/// Looks up a model by id.
pub fn find(id: &str) -> Option<ModelEntry> {
    all_models().into_iter().find(|m| m.id == id)
}

/// All models, each annotated with its on-disk download status.
pub fn all_statuses() -> Vec<ModelStatus> {
    all_models().into_iter().map(ModelStatus::of).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_contains_three_streaming_gguf_models() {
        let models = all_models();
        assert_eq!(models.len(), 3);
        let ids: Vec<&str> = models.iter().map(|m| m.id.as_str()).collect();
        assert!(ids.contains(&"parakeet-unified-en-0.6b"));
        assert!(ids.contains(&"nemotron-3.5-asr-streaming-0.6b"));
        assert!(ids.contains(&"voxtral-mini-4b-realtime"));
        // Every model is a single GGUF file.
        for m in &models {
            assert_eq!(m.files.len(), 1);
            assert!(m.files[0].name.ends_with(".gguf"));
        }
    }

    #[test]
    fn file_url_uses_pinned_revision() {
        let entry = find("nemotron-3.5-asr-streaming-0.6b").unwrap();
        let url = file_url(&entry, &entry.files[0]);
        assert_eq!(
            url,
            format!(
                "https://huggingface.co/{}/resolve/{}/nemotron-3.5-asr-streaming-0.6b-Q8_0.gguf",
                entry.repo, entry.revision
            )
        );
    }

    #[test]
    fn total_size_is_sum_of_files() {
        let entry = find("parakeet-unified-en-0.6b").unwrap();
        let sum: u64 = entry.files.iter().map(|f| f.size).sum();
        assert_eq!(entry.total_size(), sum);
    }
}
