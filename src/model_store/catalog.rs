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
}

/// Resolves a file to its Hugging Face `resolve` URL.
pub fn file_url(entry: &ModelEntry, file: &ModelFile) -> String {
    format!(
        "https://huggingface.co/{}/resolve/{}/{}",
        entry.repo, entry.revision, file.name
    )
}

/// The local Parakeet TDT 0.6B v3 (int8) model — 25 European languages
/// including Russian and English, with automatic language detection.
fn parakeet_tdt_v3() -> ModelEntry {
    ModelEntry {
        id: "parakeet-tdt-v3".to_string(),
        name: "Parakeet TDT 0.6B v3".to_string(),
        repo: "csukuangfj/sherpa-onnx-nemo-parakeet-tdt-0.6b-v3-int8".to_string(),
        revision: "2bda32ec70b097a55adaa07d9a7173915b43cc78".to_string(),
        files: vec![
            ModelFile {
                name: "encoder.int8.onnx".to_string(),
                size: 652184281,
                sha256: Some(
                    "acfc2b4456377e15d04f0243af540b7fe7c992f8d898d751cf134c3a55fd2247".to_string(),
                ),
            },
            ModelFile {
                name: "decoder.int8.onnx".to_string(),
                size: 11845275,
                sha256: Some(
                    "179e50c43d1a9de79c8a24149a2f9bac6eb5981823f2a2ed88d655b24248db4e".to_string(),
                ),
            },
            ModelFile {
                name: "joiner.int8.onnx".to_string(),
                size: 6355277,
                sha256: Some(
                    "3164c13fc2821009440d20fcb5fdc78bff28b4db2f8d0f0b329101719c0948b3".to_string(),
                ),
            },
            ModelFile {
                name: "tokens.txt".to_string(),
                size: 93939,
                sha256: None,
            },
        ],
    }
}

/// All models currently offered for download.
pub fn all_models() -> Vec<ModelEntry> {
    vec![parakeet_tdt_v3()]
}

/// Looks up a model by id.
pub fn find(id: &str) -> Option<ModelEntry> {
    all_models().into_iter().find(|m| m.id == id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_contains_parakeet_with_four_files() {
        let entry = find("parakeet-tdt-v3").expect("parakeet must be present");
        assert_eq!(entry.files.len(), 4);
        assert_eq!(
            entry
                .files
                .iter()
                .map(|f| f.name.clone())
                .collect::<Vec<_>>(),
            vec![
                "encoder.int8.onnx",
                "decoder.int8.onnx",
                "joiner.int8.onnx",
                "tokens.txt",
            ]
        );
    }

    #[test]
    fn file_url_uses_pinned_revision() {
        let entry = find("parakeet-tdt-v3").unwrap();
        let url = file_url(&entry, &entry.files[0]);
        assert_eq!(
            url,
            format!(
                "https://huggingface.co/{}/resolve/{}/encoder.int8.onnx",
                entry.repo, entry.revision
            )
        );
    }

    #[test]
    fn total_size_is_sum_of_files() {
        let entry = find("parakeet-tdt-v3").unwrap();
        let sum: u64 = entry.files.iter().map(|f| f.size).sum();
        assert_eq!(entry.total_size(), sum);
    }
}
