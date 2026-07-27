use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use runifold_core::RunId;
use thiserror::Error;

use crate::{EvaluationDataset, EvaluationError, EvaluationReport};

/// Evaluation artifact repository failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EvaluationStoreError {
    /// Filesystem operation failed.
    #[error("evaluation store {operation} failed for {path}")]
    Io {
        /// Stable operation name.
        operation: &'static str,
        /// Affected path.
        path: PathBuf,
        /// Original filesystem error.
        #[source]
        source: io::Error,
    },
    /// JSON encoding or decoding failed.
    #[error("evaluation artifact JSON is invalid")]
    Json(#[from] serde_json::Error),
    /// Loaded domain data violates evaluation invariants.
    #[error(transparent)]
    InvalidArtifact(#[from] EvaluationError),
    /// An immutable artifact key already contains different content.
    #[error("evaluation artifact conflict at {path}")]
    Conflict {
        /// Conflicting artifact path.
        path: PathBuf,
    },
}

/// Versioned dataset and candidate-report repository.
pub trait EvaluationRepository: Send + Sync {
    /// Saves one immutable dataset version.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, serialization, validation, or conflict error.
    fn save_dataset(&self, dataset: &EvaluationDataset) -> Result<(), EvaluationStoreError>;

    /// Loads one dataset version.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, deserialization, or validation error.
    fn load_dataset(
        &self,
        name: &str,
        version: &str,
    ) -> Result<EvaluationDataset, EvaluationStoreError>;

    /// Saves one immutable candidate report.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, serialization, validation, or conflict error.
    fn save_report(&self, report: &EvaluationReport) -> Result<(), EvaluationStoreError>;

    /// Loads one candidate report.
    ///
    /// # Errors
    ///
    /// Returns a typed storage, deserialization, or validation error.
    fn load_report(
        &self,
        dataset_name: &str,
        dataset_version: &str,
        candidate_version: &str,
    ) -> Result<EvaluationReport, EvaluationStoreError>;
}

/// Filesystem-backed immutable JSON evaluation repository.
///
/// Identity components are hex encoded before path construction. Writes use a
/// same-directory temporary file and atomic no-replace link. Re-saving identical bytes
/// is idempotent; replacing an existing version with different bytes fails.
#[derive(Clone, Debug)]
pub struct FileEvaluationRepository {
    root: PathBuf,
}

impl FileEvaluationRepository {
    /// Creates a repository handle without touching the filesystem.
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }

    fn dataset_path(&self, name: &str, version: &str) -> PathBuf {
        self.root
            .join("datasets")
            .join(encode_component(name))
            .join(format!("{}.json", encode_component(version)))
    }

    fn report_path(
        &self,
        dataset_name: &str,
        dataset_version: &str,
        candidate_version: &str,
    ) -> PathBuf {
        self.root
            .join("reports")
            .join(encode_component(dataset_name))
            .join(encode_component(dataset_version))
            .join(format!("{}.json", encode_component(candidate_version)))
    }
}

impl EvaluationRepository for FileEvaluationRepository {
    fn save_dataset(&self, dataset: &EvaluationDataset) -> Result<(), EvaluationStoreError> {
        dataset.validate()?;
        let bytes = serde_json::to_vec_pretty(dataset)?;
        write_immutable(
            &self.dataset_path(dataset.name(), dataset.version()),
            &bytes,
        )
    }

    fn load_dataset(
        &self,
        name: &str,
        version: &str,
    ) -> Result<EvaluationDataset, EvaluationStoreError> {
        let path = self.dataset_path(name, version);
        let bytes = read(&path)?;
        let dataset = serde_json::from_slice::<EvaluationDataset>(&bytes)?;
        dataset.validate()?;
        Ok(dataset)
    }

    fn save_report(&self, report: &EvaluationReport) -> Result<(), EvaluationStoreError> {
        report.validate()?;
        let bytes = serde_json::to_vec_pretty(report)?;
        write_immutable(
            &self.report_path(
                &report.dataset_name,
                &report.dataset_version,
                &report.candidate_version,
            ),
            &bytes,
        )
    }

    fn load_report(
        &self,
        dataset_name: &str,
        dataset_version: &str,
        candidate_version: &str,
    ) -> Result<EvaluationReport, EvaluationStoreError> {
        let path = self.report_path(dataset_name, dataset_version, candidate_version);
        let bytes = read(&path)?;
        let report = serde_json::from_slice::<EvaluationReport>(&bytes)?;
        report.validate()?;
        Ok(report)
    }
}

fn read(path: &Path) -> Result<Vec<u8>, EvaluationStoreError> {
    fs::read(path).map_err(|source| EvaluationStoreError::Io {
        operation: "read",
        path: path.to_path_buf(),
        source,
    })
}

fn write_immutable(path: &Path, bytes: &[u8]) -> Result<(), EvaluationStoreError> {
    if let Some(existing) = read_if_present(path)? {
        return if existing == bytes {
            Ok(())
        } else {
            Err(EvaluationStoreError::Conflict {
                path: path.to_path_buf(),
            })
        };
    }
    let parent = path.parent().ok_or_else(|| EvaluationStoreError::Io {
        operation: "resolve parent",
        path: path.to_path_buf(),
        source: io::Error::new(io::ErrorKind::InvalidInput, "artifact path has no parent"),
    })?;
    fs::create_dir_all(parent).map_err(|source| EvaluationStoreError::Io {
        operation: "create directory",
        path: parent.to_path_buf(),
        source,
    })?;
    let temporary = parent.join(format!(".{}.tmp", RunId::new()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|source| EvaluationStoreError::Io {
            operation: "create temporary artifact",
            path: temporary.clone(),
            source,
        })?;
    if let Err(source) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = fs::remove_file(&temporary);
        return Err(EvaluationStoreError::Io {
            operation: "persist temporary artifact",
            path: temporary,
            source,
        });
    }
    match fs::hard_link(&temporary, path) {
        Ok(()) => fs::remove_file(&temporary).map_err(|source| EvaluationStoreError::Io {
            operation: "remove temporary artifact",
            path: temporary,
            source,
        }),
        Err(_source) if path.exists() => {
            let _ = fs::remove_file(&temporary);
            if read(path)? == bytes {
                Ok(())
            } else {
                Err(EvaluationStoreError::Conflict {
                    path: path.to_path_buf(),
                })
            }
        }
        Err(source) => {
            let _ = fs::remove_file(&temporary);
            Err(EvaluationStoreError::Io {
                operation: "commit artifact",
                path: path.to_path_buf(),
                source,
            })
        }
    }
}

fn read_if_present(path: &Path) -> Result<Option<Vec<u8>>, EvaluationStoreError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(EvaluationStoreError::Io {
            operation: "read existing artifact",
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn encode_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::{
        EvaluationCase, EvaluationDataset, EvaluationRepository, EvaluationStoreError,
        FileEvaluationRepository,
    };

    #[test]
    fn file_repository_round_trips_and_rejects_version_replacement() {
        let root =
            std::env::temp_dir().join(format!("runifold-eval-{}", runifold_core::RunId::new()));
        let repository = FileEvaluationRepository::new(&root);
        let dataset = EvaluationDataset::new(
            "../unsafe name",
            "v1",
            vec![EvaluationCase::new("one", serde_json::json!("private")).unwrap()],
        )
        .unwrap();

        repository.save_dataset(&dataset).unwrap();
        repository.save_dataset(&dataset).unwrap();
        let loaded = repository.load_dataset("../unsafe name", "v1").unwrap();
        assert_eq!(loaded.name(), "../unsafe name");
        assert_eq!(loaded.cases()[0].input(), &serde_json::json!("private"));

        let changed = EvaluationDataset::new(
            "../unsafe name",
            "v1",
            vec![EvaluationCase::new("two", serde_json::json!("changed")).unwrap()],
        )
        .unwrap();
        assert!(matches!(
            repository.save_dataset(&changed),
            Err(EvaluationStoreError::Conflict { .. })
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
