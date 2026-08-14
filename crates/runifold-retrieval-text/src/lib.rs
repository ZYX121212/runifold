//! Deterministic, dependency-light text ingestion for retrieval pipelines.

use std::{collections::BTreeMap, num::NonZeroUsize, str::Utf8Error};

use runifold_retrieval::{Document, RetrievalError};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

/// Maximum accepted source size for the default plain-text loader.
pub const DEFAULT_MAX_TEXT_BYTES: usize = 16 * 1024 * 1024;

/// Validated Unicode character chunking policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TextChunkPolicy {
    max_chars: NonZeroUsize,
    overlap_chars: usize,
}

impl TextChunkPolicy {
    /// Creates a bounded policy.
    ///
    /// # Errors
    ///
    /// Rejects overlap greater than or equal to the chunk size.
    pub fn new(max_chars: NonZeroUsize, overlap_chars: usize) -> Result<Self, TextIngestionError> {
        if overlap_chars >= max_chars.get() {
            return Err(TextIngestionError::InvalidOverlap {
                max_chars: max_chars.get(),
                overlap_chars,
            });
        }
        Ok(Self {
            max_chars,
            overlap_chars,
        })
    }

    /// Returns the maximum Unicode scalar count per chunk.
    pub const fn max_chars(self) -> usize {
        self.max_chars.get()
    }

    /// Returns the repeated Unicode scalar count between adjacent chunks.
    pub const fn overlap_chars(self) -> usize {
        self.overlap_chars
    }
}

/// Loads bounded UTF-8 bytes into one application-owned document.
///
/// # Errors
///
/// Rejects oversized or invalid UTF-8 input and propagates document validation.
pub fn load_text(
    id: impl Into<String>,
    bytes: &[u8],
    max_bytes: usize,
) -> Result<Document, TextIngestionError> {
    if bytes.len() > max_bytes {
        return Err(TextIngestionError::SourceTooLarge {
            limit: max_bytes,
            actual: bytes.len(),
        });
    }
    let text = std::str::from_utf8(bytes)?;
    Document::new(id, text).map_err(TextIngestionError::Document)
}

/// Loads newline-delimited JSON records containing `id`, `text`, and optional
/// object `metadata` fields.
///
/// Blank lines are ignored. The complete source and output count are bounded
/// before records are returned.
///
/// # Errors
///
/// Rejects oversized input, malformed records, duplicate identities, excessive
/// record counts, and invalid canonical documents.
pub fn load_json_lines(
    bytes: &[u8],
    max_bytes: usize,
    max_documents: usize,
) -> Result<Vec<Document>, TextIngestionError> {
    if bytes.len() > max_bytes {
        return Err(TextIngestionError::SourceTooLarge {
            limit: max_bytes,
            actual: bytes.len(),
        });
    }
    let text = std::str::from_utf8(bytes)?;
    let mut documents = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        if documents.len() >= max_documents {
            return Err(TextIngestionError::TooManyDocuments {
                limit: max_documents,
            });
        }
        let line_number = index.saturating_add(1);
        let record: JsonLineDocument =
            serde_json::from_str(line).map_err(|error| TextIngestionError::InvalidJsonLine {
                line: line_number,
                message: error.to_string(),
            })?;
        let document = Document::new(record.id, record.text)
            .map(|document| document.with_metadata(record.metadata))
            .map_err(TextIngestionError::Document)?;
        if !seen.insert(document.id.clone()) {
            return Err(TextIngestionError::DuplicateDocument {
                id: document.id.to_string(),
            });
        }
        documents.push(document);
    }
    Ok(documents)
}

/// Splits Markdown at ATX headings while preserving headings in section text.
///
/// Text before the first heading becomes section zero. Empty sections are not
/// emitted. Section IDs use `<source-id>#section-<zero-based-index>` and carry
/// `runifold.text.markdown_heading` metadata when a heading is present.
///
/// # Errors
///
/// Propagates canonical document validation failures.
pub fn split_markdown_sections(source: &Document) -> Result<Vec<Document>, TextIngestionError> {
    let mut sections = Vec::<(Option<String>, String)>::new();
    let mut heading = None;
    let mut body = String::new();
    for line in source.text.lines() {
        if let Some(title) = markdown_heading(line) {
            push_markdown_section(&mut sections, heading.take(), &mut body);
            heading = Some(title.into());
        }
        body.push_str(line);
        body.push('\n');
    }
    push_markdown_section(&mut sections, heading, &mut body);
    sections
        .into_iter()
        .enumerate()
        .map(|(index, (heading, text))| {
            let mut metadata = source.metadata.clone();
            metadata.insert(
                "runifold.text.source_document_id".into(),
                Value::String(source.id.to_string()),
            );
            metadata.insert("runifold.text.section_index".into(), Value::from(index));
            if let Some(heading) = heading {
                metadata.insert(
                    "runifold.text.markdown_heading".into(),
                    Value::String(heading),
                );
            }
            Document::new(format!("{}#section-{index}", source.id), text)
                .map(|document| document.with_metadata(metadata))
                .map_err(TextIngestionError::Document)
        })
        .collect()
}

#[derive(Deserialize)]
struct JsonLineDocument {
    id: String,
    text: String,
    #[serde(default)]
    metadata: BTreeMap<String, Value>,
}

fn markdown_heading(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let hashes = trimmed.bytes().take_while(|byte| *byte == b'#').count();
    if !(1..=6).contains(&hashes) || trimmed.as_bytes().get(hashes) != Some(&b' ') {
        return None;
    }
    Some(trimmed[hashes + 1..].trim())
}

fn push_markdown_section(
    sections: &mut Vec<(Option<String>, String)>,
    heading: Option<String>,
    body: &mut String,
) {
    let text = body.trim().to_owned();
    if !text.is_empty() {
        sections.push((heading, text));
    }
    body.clear();
}

/// Splits one document into stable, provenance-preserving Unicode chunks.
///
/// Chunk IDs use `<source-id>#chunk-<zero-based-index>`. Existing metadata is
/// retained, while `runifold.text.*` keys record source identity and character
/// offsets. Metadata remains host-only at the retrieval boundary.
///
/// # Errors
///
/// Propagates document validation if a generated identity or body is invalid.
pub fn chunk_document(
    document: &Document,
    policy: TextChunkPolicy,
) -> Result<Vec<Document>, TextIngestionError> {
    let boundaries = document
        .text
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(document.text.len()))
        .collect::<Vec<_>>();
    let char_count = boundaries.len().saturating_sub(1);
    if char_count <= policy.max_chars() {
        return Ok(vec![chunk(document, 0, 0, char_count, &boundaries)?]);
    }
    let step = policy.max_chars() - policy.overlap_chars();
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < char_count {
        let end = start.saturating_add(policy.max_chars()).min(char_count);
        chunks.push(chunk(document, chunks.len(), start, end, &boundaries)?);
        if end == char_count {
            break;
        }
        start = start.saturating_add(step);
    }
    Ok(chunks)
}

fn chunk(
    source: &Document,
    index: usize,
    start: usize,
    end: usize,
    boundaries: &[usize],
) -> Result<Document, TextIngestionError> {
    let text = source.text[boundaries[start]..boundaries[end]].to_owned();
    let mut metadata: BTreeMap<String, Value> = source.metadata.clone();
    metadata.insert(
        "runifold.text.source_document_id".into(),
        Value::String(source.id.as_str().into()),
    );
    metadata.insert("runifold.text.chunk_index".into(), Value::from(index));
    metadata.insert("runifold.text.char_start".into(), Value::from(start));
    metadata.insert("runifold.text.char_end".into(), Value::from(end));
    Document::new(format!("{}#chunk-{index}", source.id), text)
        .map(|document| document.with_metadata(metadata))
        .map_err(TextIngestionError::Document)
}

/// Typed text-ingestion failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TextIngestionError {
    /// Source bytes exceed the explicit loader limit.
    #[error("text source contains {actual} bytes; limit is {limit}")]
    SourceTooLarge {
        /// Configured maximum.
        limit: usize,
        /// Observed byte count.
        actual: usize,
    },
    /// Source bytes are not valid UTF-8.
    #[error("text source is not valid UTF-8: {0}")]
    InvalidUtf8(#[from] Utf8Error),
    /// Chunk overlap cannot make forward progress.
    #[error("overlap {overlap_chars} must be smaller than chunk size {max_chars}")]
    InvalidOverlap {
        /// Maximum characters per chunk.
        max_chars: usize,
        /// Requested overlap.
        overlap_chars: usize,
    },
    /// A JSON Lines source exceeded its explicit record bound.
    #[error("JSON Lines source exceeds {limit} documents")]
    TooManyDocuments {
        /// Configured maximum document count.
        limit: usize,
    },
    /// One JSON Lines record was malformed.
    #[error("invalid JSON Lines record at line {line}: {message}")]
    InvalidJsonLine {
        /// One-based source line.
        line: usize,
        /// Parser diagnostic.
        message: String,
    },
    /// A JSON Lines source repeated one document identity.
    #[error("duplicate JSON Lines document id `{id}`")]
    DuplicateDocument {
        /// Repeated identity.
        id: String,
    },
    /// Canonical document validation failed.
    #[error("invalid retrieval document: {0}")]
    Document(#[source] RetrievalError),
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use runifold_retrieval::Document;

    use super::{
        TextChunkPolicy, TextIngestionError, chunk_document, load_json_lines, load_text,
        split_markdown_sections,
    };

    #[test]
    fn unicode_chunks_have_stable_overlap_and_provenance() {
        let source = Document::new("guide", "甲乙丙丁戊己庚").unwrap();
        let policy = TextChunkPolicy::new(NonZeroUsize::new(4).unwrap(), 1).unwrap();

        let chunks = chunk_document(&source, policy).unwrap();

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "甲乙丙丁");
        assert_eq!(chunks[1].text, "丁戊己庚");
        assert_eq!(chunks[1].id.as_str(), "guide#chunk-1");
        assert_eq!(chunks[1].metadata["runifold.text.char_start"], 3);
    }

    #[test]
    fn loader_rejects_oversize_before_utf8_decode() {
        let error = load_text("doc", &[0xff, 0xff], 1).unwrap_err();
        assert!(matches!(error, TextIngestionError::SourceTooLarge { .. }));
    }

    #[test]
    fn json_lines_loader_is_bounded_and_preserves_metadata() {
        let documents = load_json_lines(
            br#"{"id":"a","text":"alpha","metadata":{"tenant":"one"}}
{"id":"b","text":"beta"}
"#,
            1_024,
            2,
        )
        .unwrap();
        assert_eq!(documents.len(), 2);
        assert_eq!(documents[0].metadata["tenant"], "one");
    }

    #[test]
    fn markdown_sections_have_stable_ids_and_headings() {
        let source = Document::new("guide", "intro\n# First\none\n## Second\ntwo").unwrap();
        let sections = split_markdown_sections(&source).unwrap();

        assert_eq!(sections.len(), 3);
        assert_eq!(sections[1].id.as_str(), "guide#section-1");
        assert_eq!(
            sections[1].metadata["runifold.text.markdown_heading"],
            "First"
        );
        assert!(sections[1].text.starts_with("# First"));
    }
}
