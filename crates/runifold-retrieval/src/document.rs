use std::{collections::BTreeMap, fmt};

use serde::Serialize;
use serde_json::Value;

use crate::RetrievalError;

/// Stable application-owned document identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct DocumentId(String);

impl DocumentId {
    /// Validates and creates a document identity.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievalError::EmptyDocumentId`] for blank input.
    pub fn new(value: impl Into<String>) -> Result<Self, RetrievalError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(RetrievalError::EmptyDocumentId);
        }
        Ok(Self(value))
    }

    /// Returns the application identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl TryFrom<String> for DocumentId {
    type Error = RetrievalError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for DocumentId {
    type Error = RetrievalError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Text document and host-only metadata used for retrieval.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Document {
    /// Stable application identity.
    pub id: DocumentId,
    /// Text embedded and optionally exposed as untrusted model context.
    pub text: String,
    /// Host-only metadata. Agent context rendering does not expose it.
    pub metadata: BTreeMap<String, Value>,
}

impl Document {
    /// Validates and creates a text document.
    ///
    /// # Errors
    ///
    /// Returns an error for a blank identity or text body.
    pub fn new(id: impl Into<String>, text: impl Into<String>) -> Result<Self, RetrievalError> {
        let id = DocumentId::new(id)?;
        let text = text.into();
        if text.trim().is_empty() {
            return Err(RetrievalError::EmptyDocumentText { id });
        }
        Ok(Self {
            id,
            text,
            metadata: BTreeMap::new(),
        })
    }

    /// Replaces host-only metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: BTreeMap<String, Value>) -> Self {
        self.metadata = metadata;
        self
    }
}
