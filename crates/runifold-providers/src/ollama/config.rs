//! Validated Ollama client configuration.

use std::fmt;

use secrecy::SecretString;
use thiserror::Error;
use url::Url;

/// Invalid Ollama endpoint configuration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum OllamaConfigError {
    /// The base URL was invalid.
    #[error("invalid Ollama base URL: {0}")]
    InvalidBaseUrl(#[from] url::ParseError),
    /// The URL was not a hierarchical HTTP endpoint.
    #[error("Ollama base URL must be an HTTP(S) base URL")]
    InvalidBaseUrlShape,
    /// A configured bearer token was blank.
    #[error("Ollama bearer token cannot be empty")]
    EmptyBearerToken,
    /// An embedding model identity was empty.
    #[error("Ollama embedding model cannot be empty")]
    EmptyEmbeddingModel,
}

/// Ollama native API configuration for local or hosted endpoints.
#[derive(Clone)]
pub struct OllamaConfig {
    pub(crate) base_url: Url,
    pub(crate) bearer_token: Option<SecretString>,
}

impl OllamaConfig {
    /// Creates configuration for an Ollama endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid or non-HTTP(S) URL.
    pub fn new(base_url: &str) -> Result<Self, OllamaConfigError> {
        let mut base_url = Url::parse(base_url)?;
        if !matches!(base_url.scheme(), "http" | "https") || base_url.cannot_be_a_base() {
            return Err(OllamaConfigError::InvalidBaseUrlShape);
        }
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Ok(Self {
            base_url,
            bearer_token: None,
        })
    }

    /// Creates configuration for the standard local daemon.
    ///
    /// # Errors
    ///
    /// Returns an error only if the built-in URL cannot be parsed.
    pub fn local() -> Result<Self, OllamaConfigError> {
        Self::new("http://127.0.0.1:11434/")
    }

    /// Adds bearer authentication for a hosted Ollama endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for a blank token.
    pub fn with_bearer_token(
        mut self,
        token: impl Into<String>,
    ) -> Result<Self, OllamaConfigError> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(OllamaConfigError::EmptyBearerToken);
        }
        self.bearer_token = Some(SecretString::from(token));
        Ok(self)
    }

    pub(crate) fn endpoint_url(&self) -> Url {
        self.base_url
            .join("api/chat")
            .expect("validated base URLs accept relative paths")
    }

    pub(crate) fn embedding_endpoint_url(&self) -> Url {
        self.base_url
            .join("api/embed")
            .expect("validated base URLs accept relative paths")
    }
}

impl fmt::Debug for OllamaConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OllamaConfig")
            .field("base_url", &self.base_url)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::OllamaConfig;

    #[test]
    fn local_config_targets_native_chat() {
        let config = OllamaConfig::local().unwrap();
        assert_eq!(
            config.endpoint_url().as_str(),
            "http://127.0.0.1:11434/api/chat"
        );
    }
}
