use std::fmt;

use secrecy::SecretString;
use thiserror::Error;
use url::Url;

/// Invalid Gemini client configuration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum GeminiConfigError {
    /// The API key was blank.
    #[error("Gemini API key cannot be empty")]
    EmptyApiKey,
    /// The base URL was invalid.
    #[error("invalid Gemini base URL: {0}")]
    InvalidBaseUrl(#[from] url::ParseError),
    /// The URL was not a hierarchical HTTP endpoint.
    #[error("Gemini base URL must be an HTTP(S) base URL")]
    InvalidBaseUrlShape,
}

/// Secret-safe Gemini `GenerateContent` configuration.
#[derive(Clone)]
pub struct GeminiConfig {
    pub(crate) api_key: SecretString,
    pub(crate) base_url: Url,
}

impl GeminiConfig {
    /// Creates public Gemini API configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for a blank API key.
    pub fn new(api_key: impl Into<String>) -> Result<Self, GeminiConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(GeminiConfigError::EmptyApiKey);
        }
        Ok(Self {
            api_key: SecretString::from(api_key),
            base_url: Url::parse("https://generativelanguage.googleapis.com/v1beta/")?,
        })
    }

    /// Overrides the API base URL.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid or non-HTTP(S) base URL.
    pub fn with_base_url(mut self, base_url: &str) -> Result<Self, GeminiConfigError> {
        let mut parsed = Url::parse(base_url)?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.cannot_be_a_base() {
            return Err(GeminiConfigError::InvalidBaseUrlShape);
        }
        if !parsed.path().ends_with('/') {
            parsed.set_path(&format!("{}/", parsed.path()));
        }
        self.base_url = parsed;
        Ok(self)
    }

    pub(crate) fn endpoint_url(&self, model: &str) -> Result<Url, url::ParseError> {
        let model = model.strip_prefix("models/").unwrap_or(model);
        let mut endpoint = self
            .base_url
            .join(&format!("models/{model}:streamGenerateContent"))?;
        endpoint.query_pairs_mut().append_pair("alt", "sse");
        Ok(endpoint)
    }
}

impl fmt::Debug for GeminiConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeminiConfig")
            .field("api_key", &"[REDACTED]")
            .field("base_url", &self.base_url)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::GeminiConfig;

    #[test]
    fn endpoint_uses_native_stream_method() {
        let config = GeminiConfig::new("super-secret-value")
            .unwrap()
            .with_base_url("http://localhost/v1beta")
            .unwrap();

        assert_eq!(
            config.endpoint_url("models/gemini-test").unwrap().as_str(),
            "http://localhost/v1beta/models/gemini-test:streamGenerateContent?alt=sse"
        );
        assert!(!format!("{config:?}").contains("super-secret-value"));
    }
}
