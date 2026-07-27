use std::fmt;

use secrecy::SecretString;
use thiserror::Error;
use url::Url;

/// Configuration error detected before creating an Anthropic client.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum AnthropicConfigError {
    /// The API key was empty.
    #[error("Anthropic API key cannot be empty")]
    EmptyApiKey,
    /// The API version was empty.
    #[error("Anthropic API version cannot be empty")]
    EmptyApiVersion,
    /// The default output limit was zero.
    #[error("Anthropic default max tokens must be greater than zero")]
    ZeroMaxTokens,
    /// The base URL could not be parsed.
    #[error("invalid Anthropic base URL: {0}")]
    InvalidBaseUrl(#[from] url::ParseError),
    /// The URL is not a hierarchical HTTP endpoint.
    #[error("Anthropic base URL must be an HTTP(S) base URL")]
    InvalidBaseUrlShape,
}

/// Explicit, secret-safe configuration for Anthropic's Messages API.
#[derive(Clone)]
pub struct AnthropicConfig {
    pub(crate) api_key: SecretString,
    pub(crate) base_url: Url,
    pub(crate) api_version: String,
    pub(crate) beta_features: Vec<String>,
    pub(crate) default_max_tokens: u64,
}

impl AnthropicConfig {
    /// Creates configuration for Anthropic's public API.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is blank.
    pub fn new(api_key: impl Into<String>) -> Result<Self, AnthropicConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(AnthropicConfigError::EmptyApiKey);
        }
        Ok(Self {
            api_key: SecretString::from(api_key),
            base_url: Url::parse("https://api.anthropic.com/v1/")?,
            api_version: "2023-06-01".into(),
            beta_features: Vec::new(),
            default_max_tokens: 1_024,
        })
    }

    /// Overrides the API base URL.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid, non-HTTP(S), or non-base URLs.
    pub fn with_base_url(mut self, base_url: &str) -> Result<Self, AnthropicConfigError> {
        let mut parsed = Url::parse(base_url)?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.cannot_be_a_base() {
            return Err(AnthropicConfigError::InvalidBaseUrlShape);
        }
        if !parsed.path().ends_with('/') {
            parsed.set_path(&format!("{}/", parsed.path()));
        }
        self.base_url = parsed;
        Ok(self)
    }

    /// Selects the Anthropic API version header.
    ///
    /// # Errors
    ///
    /// Returns an error when the version is blank.
    pub fn with_api_version(
        mut self,
        api_version: impl Into<String>,
    ) -> Result<Self, AnthropicConfigError> {
        let api_version = api_version.into();
        if api_version.trim().is_empty() {
            return Err(AnthropicConfigError::EmptyApiVersion);
        }
        self.api_version = api_version;
        Ok(self)
    }

    /// Enables an explicit Anthropic beta header value.
    #[must_use]
    pub fn with_beta(mut self, beta: impl Into<String>) -> Self {
        self.beta_features.push(beta.into());
        self
    }

    /// Sets the output limit used when a canonical request omits one.
    ///
    /// # Errors
    ///
    /// Returns an error when `max_tokens` is zero.
    pub fn with_default_max_tokens(
        mut self,
        max_tokens: u64,
    ) -> Result<Self, AnthropicConfigError> {
        if max_tokens == 0 {
            return Err(AnthropicConfigError::ZeroMaxTokens);
        }
        self.default_max_tokens = max_tokens;
        Ok(self)
    }

    pub(crate) fn endpoint_url(&self) -> Url {
        self.base_url
            .join("messages")
            .expect("validated base URLs accept relative paths")
    }
}

impl fmt::Debug for AnthropicConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicConfig")
            .field("api_key", &"[REDACTED]")
            .field("base_url", &self.base_url)
            .field("api_version", &self.api_version)
            .field("beta_features", &self.beta_features)
            .field("default_max_tokens", &self.default_max_tokens)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{AnthropicConfig, AnthropicConfigError};

    #[test]
    fn debug_output_redacts_the_key() {
        let config = AnthropicConfig::new("secret-value").unwrap();

        let debug = format!("{config:?}");

        assert!(!debug.contains("secret-value"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn base_url_is_normalized_for_messages() {
        let config = AnthropicConfig::new("key")
            .unwrap()
            .with_base_url("http://localhost:8080/v1")
            .unwrap();

        assert_eq!(
            config.endpoint_url().as_str(),
            "http://localhost:8080/v1/messages"
        );
    }

    #[test]
    fn invalid_limits_are_rejected() {
        let error = AnthropicConfig::new("key")
            .unwrap()
            .with_default_max_tokens(0)
            .unwrap_err();

        assert_eq!(error, AnthropicConfigError::ZeroMaxTokens);
    }
}
