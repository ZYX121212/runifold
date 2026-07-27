use std::fmt;

use secrecy::SecretString;
use thiserror::Error;
use url::Url;

/// OpenAI-derived HTTP wire protocol used by a provider endpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiWireProtocol {
    /// Typed `/responses` requests and semantic SSE events.
    #[default]
    Responses,
    /// Widely implemented `/chat/completions` requests and delta chunks.
    ChatCompletions,
}

/// Configuration error detected before creating an `OpenAI` client.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum OpenAiConfigError {
    /// The API key was empty.
    #[error("OpenAI API key cannot be empty")]
    EmptyApiKey,
    /// The canonical provider identity was empty.
    #[error("provider identity cannot be empty")]
    EmptyProvider,
    /// The base URL could not be parsed.
    #[error("invalid OpenAI base URL: {0}")]
    InvalidBaseUrl(#[from] url::ParseError),
    /// The base URL cannot be used as a hierarchical HTTP endpoint.
    #[error("OpenAI base URL must be an HTTP(S) base URL")]
    InvalidBaseUrlShape,
}

/// Explicit, secret-safe configuration for an OpenAI-compatible endpoint.
#[derive(Clone)]
pub struct OpenAiConfig {
    pub(crate) api_key: Option<SecretString>,
    pub(crate) base_url: Url,
    pub(crate) provider: String,
    pub(crate) wire_protocol: OpenAiWireProtocol,
    pub(crate) organization: Option<String>,
    pub(crate) project: Option<String>,
}

impl OpenAiConfig {
    /// Creates configuration for the public `OpenAI` v1 API.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::EmptyApiKey`] for a blank credential.
    pub fn new(api_key: impl Into<String>) -> Result<Self, OpenAiConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(OpenAiConfigError::EmptyApiKey);
        }
        let base_url = Url::parse("https://api.openai.com/v1/")?;
        Ok(Self {
            api_key: Some(SecretString::from(api_key)),
            base_url,
            provider: "openai".into(),
            wire_protocol: OpenAiWireProtocol::Responses,
            organization: None,
            project: None,
        })
    }

    /// Creates configuration for an OpenAI-compatible provider.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty provider, empty key, or invalid base URL.
    pub fn compatible(
        provider: impl Into<String>,
        api_key: impl Into<String>,
        base_url: &str,
        wire_protocol: OpenAiWireProtocol,
    ) -> Result<Self, OpenAiConfigError> {
        let provider = provider.into();
        if provider.trim().is_empty() {
            return Err(OpenAiConfigError::EmptyProvider);
        }
        Self::new(api_key)?.with_base_url(base_url).map(|config| {
            config
                .with_provider(provider)
                .with_wire_protocol(wire_protocol)
        })
    }

    /// Creates a custom endpoint without assuming Bearer authentication.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty provider or invalid base URL.
    pub fn custom(
        provider: impl Into<String>,
        base_url: &str,
        wire_protocol: OpenAiWireProtocol,
    ) -> Result<Self, OpenAiConfigError> {
        let provider = provider.into();
        if provider.trim().is_empty() {
            return Err(OpenAiConfigError::EmptyProvider);
        }
        let default_base_url = Url::parse("http://localhost/")?;
        Self {
            api_key: None,
            base_url: default_base_url,
            provider,
            wire_protocol,
            organization: None,
            project: None,
        }
        .with_base_url(base_url)
    }

    /// Creates a Volcengine Ark Responses configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty key.
    pub fn ark(api_key: impl Into<String>) -> Result<Self, OpenAiConfigError> {
        Self::compatible(
            "ark",
            api_key,
            "https://ark.cn-beijing.volces.com/api/v3/",
            OpenAiWireProtocol::Responses,
        )
    }

    /// Creates an Alibaba Model Studio configuration for a regional base URL.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty key or invalid regional base URL.
    pub fn qwen(
        api_key: impl Into<String>,
        base_url: &str,
        wire_protocol: OpenAiWireProtocol,
    ) -> Result<Self, OpenAiConfigError> {
        Self::compatible("qwen", api_key, base_url, wire_protocol)
    }

    /// Overrides the API base URL for an OpenAI-compatible service.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid, non-HTTP(S), or non-base URLs.
    pub fn with_base_url(mut self, base_url: &str) -> Result<Self, OpenAiConfigError> {
        let mut parsed = Url::parse(base_url)?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.cannot_be_a_base() {
            return Err(OpenAiConfigError::InvalidBaseUrlShape);
        }
        if !parsed.path().ends_with('/') {
            parsed.set_path(&format!("{}/", parsed.path()));
        }
        self.base_url = parsed;
        Ok(self)
    }

    /// Selects an `OpenAI` organization.
    #[must_use]
    pub fn with_organization(mut self, organization: impl Into<String>) -> Self {
        self.organization = Some(organization.into());
        self
    }

    /// Selects an `OpenAI` project.
    #[must_use]
    pub fn with_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    /// Overrides the canonical provider identity.
    #[must_use]
    pub fn with_provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self
    }

    /// Selects the endpoint wire protocol.
    #[must_use]
    pub const fn with_wire_protocol(mut self, wire_protocol: OpenAiWireProtocol) -> Self {
        self.wire_protocol = wire_protocol;
        self
    }

    pub(crate) fn endpoint_url(&self) -> Url {
        let path = match self.wire_protocol {
            OpenAiWireProtocol::Responses => "responses",
            OpenAiWireProtocol::ChatCompletions => "chat/completions",
        };
        self.base_url
            .join(path)
            .expect("validated base URLs accept relative paths")
    }
}

impl fmt::Debug for OpenAiConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiConfig")
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field("base_url", &self.base_url)
            .field("provider", &self.provider)
            .field("wire_protocol", &self.wire_protocol)
            .field("organization", &self.organization)
            .field("project", &self.project)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{OpenAiConfig, OpenAiConfigError, OpenAiWireProtocol};

    #[test]
    fn debug_output_never_contains_the_key() {
        let config = OpenAiConfig::new("super-secret-key").unwrap();
        let debug = format!("{config:?}");

        assert!(!debug.contains("super-secret-key"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn compatible_base_urls_are_normalized() {
        let config = OpenAiConfig::new("key")
            .unwrap()
            .with_base_url("http://localhost:8080/v1")
            .unwrap();

        assert_eq!(
            config.endpoint_url().as_str(),
            "http://localhost:8080/v1/responses"
        );
    }

    #[test]
    fn empty_keys_are_rejected() {
        assert_eq!(
            OpenAiConfig::new("  ").unwrap_err(),
            OpenAiConfigError::EmptyApiKey
        );
    }

    #[test]
    fn invalid_url_preserves_its_typed_source() {
        use std::error::Error as _;

        let error = OpenAiConfig::new("key")
            .unwrap()
            .with_base_url("not a URL")
            .unwrap_err();

        assert!(matches!(error, OpenAiConfigError::InvalidBaseUrl(_)));
        assert!(error.source().is_some());
    }

    #[test]
    fn ark_preset_uses_its_own_identity_and_responses_endpoint() {
        let config = OpenAiConfig::ark("key").unwrap();

        assert_eq!(config.provider, "ark");
        assert_eq!(config.wire_protocol, OpenAiWireProtocol::Responses);
        assert_eq!(
            config.endpoint_url().as_str(),
            "https://ark.cn-beijing.volces.com/api/v3/responses"
        );
    }

    #[test]
    fn qwen_can_select_chat_completions_per_endpoint() {
        let config = OpenAiConfig::qwen(
            "key",
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
            OpenAiWireProtocol::ChatCompletions,
        )
        .unwrap();

        assert_eq!(config.provider, "qwen");
        assert_eq!(
            config.endpoint_url().as_str(),
            "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
        );
    }

    #[test]
    fn custom_endpoints_can_omit_bearer_authentication() {
        let config = OpenAiConfig::custom(
            "local",
            "http://127.0.0.1:11434/v1",
            OpenAiWireProtocol::ChatCompletions,
        )
        .unwrap();

        assert!(config.api_key.is_none());
        assert_eq!(
            config.endpoint_url().as_str(),
            "http://127.0.0.1:11434/v1/chat/completions"
        );
    }
}
