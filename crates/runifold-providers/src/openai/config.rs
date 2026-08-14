//! Validated OpenAI-compatible client configuration.

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

/// Request-field dialect used by a Chat Completions endpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiChatDialect {
    /// Broad legacy-compatible fields implemented by most third-party servers.
    #[default]
    Compatible,
    /// Current public `OpenAI` Chat Completions fields.
    OpenAi,
}

/// Response-event validation dialect used by a Responses endpoint.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiResponsesDialect {
    /// Allows compatibility servers to omit fields that the public `OpenAI` API
    /// requires, while still validating those fields when present.
    #[default]
    Compatible,
    /// Enforces the current public `OpenAI` Responses event contract.
    OpenAi,
}

/// Azure `OpenAI` v1 API version selector.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum AzureOpenAiApiVersion {
    /// Stable v1 contract.
    #[default]
    V1,
    /// Preview additions on the v1 endpoint.
    Preview,
}

impl AzureOpenAiApiVersion {
    const fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::Preview => "preview",
        }
    }
}

/// Built-in endpoint profile for an OpenAI-compatible provider.
///
/// A profile fixes only transport identity, base URL, and wire protocol. Model
/// capabilities remain explicit because they can differ between models served
/// by the same provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiCompatibleProfile {
    /// Public `OpenAI` API using the Responses protocol.
    OpenAi,
    /// Volcengine Ark Responses API.
    Ark,
    /// Alibaba Model Studio international endpoint.
    QwenInternational,
    /// Alibaba Model Studio China endpoint.
    QwenChina,
    /// `DeepSeek`'s OpenAI-compatible endpoint.
    DeepSeek,
    /// `OpenRouter`'s multi-provider endpoint.
    OpenRouter,
    /// xAI's OpenAI-compatible endpoint.
    XAi,
    /// Groq's OpenAI-compatible endpoint.
    Groq,
    /// Mistral's OpenAI-compatible endpoint.
    Mistral,
    /// Together AI's OpenAI-compatible endpoint.
    Together,
    /// Perplexity's Sonar Chat Completions endpoint.
    Perplexity,
    /// `MiniMax`'s international endpoint.
    MiniMaxInternational,
    /// `MiniMax`'s China endpoint.
    MiniMaxChina,
    /// Zhipu AI's OpenAI-compatible endpoint.
    Zhipu,
    /// `SiliconFlow`'s OpenAI-compatible endpoint.
    SiliconFlow,
    /// Hugging Face Inference Providers router.
    HuggingFace,
}

impl OpenAiCompatibleProfile {
    /// Stable provider identity used in [`runifold_model::ModelRef`].
    pub const fn provider(self) -> &'static str {
        match self {
            Self::OpenAi => "openai",
            Self::Ark => "ark",
            Self::QwenInternational | Self::QwenChina => "qwen",
            Self::DeepSeek => "deepseek",
            Self::OpenRouter => "openrouter",
            Self::XAi => "xai",
            Self::Groq => "groq",
            Self::Mistral => "mistral",
            Self::Together => "together",
            Self::Perplexity => "perplexity",
            Self::MiniMaxInternational | Self::MiniMaxChina => "minimax",
            Self::Zhipu => "zhipu",
            Self::SiliconFlow => "siliconflow",
            Self::HuggingFace => "huggingface",
        }
    }

    /// Provider API base URL verified by its public compatibility contract.
    pub const fn base_url(self) -> &'static str {
        match self {
            Self::OpenAi => "https://api.openai.com/v1/",
            Self::Ark => "https://ark.cn-beijing.volces.com/api/v3/",
            Self::QwenInternational => "https://dashscope-intl.aliyuncs.com/compatible-mode/v1/",
            Self::QwenChina => "https://dashscope.aliyuncs.com/compatible-mode/v1/",
            Self::DeepSeek => "https://api.deepseek.com/",
            Self::OpenRouter => "https://openrouter.ai/api/v1/",
            Self::XAi => "https://api.x.ai/v1/",
            Self::Groq => "https://api.groq.com/openai/v1/",
            Self::Mistral => "https://api.mistral.ai/v1/",
            Self::Together => "https://api.together.ai/v1/",
            Self::Perplexity => "https://api.perplexity.ai/",
            Self::MiniMaxInternational => "https://api.minimax.io/v1/",
            Self::MiniMaxChina => "https://api.minimaxi.com/v1/",
            Self::Zhipu => "https://open.bigmodel.cn/api/paas/v4/",
            Self::SiliconFlow => "https://api.siliconflow.cn/v1/",
            Self::HuggingFace => "https://router.huggingface.co/v1/",
        }
    }

    /// Wire protocol supported by the built-in endpoint.
    pub const fn wire_protocol(self) -> OpenAiWireProtocol {
        match self {
            Self::OpenAi | Self::Ark => OpenAiWireProtocol::Responses,
            _ => OpenAiWireProtocol::ChatCompletions,
        }
    }

    const fn endpoint_path(self) -> Option<&'static str> {
        match self {
            Self::Perplexity => Some("v1/sonar"),
            _ => None,
        }
    }
}

/// Configuration error detected before creating an `OpenAI` client.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum OpenAiConfigError {
    /// The API key was empty.
    #[error("OpenAI API key cannot be empty")]
    EmptyApiKey,
    /// The bearer token was empty.
    #[error("Azure OpenAI bearer token cannot be empty")]
    EmptyBearerToken,
    /// The canonical provider identity was empty.
    #[error("provider identity cannot be empty")]
    EmptyProvider,
    /// The canonical provider identity was unsafe or ambiguous.
    #[error("provider identity must have no surrounding whitespace or control characters")]
    InvalidProvider,
    /// An embedding model identity was empty.
    #[error("OpenAI embedding model cannot be empty")]
    EmptyEmbeddingModel,
    /// The base URL could not be parsed.
    #[error("invalid OpenAI base URL: {0}")]
    InvalidBaseUrl(#[from] url::ParseError),
    /// The base URL cannot be used as a safe hierarchical HTTP endpoint.
    #[error("OpenAI base URL must be an HTTP(S) base URL without credentials, query, or fragment")]
    InvalidBaseUrlShape,
    /// `OpenRouter` attribution requires a public HTTP(S) application URL.
    #[error(
        "OpenRouter attribution URL must be an HTTP(S) URL without credentials, query, or fragment"
    )]
    InvalidAttributionUrl,
    /// `OpenRouter` attribution title was empty or unsafe for an HTTP header.
    #[error("OpenRouter application title must be non-empty and contain no control characters")]
    InvalidApplicationTitle,
    /// The organization identifier was empty or unsafe for an HTTP header.
    #[error("OpenAI organization must be non-empty and contain no control characters")]
    InvalidOrganization,
    /// The project identifier was empty or unsafe for an HTTP header.
    #[error("OpenAI project must be non-empty and contain no control characters")]
    InvalidProject,
}

/// Explicit, secret-safe configuration for an OpenAI-compatible endpoint.
#[derive(Clone)]
pub struct OpenAiConfig {
    pub(crate) api_key: Option<SecretString>,
    pub(crate) azure_api_key: Option<SecretString>,
    pub(crate) azure_api_version: Option<AzureOpenAiApiVersion>,
    pub(crate) base_url: Url,
    pub(crate) provider: String,
    pub(crate) wire_protocol: OpenAiWireProtocol,
    pub(crate) chat_dialect: OpenAiChatDialect,
    pub(crate) responses_dialect: OpenAiResponsesDialect,
    endpoint_path_override: Option<String>,
    pub(crate) organization: Option<String>,
    pub(crate) project: Option<String>,
    pub(crate) application_url: Option<Url>,
    pub(crate) application_title: Option<String>,
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
            azure_api_key: None,
            azure_api_version: None,
            base_url,
            provider: "openai".into(),
            wire_protocol: OpenAiWireProtocol::Responses,
            chat_dialect: OpenAiChatDialect::OpenAi,
            responses_dialect: OpenAiResponsesDialect::OpenAi,
            endpoint_path_override: None,
            organization: None,
            project: None,
            application_url: None,
            application_title: None,
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
        validate_provider(&provider)?;
        Self::new(api_key)?
            .with_base_url(base_url)
            .map(|mut config| {
                config.provider = provider;
                config.wire_protocol = wire_protocol;
                config.chat_dialect = OpenAiChatDialect::Compatible;
                config.responses_dialect = OpenAiResponsesDialect::Compatible;
                config
            })
    }

    /// Creates configuration from a verified built-in provider profile.
    ///
    /// # Errors
    ///
    /// Returns an error when the API key is blank or the built-in URL cannot
    /// be constructed.
    pub fn from_profile(
        profile: OpenAiCompatibleProfile,
        api_key: impl Into<String>,
    ) -> Result<Self, OpenAiConfigError> {
        Self::compatible(
            profile.provider(),
            api_key,
            profile.base_url(),
            profile.wire_protocol(),
        )
        .map(|mut config| {
            config.endpoint_path_override = profile.endpoint_path().map(String::from);
            config.chat_dialect = if profile == OpenAiCompatibleProfile::OpenAi {
                OpenAiChatDialect::OpenAi
            } else {
                OpenAiChatDialect::Compatible
            };
            config.responses_dialect = if profile == OpenAiCompatibleProfile::OpenAi {
                OpenAiResponsesDialect::OpenAi
            } else {
                OpenAiResponsesDialect::Compatible
            };
            config
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
        validate_provider(&provider)?;
        let default_base_url = Url::parse("http://localhost/")?;
        Self {
            api_key: None,
            azure_api_key: None,
            azure_api_version: None,
            base_url: default_base_url,
            provider,
            wire_protocol,
            chat_dialect: OpenAiChatDialect::Compatible,
            responses_dialect: OpenAiResponsesDialect::Compatible,
            endpoint_path_override: None,
            organization: None,
            project: None,
            application_url: None,
            application_title: None,
        }
        .with_base_url(base_url)
    }

    /// Creates a Volcengine Ark Responses configuration.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty key.
    pub fn ark(api_key: impl Into<String>) -> Result<Self, OpenAiConfigError> {
        Self::from_profile(OpenAiCompatibleProfile::Ark, api_key)
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

    /// Creates an Azure `OpenAI` v1 Responses configuration using an API key.
    ///
    /// `resource_endpoint` is the resource or Foundry project endpoint before
    /// the `/openai/v1/` suffix.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty key or invalid resource endpoint.
    pub fn azure_api_key(
        resource_endpoint: &str,
        api_key: impl Into<String>,
    ) -> Result<Self, OpenAiConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(OpenAiConfigError::EmptyApiKey);
        }
        let base_url = azure_base_url(resource_endpoint)?;
        let mut config = Self::custom(
            "azure-openai",
            base_url.as_str(),
            OpenAiWireProtocol::Responses,
        )?;
        config.azure_api_key = Some(SecretString::from(api_key));
        Ok(config)
    }

    /// Creates an Azure `OpenAI` v1 Responses configuration using an Entra
    /// bearer token.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty token or invalid resource endpoint.
    pub fn azure_bearer_token(
        resource_endpoint: &str,
        token: impl Into<String>,
    ) -> Result<Self, OpenAiConfigError> {
        let token = token.into();
        if token.trim().is_empty() {
            return Err(OpenAiConfigError::EmptyBearerToken);
        }
        let base_url = azure_base_url(resource_endpoint)?;
        Self::compatible(
            "azure-openai",
            token,
            base_url.as_str(),
            OpenAiWireProtocol::Responses,
        )
    }

    /// Selects an explicit Azure `OpenAI` v1 API version query.
    #[must_use]
    pub const fn with_azure_api_version(mut self, version: AzureOpenAiApiVersion) -> Self {
        self.azure_api_version = Some(version);
        self
    }

    /// Overrides the API base URL for an OpenAI-compatible service.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid, non-HTTP(S), or non-base URLs.
    pub fn with_base_url(mut self, base_url: &str) -> Result<Self, OpenAiConfigError> {
        let mut parsed = Url::parse(base_url)?;
        if !is_safe_public_http_url(&parsed, true) {
            return Err(OpenAiConfigError::InvalidBaseUrlShape);
        }
        if !parsed.path().ends_with('/') {
            parsed.set_path(&format!("{}/", parsed.path()));
        }
        self.base_url = parsed;
        Ok(self)
    }

    /// Selects an `OpenAI` organization.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot safely become an HTTP header.
    pub fn with_organization(
        mut self,
        organization: impl Into<String>,
    ) -> Result<Self, OpenAiConfigError> {
        let organization = organization.into();
        if !is_safe_header_value(&organization) {
            return Err(OpenAiConfigError::InvalidOrganization);
        }
        self.organization = Some(organization);
        Ok(self)
    }

    /// Selects an `OpenAI` project.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot safely become an HTTP header.
    pub fn with_project(mut self, project: impl Into<String>) -> Result<Self, OpenAiConfigError> {
        let project = project.into();
        if !is_safe_header_value(&project) {
            return Err(OpenAiConfigError::InvalidProject);
        }
        self.project = Some(project);
        Ok(self)
    }

    /// Adds the optional application attribution recommended by `OpenRouter`.
    ///
    /// The values are also safe to retain on cloned clients: the URL and title
    /// are public metadata rather than credentials.
    ///
    /// # Errors
    ///
    /// Returns an error for a non-HTTP(S) URL, blank title, or title containing
    /// control characters that cannot safely become a request header.
    pub fn with_openrouter_attribution(
        mut self,
        application_url: &str,
        application_title: impl Into<String>,
    ) -> Result<Self, OpenAiConfigError> {
        let application_url =
            Url::parse(application_url).map_err(|_| OpenAiConfigError::InvalidAttributionUrl)?;
        if !is_safe_public_http_url(&application_url, false) {
            return Err(OpenAiConfigError::InvalidAttributionUrl);
        }
        let application_title = application_title.into();
        if application_title.trim().is_empty() || application_title.chars().any(char::is_control) {
            return Err(OpenAiConfigError::InvalidApplicationTitle);
        }
        self.application_url = Some(application_url);
        self.application_title = Some(application_title);
        Ok(self)
    }

    /// Overrides the canonical provider identity.
    ///
    /// # Errors
    ///
    /// Returns an error for a blank, whitespace-padded, or control-bearing
    /// identity.
    pub fn with_provider(mut self, provider: impl Into<String>) -> Result<Self, OpenAiConfigError> {
        let provider = provider.into();
        validate_provider(&provider)?;
        self.provider = provider;
        Ok(self)
    }

    /// Selects the endpoint wire protocol.
    #[must_use]
    pub const fn with_wire_protocol(mut self, wire_protocol: OpenAiWireProtocol) -> Self {
        self.wire_protocol = wire_protocol;
        self
    }

    /// Selects the Chat Completions request-field dialect explicitly.
    #[must_use]
    pub const fn with_chat_dialect(mut self, chat_dialect: OpenAiChatDialect) -> Self {
        self.chat_dialect = chat_dialect;
        self
    }

    /// Selects the Responses event-validation dialect explicitly.
    #[must_use]
    pub const fn with_responses_dialect(
        mut self,
        responses_dialect: OpenAiResponsesDialect,
    ) -> Self {
        self.responses_dialect = responses_dialect;
        self
    }

    pub(crate) fn endpoint_url(&self) -> Url {
        let path = self
            .endpoint_path_override
            .as_deref()
            .unwrap_or(match self.wire_protocol {
                OpenAiWireProtocol::Responses => "responses",
                OpenAiWireProtocol::ChatCompletions => "chat/completions",
            });
        let mut endpoint = self
            .base_url
            .join(path)
            .expect("validated base URLs accept relative paths");
        self.append_api_version(&mut endpoint);
        endpoint
    }

    pub(crate) fn embedding_endpoint_url(&self) -> Url {
        let mut endpoint = self
            .base_url
            .join("embeddings")
            .expect("validated base URLs accept relative paths");
        self.append_api_version(&mut endpoint);
        endpoint
    }

    pub(crate) fn control_endpoint_url(&self, path: &str) -> Url {
        let mut endpoint = self
            .base_url
            .join(path)
            .expect("validated base URLs accept relative control-plane paths");
        self.append_api_version(&mut endpoint);
        endpoint
    }

    fn append_api_version(&self, endpoint: &mut Url) {
        if let Some(version) = self.azure_api_version {
            endpoint
                .query_pairs_mut()
                .append_pair("api-version", version.as_str());
        }
    }
}

fn azure_base_url(resource_endpoint: &str) -> Result<Url, OpenAiConfigError> {
    let mut endpoint = Url::parse(resource_endpoint)?;
    if !is_safe_public_http_url(&endpoint, true) {
        return Err(OpenAiConfigError::InvalidBaseUrlShape);
    }
    if !endpoint.path().ends_with('/') {
        endpoint.set_path(&format!("{}/", endpoint.path()));
    }
    endpoint
        .join("openai/v1/")
        .map_err(OpenAiConfigError::InvalidBaseUrl)
}

fn validate_provider(provider: &str) -> Result<(), OpenAiConfigError> {
    if provider.trim().is_empty() {
        return Err(OpenAiConfigError::EmptyProvider);
    }
    if provider.trim() != provider || provider.chars().any(char::is_control) {
        return Err(OpenAiConfigError::InvalidProvider);
    }
    Ok(())
}

fn is_safe_header_value(value: &str) -> bool {
    !value.trim().is_empty() && !value.chars().any(char::is_control)
}

fn is_safe_public_http_url(url: &Url, base_url: bool) -> bool {
    matches!(url.scheme(), "http" | "https")
        && url.host_str().is_some()
        && (!base_url || !url.cannot_be_a_base())
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}

fn debug_url(url: &Url) -> String {
    let mut sanitized = url.clone();
    let _ = sanitized.set_username("");
    let _ = sanitized.set_password(None);
    sanitized.set_query(None);
    sanitized.set_fragment(None);
    sanitized.to_string()
}

impl fmt::Debug for OpenAiConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiConfig")
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .field(
                "azure_api_key",
                &self.azure_api_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("azure_api_version", &self.azure_api_version)
            .field("base_url", &debug_url(&self.base_url))
            .field("provider", &self.provider)
            .field("wire_protocol", &self.wire_protocol)
            .field("chat_dialect", &self.chat_dialect)
            .field("responses_dialect", &self.responses_dialect)
            .field("endpoint_path_override", &self.endpoint_path_override)
            .field("organization", &self.organization)
            .field("project", &self.project)
            .field(
                "application_url",
                &self.application_url.as_ref().map(debug_url),
            )
            .field("application_title", &self.application_title)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AzureOpenAiApiVersion, OpenAiChatDialect, OpenAiCompatibleProfile, OpenAiConfig,
        OpenAiConfigError, OpenAiResponsesDialect, OpenAiWireProtocol,
    };

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
    fn chat_dialect_follows_verified_endpoint_ownership() {
        let official = OpenAiConfig::from_profile(OpenAiCompatibleProfile::OpenAi, "key").unwrap();
        assert_eq!(official.chat_dialect, OpenAiChatDialect::OpenAi);
        assert_eq!(official.responses_dialect, OpenAiResponsesDialect::OpenAi);

        let compatible = OpenAiConfig::compatible(
            "gateway",
            "key",
            "https://example.test/v1",
            OpenAiWireProtocol::ChatCompletions,
        )
        .unwrap();
        assert_eq!(compatible.chat_dialect, OpenAiChatDialect::Compatible);
        assert_eq!(
            compatible.responses_dialect,
            OpenAiResponsesDialect::Compatible
        );
    }

    #[test]
    fn unsafe_urls_and_header_values_are_rejected_before_transport() {
        for base_url in [
            "https://user:secret@example.test/v1",
            "https://example.test/v1?token=secret",
            "https://example.test/v1#fragment",
        ] {
            assert_eq!(
                OpenAiConfig::new("key")
                    .unwrap()
                    .with_base_url(base_url)
                    .unwrap_err(),
                OpenAiConfigError::InvalidBaseUrlShape
            );
        }

        assert_eq!(
            OpenAiConfig::new("key")
                .unwrap()
                .with_organization("org\nunsafe")
                .unwrap_err(),
            OpenAiConfigError::InvalidOrganization
        );
        assert_eq!(
            OpenAiConfig::new("key")
                .unwrap()
                .with_project(" ")
                .unwrap_err(),
            OpenAiConfigError::InvalidProject
        );
        assert_eq!(
            OpenAiConfig::new("key")
                .unwrap()
                .with_provider(" openai")
                .unwrap_err(),
            OpenAiConfigError::InvalidProvider
        );
    }

    #[test]
    fn debug_url_defensively_removes_credentials_and_query() {
        let unsafe_url =
            url::Url::parse("https://user:secret@example.test/v1?token=secret").unwrap();
        let safe = super::debug_url(&unsafe_url);

        assert_eq!(safe, "https://example.test/v1");
        assert!(!safe.contains("secret"));
    }

    #[test]
    fn empty_keys_are_rejected() {
        assert_eq!(
            OpenAiConfig::new("  ").unwrap_err(),
            OpenAiConfigError::EmptyApiKey
        );
        assert_eq!(
            OpenAiConfig::azure_bearer_token("https://resource.openai.azure.com", " ").unwrap_err(),
            OpenAiConfigError::EmptyBearerToken
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
        assert!(config.azure_api_key.is_none());
        assert_eq!(
            config.endpoint_url().as_str(),
            "http://127.0.0.1:11434/v1/chat/completions"
        );
    }

    #[test]
    fn azure_v1_uses_resource_path_api_key_and_explicit_preview() {
        let config =
            OpenAiConfig::azure_api_key("https://resource.openai.azure.com", "azure-secret")
                .unwrap()
                .with_azure_api_version(AzureOpenAiApiVersion::Preview);

        assert_eq!(config.provider, "azure-openai");
        assert!(config.api_key.is_none());
        assert!(config.azure_api_key.is_some());
        assert_eq!(
            config.endpoint_url().as_str(),
            "https://resource.openai.azure.com/openai/v1/responses?api-version=preview"
        );
        let debug = format!("{config:?}");
        assert!(!debug.contains("azure-secret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn azure_entra_uses_bearer_authentication_without_legacy_query() {
        let config =
            OpenAiConfig::azure_bearer_token("https://resource.openai.azure.com/", "entra-token")
                .unwrap();

        assert!(config.api_key.is_some());
        assert!(config.azure_api_key.is_none());
        assert_eq!(
            config.endpoint_url().as_str(),
            "https://resource.openai.azure.com/openai/v1/responses"
        );
    }

    #[test]
    fn openrouter_attribution_is_validated_before_transport() {
        let config = OpenAiConfig::from_profile(OpenAiCompatibleProfile::OpenRouter, "key")
            .unwrap()
            .with_openrouter_attribution("https://example.com/app", "Runifold example")
            .unwrap();

        assert_eq!(
            config.application_url.as_ref().map(url::Url::as_str),
            Some("https://example.com/app")
        );
        assert_eq!(
            config.application_title.as_deref(),
            Some("Runifold example")
        );
        assert_eq!(
            OpenAiConfig::from_profile(OpenAiCompatibleProfile::OpenRouter, "key")
                .unwrap()
                .with_openrouter_attribution("file:///tmp/app", "app")
                .unwrap_err(),
            OpenAiConfigError::InvalidAttributionUrl
        );
        assert_eq!(
            OpenAiConfig::from_profile(OpenAiCompatibleProfile::OpenRouter, "key")
                .unwrap()
                .with_openrouter_attribution("https://user:secret@example.com/app", "app")
                .unwrap_err(),
            OpenAiConfigError::InvalidAttributionUrl
        );
    }

    #[test]
    fn built_in_profiles_have_stable_identity_protocol_and_endpoint() {
        let cases = [
            (
                OpenAiCompatibleProfile::OpenAi,
                "openai",
                "https://api.openai.com/v1/responses",
            ),
            (
                OpenAiCompatibleProfile::Ark,
                "ark",
                "https://ark.cn-beijing.volces.com/api/v3/responses",
            ),
            (
                OpenAiCompatibleProfile::QwenInternational,
                "qwen",
                "https://dashscope-intl.aliyuncs.com/compatible-mode/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::QwenChina,
                "qwen",
                "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::DeepSeek,
                "deepseek",
                "https://api.deepseek.com/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::OpenRouter,
                "openrouter",
                "https://openrouter.ai/api/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::XAi,
                "xai",
                "https://api.x.ai/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::Groq,
                "groq",
                "https://api.groq.com/openai/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::Mistral,
                "mistral",
                "https://api.mistral.ai/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::Together,
                "together",
                "https://api.together.ai/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::Perplexity,
                "perplexity",
                "https://api.perplexity.ai/v1/sonar",
            ),
            (
                OpenAiCompatibleProfile::MiniMaxInternational,
                "minimax",
                "https://api.minimax.io/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::MiniMaxChina,
                "minimax",
                "https://api.minimaxi.com/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::Zhipu,
                "zhipu",
                "https://open.bigmodel.cn/api/paas/v4/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::SiliconFlow,
                "siliconflow",
                "https://api.siliconflow.cn/v1/chat/completions",
            ),
            (
                OpenAiCompatibleProfile::HuggingFace,
                "huggingface",
                "https://router.huggingface.co/v1/chat/completions",
            ),
        ];

        for (profile, provider, endpoint) in cases {
            let config = OpenAiConfig::from_profile(profile, "key").unwrap();
            assert_eq!(config.provider, provider);
            assert_eq!(config.wire_protocol, profile.wire_protocol());
            assert_eq!(config.endpoint_url().as_str(), endpoint);
        }
    }
}
