//! Named endpoint profiles sharing the OpenAI-compatible adapter.

/// Azure `OpenAI` v1 Responses adapter.
pub mod azure {
    pub use crate::openai::{
        AzureOpenAiApiVersion, OpenAiClient as AzureOpenAiClient, OpenAiConfig, OpenAiConfigError,
        OpenAiEmbeddingModel as AzureOpenAiEmbeddingModel,
    };

    /// Creates an Azure `OpenAI` client using the resource API key.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] for a blank key or invalid endpoint.
    pub fn api_key_client(
        resource_endpoint: &str,
        api_key: impl Into<String>,
    ) -> Result<AzureOpenAiClient, OpenAiConfigError> {
        OpenAiConfig::azure_api_key(resource_endpoint, api_key).map(AzureOpenAiClient::new)
    }

    /// Creates an Azure `OpenAI` client using an application-provided Entra
    /// bearer token.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] for a blank token or invalid endpoint.
    pub fn entra_client(
        resource_endpoint: &str,
        token: impl Into<String>,
    ) -> Result<AzureOpenAiClient, OpenAiConfigError> {
        OpenAiConfig::azure_bearer_token(resource_endpoint, token).map(AzureOpenAiClient::new)
    }
}

macro_rules! compatible_provider_module {
    ($(#[$meta:meta])* $module:ident, $client:ident, $profile:ident) => {
        $(#[$meta])*
        pub mod $module {
            pub use crate::openai::{
                OpenAiClient as $client, OpenAiConfigError, OpenAiEmbeddingModel,
                OpenAiWireProtocol,
            };

            use crate::openai::OpenAiCompatibleProfile;

            /// Creates a client using the provider's verified public endpoint.
            ///
            /// # Errors
            ///
            /// Returns [`OpenAiConfigError`] when the API key is blank.
            pub fn client(api_key: impl Into<String>) -> Result<$client, OpenAiConfigError> {
                $client::from_profile(OpenAiCompatibleProfile::$profile, api_key)
            }
        }
    };
}

/// Volcengine Ark Responses API adapter.
pub mod ark {
    pub use crate::openai::{
        ArkWebSearchTool, OpenAiClient as ArkClient, OpenAiConfigError, OpenAiEmbeddingModel,
        OpenAiFile, OpenAiFileDeletion, OpenAiFilePurpose, OpenAiFileStatus, OpenAiFileUpload,
        OpenAiFileWaitPolicy, OpenAiWireProtocol,
    };

    /// Creates a client using Ark's verified public Responses endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] when the API key is blank.
    pub fn client(api_key: impl Into<String>) -> Result<ArkClient, OpenAiConfigError> {
        ArkClient::from_profile(crate::openai::OpenAiCompatibleProfile::Ark, api_key)
    }
}

compatible_provider_module!(
    /// `DeepSeek` Chat Completions API adapter.
    deepseek,
    DeepSeekClient,
    DeepSeek
);
compatible_provider_module!(
    /// Groq Chat Completions API adapter.
    groq,
    GroqClient,
    Groq
);
compatible_provider_module!(
    /// Mistral Chat Completions API adapter.
    mistral,
    MistralClient,
    Mistral
);
compatible_provider_module!(
    /// `OpenRouter` multi-provider Chat Completions adapter.
    openrouter,
    OpenRouterClient,
    OpenRouter
);
compatible_provider_module!(
    /// Perplexity Sonar Chat Completions adapter.
    perplexity,
    PerplexityClient,
    Perplexity
);
compatible_provider_module!(
    /// Together AI Chat Completions adapter.
    together,
    TogetherClient,
    Together
);
compatible_provider_module!(
    /// `SiliconFlow` Chat Completions adapter.
    siliconflow,
    SiliconFlowClient,
    SiliconFlow
);
compatible_provider_module!(
    /// xAI Chat Completions adapter.
    xai,
    XAiClient,
    XAi
);
compatible_provider_module!(
    /// Zhipu AI Chat Completions adapter.
    zhipu,
    ZhipuClient,
    Zhipu
);
compatible_provider_module!(
    /// Hugging Face Inference Providers Chat Completions router.
    huggingface,
    HuggingFaceClient,
    HuggingFace
);

/// Alibaba Model Studio OpenAI-compatible adapter.
pub mod qwen {
    pub use crate::openai::{
        OpenAiClient as QwenClient, OpenAiConfigError, OpenAiEmbeddingModel, OpenAiWireProtocol,
    };

    use crate::openai::OpenAiCompatibleProfile;

    /// Alibaba Model Studio endpoint region.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub enum QwenRegion {
        /// International endpoint hosted in Singapore.
        #[default]
        International,
        /// Mainland China endpoint hosted in Beijing.
        China,
    }

    /// Creates a client using the selected regional endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] when the API key is blank.
    pub fn client(
        region: QwenRegion,
        api_key: impl Into<String>,
    ) -> Result<QwenClient, OpenAiConfigError> {
        let profile = match region {
            QwenRegion::International => OpenAiCompatibleProfile::QwenInternational,
            QwenRegion::China => OpenAiCompatibleProfile::QwenChina,
        };
        QwenClient::from_profile(profile, api_key)
    }
}

/// `MiniMax` OpenAI-compatible adapter.
pub mod minimax {
    pub use crate::openai::{
        OpenAiClient as MiniMaxClient, OpenAiConfigError, OpenAiEmbeddingModel, OpenAiWireProtocol,
    };

    use crate::openai::OpenAiCompatibleProfile;

    /// `MiniMax` endpoint region.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub enum MiniMaxRegion {
        /// International endpoint.
        #[default]
        International,
        /// Mainland China endpoint.
        China,
    }

    /// Creates a client using the selected regional endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] when the API key is blank.
    pub fn client(
        region: MiniMaxRegion,
        api_key: impl Into<String>,
    ) -> Result<MiniMaxClient, OpenAiConfigError> {
        let profile = match region {
            MiniMaxRegion::International => OpenAiCompatibleProfile::MiniMaxInternational,
            MiniMaxRegion::China => OpenAiCompatibleProfile::MiniMaxChina,
        };
        MiniMaxClient::from_profile(profile, api_key)
    }
}

macro_rules! local_compatible_module {
    ($(#[$meta:meta])* $module:ident, $client:ident, $provider:literal) => {
        $(#[$meta])*
        pub mod $module {
            pub use crate::openai::{
                OpenAiClient as $client, OpenAiConfigError, OpenAiEmbeddingModel,
                OpenAiWireProtocol,
            };

            use crate::openai::OpenAiConfig;

            /// Connects to a credential-free application-owned endpoint.
            ///
            /// # Errors
            ///
            /// Returns [`OpenAiConfigError`] for an invalid endpoint.
            pub fn client(
                base_url: &str,
                wire_protocol: OpenAiWireProtocol,
            ) -> Result<$client, OpenAiConfigError> {
                OpenAiConfig::custom($provider, base_url, wire_protocol).map($client::new)
            }

            /// Connects to an endpoint protected by one bearer token.
            ///
            /// # Errors
            ///
            /// Returns [`OpenAiConfigError`] for a blank token or invalid endpoint.
            pub fn authenticated_client(
                base_url: &str,
                token: impl Into<String>,
                wire_protocol: OpenAiWireProtocol,
            ) -> Result<$client, OpenAiConfigError> {
                OpenAiConfig::compatible($provider, token, base_url, wire_protocol)
                    .map($client::new)
            }
        }
    };
}

local_compatible_module!(
    /// Application-owned vLLM OpenAI-compatible server.
    vllm,
    VllmClient,
    "vllm"
);
local_compatible_module!(
    /// Application-owned `llama.cpp` OpenAI-inspired server.
    llama_cpp,
    LlamaCppClient,
    "llama.cpp"
);
local_compatible_module!(
    /// Application-owned llamafile OpenAI-inspired server.
    llamafile,
    LlamafileClient,
    "llamafile"
);

#[cfg(test)]
mod tests {
    #[test]
    fn named_profiles_preserve_canonical_provider_identity() {
        assert_eq!(super::ark::client("key").unwrap().provider(), "ark");
        assert_eq!(
            super::deepseek::client("key").unwrap().provider(),
            "deepseek"
        );
        assert_eq!(super::xai::client("key").unwrap().provider(), "xai");
        assert_eq!(
            super::huggingface::client("key").unwrap().provider(),
            "huggingface"
        );
        assert_eq!(
            super::vllm::client(
                "http://127.0.0.1:8000/v1/",
                crate::openai::OpenAiWireProtocol::Responses,
            )
            .unwrap()
            .provider(),
            "vllm"
        );
    }
}
