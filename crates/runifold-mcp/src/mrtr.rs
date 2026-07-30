use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use runifold_core::{CancellationToken, RunContext};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{
    ClientCapabilities, CreateMessageParams, McpError, McpResultType, SamplingService,
    StatelessRequestMetadata,
};

const ALLOWED_INPUT_METHODS: &[&str] =
    &["sampling/createMessage", "elicitation/create", "roots/list"];

/// One server-originated request embedded in an MRTR incomplete result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InputRequest {
    /// Client feature requested by the server.
    pub method: String,
    /// Feature-specific request parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl InputRequest {
    /// Creates an input request with structured parameters.
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            method: method.into(),
            params,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), McpError> {
        if ALLOWED_INPUT_METHODS.contains(&self.method.as_str()) {
            Ok(())
        } else {
            Err(McpError::protocol(format!(
                "unsupported MRTR input request method `{}`",
                self.method
            )))
        }
    }
}

/// Incomplete result asking the client for additional input.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InputRequiredResult {
    /// Result discriminator required by the modern protocol.
    pub result_type: McpResultType,
    /// Independently keyed input requests.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_requests: BTreeMap<String, InputRequest>,
    /// Opaque state that must be echoed exactly on the next attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_state: Option<String>,
}

impl InputRequiredResult {
    /// Creates an incomplete result containing input requests.
    pub fn new(input_requests: BTreeMap<String, InputRequest>) -> Self {
        Self {
            result_type: McpResultType::InputRequired,
            input_requests,
            request_state: None,
        }
    }

    /// Attaches opaque server state for the next attempt.
    #[must_use]
    pub fn with_request_state(mut self, request_state: impl Into<String>) -> Self {
        self.request_state = Some(request_state.into());
        self
    }

    pub(crate) fn validate(&self, max_inputs: usize) -> Result<(), McpError> {
        if self.result_type != McpResultType::InputRequired {
            return Err(McpError::protocol(
                "MRTR incomplete result has the wrong resultType",
            ));
        }
        if self.input_requests.is_empty() && self.request_state.is_none() {
            return Err(McpError::protocol(
                "MRTR incomplete result has neither input requests nor request state",
            ));
        }
        if self.input_requests.len() > max_inputs {
            return Err(McpError::protocol(
                "MRTR input request count exceeds the configured limit",
            ));
        }
        for (key, request) in &self.input_requests {
            if key.is_empty() {
                return Err(McpError::protocol("MRTR input request key is empty"));
            }
            request.validate()?;
        }
        Ok(())
    }

    pub(crate) fn missing_capabilities(
        &self,
        capabilities: &ClientCapabilities,
    ) -> BTreeMap<String, Value> {
        let mut required = BTreeMap::new();
        for request in self.input_requests.values() {
            match request.method.as_str() {
                "sampling/createMessage" if capabilities.sampling.is_none() => {
                    required.insert("sampling".into(), serde_json::json!({}));
                }
                "elicitation/create" if capabilities.elicitation.is_none() => {
                    required.insert("elicitation".into(), serde_json::json!({}));
                }
                "roots/list" if capabilities.roots.is_none() => {
                    required.insert("roots".into(), serde_json::json!({}));
                }
                _ => {}
            }
        }
        required
    }
}

/// Future returned by an MRTR input handler.
pub type InputResponseFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Value, McpError>> + Send + 'a>>;

/// Host-controlled resolver for server-requested MRTR input.
pub trait MrtrInputHandler: Send + Sync + std::fmt::Debug {
    /// Declares the client features this handler can resolve.
    fn capabilities(&self) -> crate::ClientCapabilities {
        crate::ClientCapabilities::default()
    }

    /// Resolves one keyed request under the parent request's cancellation.
    fn handle(
        &self,
        key: String,
        request: InputRequest,
        cancellation: CancellationToken,
    ) -> InputResponseFuture<'_>;
}

impl MrtrInputHandler for SamplingService {
    fn capabilities(&self) -> crate::ClientCapabilities {
        crate::ClientCapabilities {
            sampling: Some(crate::SamplingCapability::default()),
            ..crate::ClientCapabilities::default()
        }
    }

    fn handle(
        &self,
        _key: String,
        request: InputRequest,
        cancellation: CancellationToken,
    ) -> InputResponseFuture<'_> {
        Box::pin(async move {
            if request.method != "sampling/createMessage" {
                return Err(McpError::protocol(
                    "the configured Sampling service cannot resolve this MRTR input method",
                ));
            }
            let params: CreateMessageParams =
                serde_json::from_value(request.params.unwrap_or(Value::Null))?;
            self.execute(params, cancellation)
                .await
                .map_err(|error| McpError::protocol(error.to_string()))
                .and_then(|result| serde_json::to_value(result).map_err(Into::into))
        })
    }
}

/// Inputs exposed to a Tool's stateless MRTR preflight.
#[derive(Clone, Debug)]
pub struct MrtrToolRequest {
    /// Tool name.
    pub name: String,
    /// Original Tool arguments.
    pub arguments: Map<String, Value>,
    /// Responses supplied for the latest incomplete result.
    pub input_responses: BTreeMap<String, Value>,
    /// Opaque state previously emitted by this server.
    pub request_state: Option<String>,
    /// Capability-attenuated execution authority.
    pub context: RunContext,
}

/// Decision returned by a Tool MRTR preflight.
#[derive(Clone, Debug, PartialEq)]
pub enum MrtrToolDecision {
    /// All required input is present; execute the canonical Tool once.
    Proceed,
    /// End this attempt and ask the client for more input.
    InputRequired(InputRequiredResult),
}

/// Future returned by a Tool MRTR preflight.
pub type MrtrToolFuture<'a> =
    Pin<Box<dyn Future<Output = Result<MrtrToolDecision, McpError>> + Send + 'a>>;

/// Stateless, replay-safe preflight run before a canonical Tool invocation.
pub trait MrtrToolGate: Send + Sync + std::fmt::Debug {
    /// Validates echoed state and decides whether the Tool may execute.
    fn evaluate(&self, request: MrtrToolRequest) -> MrtrToolFuture<'_>;
}

pub(crate) type MrtrToolGates = BTreeMap<String, Arc<dyn MrtrToolGate>>;

#[derive(Debug, Deserialize)]
pub(crate) struct MrtrCallToolParams {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) arguments: Option<Map<String, Value>>,
    #[serde(default, rename = "inputResponses")]
    pub(crate) input_responses: BTreeMap<String, Value>,
    #[serde(default, rename = "requestState")]
    pub(crate) request_state: Option<String>,
    #[serde(default, rename = "_meta")]
    pub(crate) metadata: Option<StatelessRequestMetadata>,
}
