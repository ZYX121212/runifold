use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use base64::Engine;
use runifold_core::CancellationToken;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::{ContentBlock, McpTool};

const DEFAULT_MAX_SERIALIZED_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_MEDIA_BYTES: usize = 4 * 1024 * 1024;

/// MCP Sampling message role.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SamplingRole {
    /// Human or Tool-result input.
    User,
    /// Model output.
    Assistant,
}

/// One or many Sampling content blocks.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum SamplingContent {
    /// One content block.
    One(ContentBlock),
    /// Multiple ordered content blocks.
    Many(Vec<ContentBlock>),
}

impl SamplingContent {
    /// Returns all content blocks as a slice.
    pub fn as_slice(&self) -> &[ContentBlock] {
        match self {
            Self::One(content) => std::slice::from_ref(content),
            Self::Many(contents) => contents,
        }
    }

    /// Creates a single text block.
    pub fn text(text: impl Into<String>) -> Self {
        Self::One(ContentBlock::text(text))
    }
}

/// One Sampling conversation message.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SamplingMessage {
    /// Message author.
    pub role: SamplingRole,
    /// Text, image, or audio content.
    pub content: SamplingContent,
    /// Protocol extension metadata.
    #[serde(rename = "_meta", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, Value>,
}

impl SamplingMessage {
    /// Creates one user text message.
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            role: SamplingRole::User,
            content: SamplingContent::text(text),
            meta: BTreeMap::new(),
        }
    }
}

/// Advisory model-name hint.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelHint {
    /// Model or model-family substring.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Advisory server preferences for client-side model selection.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelPreferences {
    /// Ordered model hints.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<ModelHint>,
    /// Importance of minimizing cost, from 0 through 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_priority: Option<f64>,
    /// Importance of minimizing latency, from 0 through 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed_priority: Option<f64>,
    /// Importance of model capability, from 0 through 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intelligence_priority: Option<f64>,
}

/// Deprecated context-inclusion request.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum IncludeContext {
    /// Include no ambient MCP context.
    #[default]
    None,
    /// Include context from the requesting server.
    ThisServer,
    /// Include context from every connected server.
    AllServers,
}

/// Sampling Tool-choice mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SamplingToolChoiceMode {
    /// The model decides.
    #[default]
    Auto,
    /// At least one Tool must be used.
    Required,
    /// Tools must not be used.
    None,
}

/// Sampling Tool-choice request.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SamplingToolChoice {
    /// Requested Tool behavior.
    #[serde(default)]
    pub mode: SamplingToolChoiceMode,
}

/// Parameters for `sampling/createMessage`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessageParams {
    /// Ordered input conversation.
    pub messages: Vec<SamplingMessage>,
    /// Advisory model preferences.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_preferences: Option<ModelPreferences>,
    /// Server-proposed system prompt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    /// Deprecated ambient-context request.
    #[serde(default, skip_serializing_if = "is_no_context")]
    pub include_context: IncludeContext,
    /// Sampling temperature.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// Maximum requested output tokens.
    pub max_tokens: u64,
    /// Stop sequences.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop_sequences: Vec<String>,
    /// Provider-specific metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// Tools available to the sampled model.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<McpTool>,
    /// Tool-selection behavior.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<SamplingToolChoice>,
}

impl CreateMessageParams {
    /// Creates a basic, Tool-free Sampling request.
    pub fn new(messages: Vec<SamplingMessage>, max_tokens: u64) -> Self {
        Self {
            messages,
            model_preferences: None,
            system_prompt: None,
            include_context: IncludeContext::None,
            temperature: None,
            max_tokens,
            stop_sequences: Vec::new(),
            metadata: None,
            tools: Vec::new(),
            tool_choice: None,
        }
    }
}

/// Result of `sampling/createMessage`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessageResult {
    /// Actual client-selected model.
    pub model: String,
    /// Optional provider stop reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    /// Normally the assistant role.
    pub role: SamplingRole,
    /// Generated text, image, or audio blocks.
    pub content: SamplingContent,
    /// Protocol extension metadata.
    #[serde(rename = "_meta", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, Value>,
}

impl CreateMessageResult {
    /// Creates one assistant text result.
    pub fn assistant_text(model: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            stop_reason: Some("endTurn".into()),
            role: SamplingRole::Assistant,
            content: SamplingContent::text(text),
            meta: BTreeMap::new(),
        }
    }
}

/// Stable Sampling failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SamplingErrorKind {
    /// A user or host policy rejected the request or response.
    Rejected,
    /// Wire input violates the Sampling contract.
    InvalidRequest,
    /// A configured request, token, concurrency, or size limit was exceeded.
    LimitExceeded,
    /// Sampling was cancelled.
    Cancelled,
    /// Sampling exceeded its deadline.
    DeadlineExceeded,
    /// The model or approval implementation failed.
    Execution,
    /// Provider output could not be represented safely in MCP.
    InvalidOutput,
}

/// Stable execution stage for Sampling diagnostics and policy decisions.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SamplingStage {
    /// Initial or edited request validation.
    RequestValidation,
    /// Pre-execution host review.
    RequestReview,
    /// Concurrency or lifetime budget reservation.
    BudgetReservation,
    /// Host-selected model execution.
    ModelExecution,
    /// Provider-output validation.
    ResponseValidation,
    /// Pre-disclosure host review.
    ResponseReview,
    /// Outer cancellation or deadline boundary.
    Lifecycle,
}

/// Safe Sampling failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct SamplingError {
    /// Stable failure category.
    pub kind: SamplingErrorKind,
    /// Safe operator-facing explanation.
    pub message: String,
    /// Execution stage that produced the failure, when known.
    pub stage: Option<SamplingStage>,
}

impl SamplingError {
    /// Creates a Sampling error.
    pub fn new(kind: SamplingErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            stage: None,
        }
    }

    /// Attaches a stable execution stage without replacing an earlier stage.
    #[must_use]
    pub fn with_stage(mut self, stage: SamplingStage) -> Self {
        self.stage.get_or_insert(stage);
        self
    }
}

/// Explicit human or host-policy review decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SamplingDecision<T> {
    /// Permit the possibly edited value.
    Approve(T),
    /// Deny disclosure or model execution.
    Reject,
}

/// Boxed future returned by Sampling policy boundaries.
pub type SamplingFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, SamplingError>> + Send + 'a>>;

/// Context shared by approval and provider boundaries.
#[derive(Clone, Debug)]
pub struct SamplingCallContext {
    deadline: Instant,
    cancellation: CancellationToken,
}

impl SamplingCallContext {
    /// Returns the effective deadline.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Returns the cancellation token for this Sampling call.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// Two-stage approval boundary for Sampling.
pub trait SamplingApprover: Send + Sync {
    /// Reviews and may edit the server-proposed model request.
    fn review_request(
        &self,
        request: CreateMessageParams,
        context: SamplingCallContext,
    ) -> SamplingFuture<'_, SamplingDecision<CreateMessageParams>>;

    /// Reviews generated output before it is disclosed to the MCP server.
    fn review_response(
        &self,
        response: CreateMessageResult,
        context: SamplingCallContext,
    ) -> SamplingFuture<'_, SamplingDecision<CreateMessageResult>>;
}

/// Host-owned provider boundary for one approved Sampling request.
pub trait SamplingProvider: Send + Sync {
    /// Selects and invokes a model. Server preferences remain advisory.
    fn sample(
        &self,
        request: CreateMessageParams,
        context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult>;
}

/// Resource and abuse limits for client-side Sampling.
#[derive(Clone, Debug)]
pub struct SamplingPolicy {
    /// Maximum conversation messages.
    pub max_messages: usize,
    /// Maximum total content blocks.
    pub max_content_blocks: usize,
    /// Maximum serialized request or response bytes.
    pub max_serialized_bytes: usize,
    /// Maximum decoded inline-media bytes.
    pub max_media_bytes: usize,
    /// Maximum output-token request.
    pub max_tokens_per_request: u64,
    /// Maximum calls accepted during this service lifetime.
    pub max_total_requests: u64,
    /// Maximum requested output tokens accepted during this service lifetime.
    pub max_total_requested_tokens: u64,
    /// Maximum simultaneous approval/model calls.
    pub max_concurrent_requests: usize,
    /// Maximum duration including both approval stages.
    pub request_timeout: Duration,
}

impl Default for SamplingPolicy {
    fn default() -> Self {
        Self {
            max_messages: 64,
            max_content_blocks: 256,
            max_serialized_bytes: DEFAULT_MAX_SERIALIZED_BYTES,
            max_media_bytes: DEFAULT_MAX_MEDIA_BYTES,
            max_tokens_per_request: 8192,
            max_total_requests: 1000,
            max_total_requested_tokens: 1_000_000,
            max_concurrent_requests: 4,
            request_timeout: Duration::from_secs(60),
        }
    }
}

/// Validated, approved client-side Sampling service.
pub struct SamplingService {
    approver: Arc<dyn SamplingApprover>,
    provider: Arc<dyn SamplingProvider>,
    policy: SamplingPolicy,
    concurrency: Arc<Semaphore>,
    accepted_requests: AtomicU64,
    accepted_tokens: AtomicU64,
}

impl SamplingService {
    /// Creates a secure Sampling service with explicit approval and provider boundaries.
    pub fn new(
        approver: Arc<dyn SamplingApprover>,
        provider: Arc<dyn SamplingProvider>,
        policy: SamplingPolicy,
    ) -> Self {
        Self {
            concurrency: Arc::new(Semaphore::new(policy.max_concurrent_requests.max(1))),
            approver,
            provider,
            policy,
            accepted_requests: AtomicU64::new(0),
            accepted_tokens: AtomicU64::new(0),
        }
    }

    /// Validates, reviews, invokes, reviews again, and returns one Sampling result.
    pub fn execute(
        &self,
        request: CreateMessageParams,
        cancellation: CancellationToken,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async move {
            validate_request(&request, &self.policy)
                .map_err(|error| error.with_stage(SamplingStage::RequestValidation))?;
            let _permit = self.concurrency.try_acquire().map_err(|_| {
                SamplingError::new(
                    SamplingErrorKind::LimitExceeded,
                    "Sampling concurrency limit exceeded",
                )
                .with_stage(SamplingStage::BudgetReservation)
            })?;
            let deadline = Instant::now()
                .checked_add(self.policy.request_timeout)
                .ok_or_else(|| {
                    SamplingError::new(
                        SamplingErrorKind::LimitExceeded,
                        "Sampling timeout is outside platform limits",
                    )
                    .with_stage(SamplingStage::Lifecycle)
                })?;
            let context = SamplingCallContext {
                deadline,
                cancellation: cancellation.clone(),
            };
            let operation = async {
                let approved = self
                    .approver
                    .review_request(request, context.clone())
                    .await
                    .map_err(|error| error.with_stage(SamplingStage::RequestReview))?;
                let SamplingDecision::Approve(request) = approved else {
                    return Err(SamplingError::new(
                        SamplingErrorKind::Rejected,
                        "Sampling request rejected",
                    )
                    .with_stage(SamplingStage::RequestReview));
                };
                validate_request(&request, &self.policy)
                    .map_err(|error| error.with_stage(SamplingStage::RequestValidation))?;
                self.reserve_budget(request.max_tokens)
                    .map_err(|error| error.with_stage(SamplingStage::BudgetReservation))?;
                let response = self
                    .provider
                    .sample(request, context.clone())
                    .await
                    .map_err(|error| error.with_stage(SamplingStage::ModelExecution))?;
                validate_response(&response, &self.policy)
                    .map_err(|error| error.with_stage(SamplingStage::ResponseValidation))?;
                match self
                    .approver
                    .review_response(response, context.clone())
                    .await
                    .map_err(|error| error.with_stage(SamplingStage::ResponseReview))?
                {
                    SamplingDecision::Approve(response) => {
                        validate_response(&response, &self.policy)
                            .map_err(|error| error.with_stage(SamplingStage::ResponseValidation))?;
                        Ok(response)
                    }
                    SamplingDecision::Reject => Err(SamplingError::new(
                        SamplingErrorKind::Rejected,
                        "Sampling response rejected",
                    )
                    .with_stage(SamplingStage::ResponseReview)),
                }
            };
            tokio::select! {
                result = operation => result,
                () = cancellation.cancelled() => Err(SamplingError::new(
                    SamplingErrorKind::Cancelled,
                    "Sampling request cancelled",
                ).with_stage(SamplingStage::Lifecycle)),
                () = tokio::time::sleep_until(deadline.into()) => Err(SamplingError::new(
                    SamplingErrorKind::DeadlineExceeded,
                    "Sampling request deadline elapsed",
                ).with_stage(SamplingStage::Lifecycle)),
            }
        })
    }

    fn reserve_budget(&self, tokens: u64) -> Result<(), SamplingError> {
        reserve(
            &self.accepted_requests,
            1,
            self.policy.max_total_requests,
            "Sampling request budget exhausted",
        )?;
        reserve(
            &self.accepted_tokens,
            tokens,
            self.policy.max_total_requested_tokens,
            "Sampling token budget exhausted",
        )
    }
}

impl std::fmt::Debug for SamplingService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SamplingService")
            .field("policy", &self.policy)
            .field(
                "accepted_requests",
                &self.accepted_requests.load(Ordering::Relaxed),
            )
            .field(
                "accepted_tokens",
                &self.accepted_tokens.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

fn validate_request(
    request: &CreateMessageParams,
    policy: &SamplingPolicy,
) -> Result<(), SamplingError> {
    if request.messages.is_empty() || request.messages.len() > policy.max_messages {
        return Err(invalid("Sampling message count is outside policy"));
    }
    if request.max_tokens == 0 || request.max_tokens > policy.max_tokens_per_request {
        return Err(limit("Sampling maxTokens is outside policy"));
    }
    if !matches!(request.include_context, IncludeContext::None) {
        return Err(invalid("Sampling context inclusion is not supported"));
    }
    if !request.tools.is_empty() || request.tool_choice.is_some() {
        return Err(invalid("Tool-enabled Sampling is not supported"));
    }
    if request
        .metadata
        .as_ref()
        .is_some_and(|metadata| !metadata.is_object())
    {
        return Err(invalid("Sampling metadata must be an object"));
    }
    if request
        .temperature
        .is_some_and(|temperature| !temperature.is_finite())
    {
        return Err(invalid("Sampling temperature must be finite"));
    }
    if request
        .model_preferences
        .as_ref()
        .is_some_and(|preferences| {
            [
                preferences.cost_priority,
                preferences.speed_priority,
                preferences.intelligence_priority,
            ]
            .into_iter()
            .flatten()
            .any(|priority| !priority.is_finite() || !(0.0..=1.0).contains(&priority))
        })
    {
        return Err(invalid("Sampling model priorities must be between 0 and 1"));
    }
    validate_messages(&request.messages, policy)?;
    validate_encoded_size(request, policy.max_serialized_bytes, "Sampling request")
}

fn validate_response(
    response: &CreateMessageResult,
    policy: &SamplingPolicy,
) -> Result<(), SamplingError> {
    if response.model.trim().is_empty() || response.role != SamplingRole::Assistant {
        return Err(output("Sampling response model or role is invalid"));
    }
    if response.stop_reason.as_deref() == Some("toolUse") {
        return Err(output(
            "basic Sampling response must not request Tool execution",
        ));
    }
    validate_blocks(response.content.as_slice(), policy, &mut 0)
        .map_err(|error| SamplingError::new(SamplingErrorKind::InvalidOutput, error.message))?;
    validate_encoded_size(response, policy.max_serialized_bytes, "Sampling response")
        .map_err(|error| SamplingError::new(SamplingErrorKind::InvalidOutput, error.message))
}

fn validate_messages(
    messages: &[SamplingMessage],
    policy: &SamplingPolicy,
) -> Result<(), SamplingError> {
    let mut blocks = 0;
    for message in messages {
        validate_blocks(message.content.as_slice(), policy, &mut blocks)?;
    }
    Ok(())
}

fn validate_blocks(
    content: &[ContentBlock],
    policy: &SamplingPolicy,
    total: &mut usize,
) -> Result<(), SamplingError> {
    if content.is_empty() {
        return Err(invalid("Sampling message content must not be empty"));
    }
    *total = total.saturating_add(content.len());
    if *total > policy.max_content_blocks {
        return Err(limit("Sampling content-block limit exceeded"));
    }
    for block in content {
        match block.kind.as_str() {
            "text" if block.fields.get("text").and_then(Value::as_str).is_some() => {}
            "image" | "audio" => validate_media(block, policy.max_media_bytes)?,
            _ => return Err(invalid("unsupported Sampling content block")),
        }
    }
    Ok(())
}

fn validate_media(block: &ContentBlock, max_media_bytes: usize) -> Result<(), SamplingError> {
    let data = block
        .fields
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("Sampling media data is missing"))?;
    let mime = block
        .fields
        .get("mimeType")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("Sampling media MIME type is missing"))?;
    if mime.trim().is_empty() {
        return Err(invalid("Sampling media MIME type is blank"));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| invalid("Sampling media is not valid base64"))?;
    if bytes.len() > max_media_bytes {
        return Err(limit("Sampling decoded-media limit exceeded"));
    }
    Ok(())
}

fn validate_encoded_size<T: Serialize>(
    value: &T,
    max_bytes: usize,
    label: &str,
) -> Result<(), SamplingError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| invalid(format!("{label} cannot be encoded")))?
        .len();
    if bytes > max_bytes {
        return Err(limit(format!("{label} exceeds the serialized-size limit")));
    }
    Ok(())
}

fn reserve(
    counter: &AtomicU64,
    amount: u64,
    maximum: u64,
    message: &str,
) -> Result<(), SamplingError> {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(amount).filter(|next| *next <= maximum)
        })
        .map(|_| ())
        .map_err(|_| limit(message))
}

fn invalid(message: impl Into<String>) -> SamplingError {
    SamplingError::new(SamplingErrorKind::InvalidRequest, message)
}

fn limit(message: impl Into<String>) -> SamplingError {
    SamplingError::new(SamplingErrorKind::LimitExceeded, message)
}

fn output(message: impl Into<String>) -> SamplingError {
    SamplingError::new(SamplingErrorKind::InvalidOutput, message)
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde skip predicates receive references.
const fn is_no_context(value: &IncludeContext) -> bool {
    matches!(value, IncludeContext::None)
}
