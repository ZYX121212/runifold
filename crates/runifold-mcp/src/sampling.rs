use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use runifold_core::CancellationToken;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::sampling_validation::{validate_request, validate_response};
use crate::{ContentBlock, McpTool, SamplingCapability, TaskMetadata};

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
    /// Optional request for durable asynchronous execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskMetadata>,
    /// Protocol metadata, including related-task correlation.
    #[serde(rename = "_meta", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, Value>,
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
            task: None,
            meta: BTreeMap::new(),
        }
    }

    /// Adds a caller-generated UUIDv4/v7 used to recover the same durable Task
    /// when a create response is lost and the request is retried.
    #[must_use]
    pub fn with_task_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.meta.insert(
            crate::SAMPLING_TASK_IDEMPOTENCY_KEY.into(),
            Value::String(key.into()),
        );
        self
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
    /// Host-controlled ambient context resolution.
    ContextResolution,
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
    /// Reports whether this provider can execute Tool-enabled Sampling.
    fn supports_tools(&self) -> bool {
        false
    }

    /// Selects and invokes a model. Server preferences remain advisory.
    fn sample(
        &self,
        request: CreateMessageParams,
        context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult>;
}

/// Host-owned resolver for soft-deprecated MCP ambient context inclusion.
///
/// The resolver returns explicit messages which are inserted before the
/// server-supplied conversation and shown to the request approver before any
/// model call occurs.
pub trait SamplingContextProvider: Send + Sync {
    /// Resolves context from the requested server scope.
    fn resolve(
        &self,
        include: IncludeContext,
        context: SamplingCallContext,
    ) -> SamplingFuture<'_, Vec<SamplingMessage>>;
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
    /// Maximum task retention accepted from a Sampling requester.
    pub max_task_ttl: Duration,
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
            max_task_ttl: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

/// Validated, approved client-side Sampling service.
pub struct SamplingService {
    approver: Arc<dyn SamplingApprover>,
    provider: Arc<dyn SamplingProvider>,
    context_provider: Option<Arc<dyn SamplingContextProvider>>,
    policy: SamplingPolicy,
    concurrency: Arc<Semaphore>,
    accepted_requests: AtomicU64,
    accepted_tokens: AtomicU64,
    accepted_task_keys: StdMutex<HashMap<String, Vec<u8>>>,
}

pub(crate) struct ApprovedTaskRequest {
    pub(crate) params: CreateMessageParams,
    pub(crate) budget_reserved: bool,
    pub(crate) idempotency_key: Option<String>,
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
            context_provider: None,
            policy,
            accepted_requests: AtomicU64::new(0),
            accepted_tokens: AtomicU64::new(0),
            accepted_task_keys: StdMutex::new(HashMap::new()),
        }
    }

    /// Enables host-controlled resolution of `includeContext` requests.
    #[must_use]
    pub fn with_context_provider(mut self, provider: Arc<dyn SamplingContextProvider>) -> Self {
        self.context_provider = Some(provider);
        self
    }

    /// Returns the exact MCP Sampling capabilities implemented by this service.
    pub fn capability(&self) -> SamplingCapability {
        SamplingCapability {
            context: self.context_provider.as_ref().map(|_| BTreeMap::new()),
            tools: self.provider.supports_tools().then(BTreeMap::new),
        }
    }

    pub(crate) fn approval_lease_ms(&self) -> u64 {
        u64::try_from(self.policy.request_timeout.as_millis())
            .unwrap_or(u64::MAX / 4)
            .saturating_add(5_000)
    }

    pub(crate) fn approve_task_request(
        &self,
        request: CreateMessageParams,
        cancellation: CancellationToken,
    ) -> SamplingFuture<'_, ApprovedTaskRequest> {
        Box::pin(async move {
            validate_request(
                &request,
                &self.policy,
                self.context_provider.is_some(),
                self.provider.supports_tools(),
            )
            .map_err(|error| error.with_stage(SamplingStage::RequestValidation))?;
            let _permit = self.sampling_permit()?;
            let deadline = self.sampling_deadline()?;
            let context = SamplingCallContext {
                deadline,
                cancellation: cancellation.clone(),
            };
            tokio::select! {
                result = self.review_task_request(request, context) => result,
                () = cancellation.cancelled() => Err(lifecycle_cancelled()),
                () = tokio::time::sleep_until(deadline.into()) => Err(lifecycle_timeout()),
            }
        })
    }

    async fn review_task_request(
        &self,
        request: CreateMessageParams,
        context: SamplingCallContext,
    ) -> Result<ApprovedTaskRequest, SamplingError> {
        let request = self.prepare_request(request, context.clone()).await?;
        let approved = self
            .approver
            .review_request(request, context)
            .await
            .map_err(|error| error.with_stage(SamplingStage::RequestReview))?;
        let SamplingDecision::Approve(request) = approved else {
            return Err(SamplingError::new(
                SamplingErrorKind::Rejected,
                "Sampling request rejected",
            )
            .with_stage(SamplingStage::RequestReview));
        };
        validate_request(
            &request,
            &self.policy,
            false,
            self.provider.supports_tools(),
        )
        .map_err(|error| error.with_stage(SamplingStage::RequestValidation))?;
        let idempotency_key = request
            .meta
            .get(crate::SAMPLING_TASK_IDEMPOTENCY_KEY)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let budget_reserved = self
            .reserve_task_budget(&request, idempotency_key.as_deref())
            .map_err(|error| error.with_stage(SamplingStage::BudgetReservation))?;
        Ok(ApprovedTaskRequest {
            params: request,
            budget_reserved,
            idempotency_key,
        })
    }

    pub(crate) fn approve_task_result(
        &self,
        request: CreateMessageParams,
        response: CreateMessageResult,
        cancellation: CancellationToken,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async move {
            let _permit = self.sampling_permit()?;
            let deadline = self.sampling_deadline()?;
            let context = SamplingCallContext {
                deadline,
                cancellation: cancellation.clone(),
            };
            tokio::select! {
                result = self.review_response(&request, response, context) => result,
                () = cancellation.cancelled() => Err(lifecycle_cancelled()),
                () = tokio::time::sleep_until(deadline.into()) => Err(lifecycle_timeout()),
            }
        })
    }

    /// Validates, reviews, invokes, reviews again, and returns one Sampling result.
    pub fn execute(
        &self,
        request: CreateMessageParams,
        cancellation: CancellationToken,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async move {
            validate_request(
                &request,
                &self.policy,
                self.context_provider.is_some(),
                self.provider.supports_tools(),
            )
            .map_err(|error| error.with_stage(SamplingStage::RequestValidation))?;
            let _permit = self.sampling_permit()?;
            let deadline = self.sampling_deadline()?;
            let context = SamplingCallContext {
                deadline,
                cancellation: cancellation.clone(),
            };
            let operation = async {
                let request = self.review_request(request, context.clone()).await?;
                let tools = request.tools.clone();
                let tool_choice = request.tool_choice.clone();
                let response = self
                    .provider
                    .sample(request, context.clone())
                    .await
                    .map_err(|error| error.with_stage(SamplingStage::ModelExecution))?;
                self.review_response_with_contract(&tools, tool_choice.as_ref(), response, context)
                    .await
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

    async fn review_request(
        &self,
        request: CreateMessageParams,
        context: SamplingCallContext,
    ) -> Result<CreateMessageParams, SamplingError> {
        let request = self.prepare_request(request, context.clone()).await?;
        let approved = self
            .approver
            .review_request(request, context)
            .await
            .map_err(|error| error.with_stage(SamplingStage::RequestReview))?;
        let SamplingDecision::Approve(request) = approved else {
            return Err(SamplingError::new(
                SamplingErrorKind::Rejected,
                "Sampling request rejected",
            )
            .with_stage(SamplingStage::RequestReview));
        };
        validate_request(
            &request,
            &self.policy,
            false,
            self.provider.supports_tools(),
        )
        .map_err(|error| error.with_stage(SamplingStage::RequestValidation))?;
        self.reserve_budget(request.max_tokens)
            .map_err(|error| error.with_stage(SamplingStage::BudgetReservation))?;
        Ok(request)
    }

    async fn review_response(
        &self,
        request: &CreateMessageParams,
        response: CreateMessageResult,
        context: SamplingCallContext,
    ) -> Result<CreateMessageResult, SamplingError> {
        self.review_response_with_contract(
            &request.tools,
            request.tool_choice.as_ref(),
            response,
            context,
        )
        .await
    }

    async fn review_response_with_contract(
        &self,
        tools: &[McpTool],
        tool_choice: Option<&SamplingToolChoice>,
        response: CreateMessageResult,
        context: SamplingCallContext,
    ) -> Result<CreateMessageResult, SamplingError> {
        validate_response(&response, &self.policy, tools, tool_choice)
            .map_err(|error| error.with_stage(SamplingStage::ResponseValidation))?;
        match self
            .approver
            .review_response(response, context)
            .await
            .map_err(|error| error.with_stage(SamplingStage::ResponseReview))?
        {
            SamplingDecision::Approve(response) => {
                validate_response(&response, &self.policy, tools, tool_choice)
                    .map_err(|error| error.with_stage(SamplingStage::ResponseValidation))?;
                Ok(response)
            }
            SamplingDecision::Reject => Err(SamplingError::new(
                SamplingErrorKind::Rejected,
                "Sampling response rejected",
            )
            .with_stage(SamplingStage::ResponseReview)),
        }
    }

    fn sampling_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>, SamplingError> {
        self.concurrency.try_acquire().map_err(|_| {
            SamplingError::new(
                SamplingErrorKind::LimitExceeded,
                "Sampling concurrency limit exceeded",
            )
            .with_stage(SamplingStage::BudgetReservation)
        })
    }

    fn sampling_deadline(&self) -> Result<Instant, SamplingError> {
        Instant::now()
            .checked_add(self.policy.request_timeout)
            .ok_or_else(|| {
                SamplingError::new(
                    SamplingErrorKind::LimitExceeded,
                    "Sampling timeout is outside platform limits",
                )
                .with_stage(SamplingStage::Lifecycle)
            })
    }

    async fn prepare_request(
        &self,
        mut request: CreateMessageParams,
        context: SamplingCallContext,
    ) -> Result<CreateMessageParams, SamplingError> {
        if matches!(request.include_context, IncludeContext::None) {
            return Ok(request);
        }
        let include = request.include_context;
        let context_provider = self.context_provider.as_ref().ok_or_else(|| {
            invalid("Sampling context inclusion was not negotiated")
                .with_stage(SamplingStage::RequestValidation)
        })?;
        let context_messages = context_provider
            .resolve(include, context)
            .await
            .map_err(|error| error.with_stage(SamplingStage::ContextResolution))?;
        request.messages.splice(0..0, context_messages);
        request.include_context = IncludeContext::None;
        validate_request(
            &request,
            &self.policy,
            false,
            self.provider.supports_tools(),
        )
        .map_err(|error| error.with_stage(SamplingStage::RequestValidation))?;
        Ok(request)
    }

    fn reserve_budget(&self, tokens: u64) -> Result<(), SamplingError> {
        reserve(
            &self.accepted_requests,
            1,
            self.policy.max_total_requests,
            "Sampling request budget exhausted",
        )?;
        if let Err(error) = reserve(
            &self.accepted_tokens,
            tokens,
            self.policy.max_total_requested_tokens,
            "Sampling token budget exhausted",
        ) {
            self.accepted_requests.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        Ok(())
    }

    fn reserve_task_budget(
        &self,
        request: &CreateMessageParams,
        idempotency_key: Option<&str>,
    ) -> Result<bool, SamplingError> {
        let Some(key) = idempotency_key else {
            self.reserve_budget(request.max_tokens)?;
            return Ok(true);
        };
        let mut fingerprint_request = request.clone();
        fingerprint_request
            .meta
            .remove(crate::SAMPLING_TASK_IDEMPOTENCY_KEY);
        let fingerprint = serde_json::to_vec(&fingerprint_request)
            .map_err(|_| invalid("Sampling Task request could not be fingerprinted"))?;
        let mut keys = self
            .accepted_task_keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = keys.get(key) {
            return if existing == &fingerprint {
                Ok(false)
            } else {
                Err(invalid(
                    "Sampling Task idempotency key was reused with a different request",
                ))
            };
        }
        self.reserve_budget(request.max_tokens)?;
        keys.insert(key.to_owned(), fingerprint);
        Ok(true)
    }

    pub(crate) fn rollback_task_budget(&self, tokens: u64) {
        let requests =
            self.accepted_requests
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_sub(1)
                });
        let requested_tokens =
            self.accepted_tokens
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_sub(tokens)
                });
        debug_assert!(requests.is_ok(), "Sampling request budget underflow");
        debug_assert!(requested_tokens.is_ok(), "Sampling token budget underflow");
    }

    pub(crate) fn forget_task_budget_key(&self, key: &str) {
        self.accepted_task_keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
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

fn lifecycle_cancelled() -> SamplingError {
    SamplingError::new(SamplingErrorKind::Cancelled, "Sampling request cancelled")
        .with_stage(SamplingStage::Lifecycle)
}

fn lifecycle_timeout() -> SamplingError {
    SamplingError::new(
        SamplingErrorKind::DeadlineExceeded,
        "Sampling request deadline elapsed",
    )
    .with_stage(SamplingStage::Lifecycle)
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde skip predicates receive references.
const fn is_no_context(value: &IncludeContext) -> bool {
    matches!(value, IncludeContext::None)
}
