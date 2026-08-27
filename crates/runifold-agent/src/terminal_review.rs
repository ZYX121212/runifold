//! Agent terminal-output review contracts and deterministic rule adapters.

use std::{fmt, sync::Arc};

use runifold_core::{CapabilitySet, RunContext};
use runifold_model::{ContentPart, FinishReason, Message, ModelResponse};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::AgentFuture;

const MAX_REVIEW_FEEDBACK_BYTES: usize = 65_536;
const MAX_REVIEW_REQUEST_BYTES: usize = 1_048_576;
const MAX_REJECTION_REASON_BYTES: usize = 4_096;
const MAX_REVIEWER_NAME_BYTES: usize = 128;
const MAX_REVIEW_ERROR_BYTES: usize = 4_096;
const MAX_DESCRIPTOR_CONFIGURATION_BYTES: usize = 65_536;

#[derive(Clone)]
pub(crate) struct TerminalReviewConfig {
    pub(crate) reviewer: Arc<dyn TerminalReviewer>,
    pub(crate) descriptor: TerminalReviewerDescriptor,
    pub(crate) policy: TerminalReviewPolicy,
    pub(crate) capabilities: CapabilitySet,
}

#[derive(Clone)]
pub(crate) struct TurnReviewConfig {
    pub(crate) reviewer: Arc<dyn TurnReviewer>,
    pub(crate) descriptor: TerminalReviewerDescriptor,
    pub(crate) policy: TurnReviewPolicy,
    pub(crate) capabilities: CapabilitySet,
}

impl fmt::Debug for TurnReviewConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnReviewConfig")
            .field("descriptor", &self.descriptor)
            .field("policy", &self.policy)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for TerminalReviewConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TerminalReviewConfig")
            .field("descriptor", &self.descriptor)
            .field("policy", &self.policy)
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

/// Stable identity bound to terminal-review checkpoints.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminalReviewerDescriptor {
    name: String,
    version: String,
    configuration_sha256: String,
}

impl TerminalReviewerDescriptor {
    /// Creates a validated descriptor and hashes canonical configuration JSON.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::InvalidConfiguration`] for invalid
    /// identifiers or configuration larger than 64 KiB.
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        configuration: &Value,
    ) -> Result<Self, TerminalReviewError> {
        let name = name.into();
        let version = version.into();
        validate_identifier("terminal reviewer name", &name)?;
        validate_identifier("terminal reviewer version", &version)?;
        let encoded = serde_json::to_vec(configuration)
            .map_err(|error| TerminalReviewError::InvalidConfiguration(error.to_string()))?;
        if encoded.len() > MAX_DESCRIPTOR_CONFIGURATION_BYTES {
            return Err(TerminalReviewError::InvalidConfiguration(format!(
                "terminal reviewer configuration exceeds {MAX_DESCRIPTOR_CONFIGURATION_BYTES} bytes"
            )));
        }
        let digest = Sha256::digest(encoded);
        let configuration_sha256 = lowercase_hex(&digest);
        Ok(Self {
            name,
            version,
            configuration_sha256,
        })
    }

    /// Returns the stable reviewer name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the caller-managed reviewer version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the lowercase SHA-256 configuration fingerprint.
    pub fn configuration_sha256(&self) -> &str {
        &self.configuration_sha256
    }
}

/// Bounded policy for semantic review of locally valid terminal candidates.
///
/// Review repairs consume ordinary model turns and the shared run-tree budget.
/// A zero repair limit still runs the reviewer, but a repair verdict terminates
/// immediately as exhausted.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminalReviewPolicy {
    max_repairs: u32,
}

impl TerminalReviewPolicy {
    /// Creates a policy with an explicit maximum number of review repairs.
    pub const fn new(max_repairs: u32) -> Self {
        Self { max_repairs }
    }

    /// Returns the maximum number of review-triggered regeneration turns.
    pub const fn max_repairs(self) -> u32 {
        self.max_repairs
    }
}

/// Model responses selected for internal turn review.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TurnReviewScope {
    /// Review tool plans and provider continuation responses, but leave the
    /// final response to the independently configured terminal reviewer.
    #[default]
    IntermediateOnly,
    /// Review every model response, including the final response.
    EveryModelResponse,
}

impl TurnReviewScope {
    pub(crate) fn includes(self, response: &ModelResponse) -> bool {
        match self {
            Self::EveryModelResponse => true,
            Self::IntermediateOnly => {
                response
                    .content
                    .iter()
                    .any(|part| matches!(part, ContentPart::ToolCall(_)))
                    || matches!(
                        &response.finish_reason,
                        FinishReason::Other(reason) if reason == "pause_turn"
                    )
            }
        }
    }
}

/// Bounded policy for reviewing internal model turns before side effects.
///
/// The repair limit is cumulative across one Agent execution. Repairs consume
/// ordinary model turns and the shared run-tree budget.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TurnReviewPolicy {
    max_repairs: u32,
    scope: TurnReviewScope,
}

impl TurnReviewPolicy {
    /// Creates an intermediate-only policy with a total repair limit.
    pub const fn new(max_repairs: u32) -> Self {
        Self {
            max_repairs,
            scope: TurnReviewScope::IntermediateOnly,
        }
    }

    /// Selects which model responses enter turn review.
    #[must_use]
    pub const fn with_scope(mut self, scope: TurnReviewScope) -> Self {
        self.scope = scope;
        self
    }

    /// Returns the maximum number of review-triggered replacement turns.
    pub const fn max_repairs(self) -> u32 {
        self.max_repairs
    }

    /// Returns the selected internal review scope.
    pub const fn scope(self) -> TurnReviewScope {
        self.scope
    }
}

/// Canonical input supplied to an Agent terminal reviewer.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TerminalReviewRequest {
    /// Stable generator Agent name.
    pub agent: String,
    /// One-based generator model turn that produced the candidate.
    pub turn: u32,
    /// One-based semantic review attempt.
    pub attempt: u32,
    /// Stable transcript before the candidate is accepted.
    pub transcript: Vec<Message>,
    /// Locally valid terminal candidate awaiting semantic review.
    pub candidate: ModelResponse,
}

impl TerminalReviewRequest {
    /// Validates the complete canonical reviewer request size.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::RequestTooLarge`] when the serialized
    /// transcript and candidate exceed 1 MiB.
    pub fn validate(&self) -> Result<(), TerminalReviewError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| TerminalReviewError::InvalidVerdict(error.to_string()))?
            .len();
        if bytes > MAX_REVIEW_REQUEST_BYTES {
            return Err(TerminalReviewError::RequestTooLarge {
                bytes,
                maximum: MAX_REVIEW_REQUEST_BYTES,
            });
        }
        Ok(())
    }
}

/// Canonical input supplied before one model response can affect the Agent.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TurnReviewRequest {
    /// Stable generator Agent name.
    pub agent: String,
    /// One-based model turn that produced the response.
    pub turn: u32,
    /// Stable transcript before the response is accepted.
    pub transcript: Vec<Message>,
    /// Model response awaiting review before tool execution or completion.
    pub candidate: ModelResponse,
}

impl TurnReviewRequest {
    /// Validates the complete canonical reviewer request size.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::RequestTooLarge`] when the serialized
    /// transcript and candidate exceed 1 MiB.
    pub fn validate(&self) -> Result<(), TerminalReviewError> {
        let bytes = serde_json::to_vec(self)
            .map_err(|error| TerminalReviewError::InvalidVerdict(error.to_string()))?
            .len();
        if bytes > MAX_REVIEW_REQUEST_BYTES {
            return Err(TerminalReviewError::RequestTooLarge {
                bytes,
                maximum: MAX_REVIEW_REQUEST_BYTES,
            });
        }
        Ok(())
    }
}

/// Stable semantic terminal-review verdict category.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TerminalReviewVerdictKind {
    /// Candidate is accepted.
    Approve,
    /// Candidate requires bounded regeneration.
    Repair,
    /// Candidate is permanently rejected.
    Reject,
}

impl TerminalReviewVerdictKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Approve => "approve",
            Self::Repair => "repair",
            Self::Reject => "reject",
        }
    }
}

/// Semantic decision returned by a terminal reviewer.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
#[non_exhaustive]
pub enum TerminalReviewVerdict {
    /// Commit the candidate as the Agent outcome.
    Approve,
    /// Append bounded feedback and ask the original Agent to regenerate.
    Repair {
        /// Structured reviewer findings supplied to the generator as data.
        feedback: Value,
    },
    /// Permanently terminate without accepting or regenerating the candidate.
    Reject {
        /// Safe, bounded rejection explanation.
        reason: String,
    },
}

impl TerminalReviewVerdict {
    /// Creates an approval verdict.
    pub const fn approve() -> Self {
        Self::Approve
    }

    /// Creates a validated repair verdict.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::InvalidVerdict`] when feedback is null
    /// or its canonical JSON representation exceeds 64 KiB.
    pub fn repair(feedback: Value) -> Result<Self, TerminalReviewError> {
        let verdict = Self::Repair { feedback };
        verdict.validate()?;
        Ok(verdict)
    }

    /// Creates a validated permanent rejection verdict.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::InvalidVerdict`] when the reason is
    /// blank or exceeds 4 KiB.
    pub fn reject(reason: impl Into<String>) -> Result<Self, TerminalReviewError> {
        let verdict = Self::Reject {
            reason: reason.into(),
        };
        verdict.validate()?;
        Ok(verdict)
    }

    /// Validates bounds and required fields before a verdict is acted upon.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::InvalidVerdict`] for invalid feedback or
    /// rejection text.
    pub fn validate(&self) -> Result<(), TerminalReviewError> {
        match self {
            Self::Approve => Ok(()),
            Self::Repair { feedback } => {
                if feedback.is_null() {
                    return Err(TerminalReviewError::InvalidVerdict(
                        "repair feedback cannot be null".into(),
                    ));
                }
                let length = serde_json::to_vec(feedback)
                    .map_err(|error| TerminalReviewError::InvalidVerdict(error.to_string()))?
                    .len();
                if length > MAX_REVIEW_FEEDBACK_BYTES {
                    return Err(TerminalReviewError::InvalidVerdict(format!(
                        "repair feedback exceeds {MAX_REVIEW_FEEDBACK_BYTES} bytes"
                    )));
                }
                Ok(())
            }
            Self::Reject { reason } => validate_bounded_text(
                "rejection reason",
                reason,
                MAX_REJECTION_REASON_BYTES,
                TerminalReviewError::InvalidVerdict,
            ),
        }
    }

    /// Returns the stable verdict category.
    pub const fn kind(&self) -> TerminalReviewVerdictKind {
        match self {
            Self::Approve => TerminalReviewVerdictKind::Approve,
            Self::Repair { .. } => TerminalReviewVerdictKind::Repair,
            Self::Reject { .. } => TerminalReviewVerdictKind::Reject,
        }
    }
}

/// Failure while evaluating a terminal candidate.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum TerminalReviewError {
    /// Reviewer configuration is invalid.
    #[error("invalid terminal reviewer configuration: {0}")]
    InvalidConfiguration(String),
    /// The transcript and candidate exceed the reviewer request boundary.
    #[error("terminal review request is {bytes} bytes; maximum is {maximum}")]
    RequestTooLarge {
        /// Canonical serialized request size.
        bytes: usize,
        /// Maximum accepted request size.
        maximum: usize,
    },
    /// The reviewer could not execute or obtain a decision.
    #[error("terminal reviewer execution failed: {0}")]
    Execution(String),
    /// A reviewer returned an unsafe or malformed decision.
    #[error("invalid terminal review verdict: {0}")]
    InvalidVerdict(String),
}

impl TerminalReviewError {
    pub(crate) fn bounded(self) -> Self {
        match self {
            Self::InvalidConfiguration(message) => {
                Self::InvalidConfiguration(truncate_utf8(message, MAX_REVIEW_ERROR_BYTES))
            }
            Self::Execution(message) => {
                Self::Execution(truncate_utf8(message, MAX_REVIEW_ERROR_BYTES))
            }
            Self::InvalidVerdict(message) => {
                Self::InvalidVerdict(truncate_utf8(message, MAX_REVIEW_ERROR_BYTES))
            }
            Self::RequestTooLarge { bytes, maximum } => Self::RequestTooLarge { bytes, maximum },
        }
    }
}

/// Future returned by a terminal reviewer.
pub type TerminalReviewFuture<'a> =
    AgentFuture<'a, Result<TerminalReviewVerdict, TerminalReviewError>>;

/// Pluggable semantic reviewer for locally valid Agent terminal candidates.
pub trait TerminalReviewer: Send + Sync {
    /// Returns the stable identity persisted into Agent checkpoints.
    fn descriptor(&self) -> &TerminalReviewerDescriptor;

    /// Reviews a candidate inside an explicitly attenuated child run.
    fn review_terminal<'a>(
        &'a self,
        request: TerminalReviewRequest,
        run: &'a RunContext,
    ) -> TerminalReviewFuture<'a>;
}

/// Turn-review verdicts use the same bounded approve/repair/reject contract as
/// terminal review.
pub type TurnReviewVerdict = TerminalReviewVerdict;

/// Turn-review failures use the same validated and bounded error contract as
/// terminal review.
pub type TurnReviewError = TerminalReviewError;

/// Stable descriptor persisted for an internal turn reviewer.
pub type TurnReviewerDescriptor = TerminalReviewerDescriptor;

/// Future returned by an internal turn reviewer.
pub type TurnReviewFuture<'a> = AgentFuture<'a, Result<TurnReviewVerdict, TurnReviewError>>;

/// Pluggable reviewer invoked after a model response and before that response
/// can execute tools or enter terminal completion.
pub trait TurnReviewer: Send + Sync {
    /// Returns the stable identity persisted into Agent checkpoints.
    fn turn_descriptor(&self) -> &TurnReviewerDescriptor;

    /// Reviews one model response inside an explicitly attenuated child run.
    fn review_turn<'a>(
        &'a self,
        request: TurnReviewRequest,
        run: &'a RunContext,
    ) -> TurnReviewFuture<'a>;
}

/// Every terminal reviewer can also review internal turns. The compatibility
/// mapping preserves the candidate and transcript and uses one attempt because
/// each generated response enters turn review at most once.
impl<T> TurnReviewer for T
where
    T: TerminalReviewer + ?Sized,
{
    fn turn_descriptor(&self) -> &TurnReviewerDescriptor {
        TerminalReviewer::descriptor(self)
    }

    fn review_turn<'a>(
        &'a self,
        request: TurnReviewRequest,
        run: &'a RunContext,
    ) -> TurnReviewFuture<'a> {
        let terminal = TerminalReviewRequest {
            agent: request.agent,
            turn: request.turn,
            attempt: 1,
            transcript: request.transcript,
            candidate: request.candidate,
        };
        self.review_terminal(terminal, run)
    }
}

type TurnRuleFunction =
    dyn Fn(&TurnReviewRequest) -> Result<TurnReviewVerdict, TurnReviewError> + Send + Sync;

/// Named deterministic rule adapter for [`TurnReviewer`].
#[derive(Clone)]
pub struct TurnRuleReviewer {
    descriptor: TurnReviewerDescriptor,
    rule: Arc<TurnRuleFunction>,
}

impl TurnRuleReviewer {
    /// Creates a validated deterministic internal-turn reviewer.
    ///
    /// # Errors
    ///
    /// Returns [`TurnReviewError::InvalidConfiguration`] for invalid stable
    /// identity fields.
    pub fn new<F>(
        name: impl Into<String>,
        version: impl Into<String>,
        rule: F,
    ) -> Result<Self, TurnReviewError>
    where
        F: Fn(&TurnReviewRequest) -> Result<TurnReviewVerdict, TurnReviewError>
            + Send
            + Sync
            + 'static,
    {
        let name = name.into();
        let version = version.into();
        let descriptor = TurnReviewerDescriptor::new(name, version, &json!({"kind": "turn_rule"}))?;
        Ok(Self {
            descriptor,
            rule: Arc::new(rule),
        })
    }

    /// Returns the stable rule reviewer name.
    pub fn name(&self) -> &str {
        self.descriptor.name()
    }
}

impl TurnReviewer for TurnRuleReviewer {
    fn turn_descriptor(&self) -> &TurnReviewerDescriptor {
        &self.descriptor
    }

    fn review_turn<'a>(
        &'a self,
        request: TurnReviewRequest,
        _run: &'a RunContext,
    ) -> TurnReviewFuture<'a> {
        let verdict = request
            .validate()
            .and_then(|()| (self.rule)(&request))
            .and_then(|verdict| {
                verdict.validate()?;
                Ok(verdict)
            });
        Box::pin(async move { verdict })
    }
}

impl fmt::Debug for TurnRuleReviewer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnRuleReviewer")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

type RuleFunction = dyn Fn(&TerminalReviewRequest) -> Result<TerminalReviewVerdict, TerminalReviewError>
    + Send
    + Sync;

/// Named deterministic rule adapter for [`TerminalReviewer`].
#[derive(Clone)]
pub struct TerminalRuleReviewer {
    descriptor: TerminalReviewerDescriptor,
    rule: Arc<RuleFunction>,
}

impl TerminalRuleReviewer {
    /// Creates a validated deterministic terminal reviewer.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::InvalidConfiguration`] when the reviewer name
    /// is blank, oversized, or is not a stable identifier.
    pub fn new<F>(
        name: impl Into<String>,
        version: impl Into<String>,
        rule: F,
    ) -> Result<Self, TerminalReviewError>
    where
        F: Fn(&TerminalReviewRequest) -> Result<TerminalReviewVerdict, TerminalReviewError>
            + Send
            + Sync
            + 'static,
    {
        let name = name.into();
        let version = version.into();
        let descriptor =
            TerminalReviewerDescriptor::new(name, version, &json!({"kind": "terminal_rule"}))?;
        Ok(Self {
            descriptor,
            rule: Arc::new(rule),
        })
    }

    /// Returns the stable rule reviewer name.
    pub fn name(&self) -> &str {
        self.descriptor.name()
    }
}

impl TerminalReviewer for TerminalRuleReviewer {
    fn descriptor(&self) -> &TerminalReviewerDescriptor {
        &self.descriptor
    }

    fn review_terminal<'a>(
        &'a self,
        request: TerminalReviewRequest,
        _run: &'a RunContext,
    ) -> TerminalReviewFuture<'a> {
        let verdict = request
            .validate()
            .and_then(|()| (self.rule)(&request))
            .and_then(|verdict| {
                verdict.validate()?;
                Ok(verdict)
            });
        Box::pin(async move { verdict })
    }
}

impl fmt::Debug for TerminalRuleReviewer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TerminalRuleReviewer")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

/// How a [`CompositeTerminalReviewer`] processes repair verdicts.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CompositeTerminalReviewMode {
    /// Run every reviewer, reject immediately, and merge repair feedback.
    #[default]
    AllMustApprove,
    /// Return on the first repair or rejection.
    FirstFailure,
}

#[derive(Clone)]
struct TerminalReviewerEntry {
    name: String,
    reviewer: Arc<dyn TerminalReviewer>,
}

/// Deterministic sequential composition of terminal reviewers.
#[derive(Clone)]
pub struct CompositeTerminalReviewer {
    name: String,
    version: String,
    mode: CompositeTerminalReviewMode,
    descriptor: TerminalReviewerDescriptor,
    reviewers: Vec<TerminalReviewerEntry>,
}

impl CompositeTerminalReviewer {
    /// Creates an empty composition with stable identity.
    ///
    /// Add at least one reviewer before execution.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::InvalidConfiguration`] for invalid
    /// identity fields.
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        mode: CompositeTerminalReviewMode,
    ) -> Result<Self, TerminalReviewError> {
        let name = name.into();
        let version = version.into();
        let descriptor = composite_descriptor(&name, &version, mode, &[])?;
        Ok(Self {
            name,
            version,
            mode,
            descriptor,
            reviewers: Vec::new(),
        })
    }

    /// Appends a uniquely named owned reviewer.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::InvalidConfiguration`] for an invalid or
    /// duplicate entry name, or an oversized composite descriptor.
    pub fn push<R>(
        &mut self,
        name: impl Into<String>,
        reviewer: R,
    ) -> Result<(), TerminalReviewError>
    where
        R: TerminalReviewer + 'static,
    {
        self.push_shared(name, Arc::new(reviewer))
    }

    /// Appends a uniquely named shared reviewer.
    ///
    /// # Errors
    ///
    /// Returns [`TerminalReviewError::InvalidConfiguration`] for an invalid or
    /// duplicate entry name, or an oversized composite descriptor.
    pub fn push_shared(
        &mut self,
        name: impl Into<String>,
        reviewer: Arc<dyn TerminalReviewer>,
    ) -> Result<(), TerminalReviewError> {
        let name = name.into();
        validate_identifier("composite terminal reviewer entry", &name)?;
        if self.reviewers.iter().any(|entry| entry.name == name) {
            return Err(TerminalReviewError::InvalidConfiguration(format!(
                "duplicate composite terminal reviewer entry `{name}`"
            )));
        }
        let mut reviewers = self.reviewers.clone();
        reviewers.push(TerminalReviewerEntry { name, reviewer });
        let descriptor = composite_descriptor(&self.name, &self.version, self.mode, &reviewers)?;
        self.reviewers = reviewers;
        self.descriptor = descriptor;
        Ok(())
    }

    /// Returns the composition strategy.
    pub const fn mode(&self) -> CompositeTerminalReviewMode {
        self.mode
    }

    /// Returns the number of configured reviewers.
    pub fn len(&self) -> usize {
        self.reviewers.len()
    }

    /// Returns whether no reviewers are configured.
    pub fn is_empty(&self) -> bool {
        self.reviewers.is_empty()
    }
}

impl TerminalReviewer for CompositeTerminalReviewer {
    fn descriptor(&self) -> &TerminalReviewerDescriptor {
        &self.descriptor
    }

    fn review_terminal<'a>(
        &'a self,
        request: TerminalReviewRequest,
        run: &'a RunContext,
    ) -> TerminalReviewFuture<'a> {
        Box::pin(async move {
            request.validate()?;
            if self.reviewers.is_empty() {
                return Err(TerminalReviewError::InvalidConfiguration(
                    "composite terminal reviewer requires at least one reviewer".into(),
                ));
            }
            let mut repairs = Vec::new();
            for entry in &self.reviewers {
                let verdict = entry
                    .reviewer
                    .review_terminal(request.clone(), run)
                    .await
                    .map_err(TerminalReviewError::bounded)?;
                verdict.validate().map_err(TerminalReviewError::bounded)?;
                match verdict {
                    TerminalReviewVerdict::Approve => {}
                    TerminalReviewVerdict::Reject { reason } => {
                        return TerminalReviewVerdict::reject(reason);
                    }
                    TerminalReviewVerdict::Repair { feedback } => {
                        if self.mode == CompositeTerminalReviewMode::FirstFailure {
                            return TerminalReviewVerdict::repair(feedback);
                        }
                        repairs.push(json!({
                            "reviewer": entry.name,
                            "feedback": feedback,
                        }));
                    }
                }
            }
            if repairs.is_empty() {
                Ok(TerminalReviewVerdict::approve())
            } else {
                TerminalReviewVerdict::repair(json!({
                    "kind": "composite",
                    "reviews": repairs,
                }))
            }
        })
    }
}

impl fmt::Debug for CompositeTerminalReviewer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompositeTerminalReviewer")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("descriptor", &self.descriptor)
            .field("mode", &self.mode)
            .field(
                "reviewers",
                &self
                    .reviewers
                    .iter()
                    .map(|entry| entry.name.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

fn composite_descriptor(
    name: &str,
    version: &str,
    mode: CompositeTerminalReviewMode,
    reviewers: &[TerminalReviewerEntry],
) -> Result<TerminalReviewerDescriptor, TerminalReviewError> {
    TerminalReviewerDescriptor::new(
        name,
        version,
        &json!({
            "kind": "composite_terminal",
            "mode": mode,
            "reviewers": reviewers.iter().map(|entry| json!({
                "entry": entry.name,
                "descriptor": entry.reviewer.descriptor(),
            })).collect::<Vec<_>>(),
        }),
    )
}

fn validate_identifier(field: &str, value: &str) -> Result<(), TerminalReviewError> {
    if value.is_empty()
        || value.len() > MAX_REVIEWER_NAME_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(TerminalReviewError::InvalidConfiguration(format!(
            "{field} must contain 1..={MAX_REVIEWER_NAME_BYTES} ASCII letters, digits, `_`, `-`, or `.`"
        )));
    }
    Ok(())
}

fn truncate_utf8(mut value: String, maximum: usize) -> String {
    if value.len() <= maximum {
        return value;
    }
    let mut end = maximum;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value.truncate(end);
    value
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes.iter().fold(
        String::with_capacity(bytes.len().saturating_mul(2)),
        |mut encoded, byte| {
            encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
            encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
            encoded
        },
    )
}

pub(crate) fn validate_bounded_text(
    field: &str,
    value: &str,
    maximum: usize,
    error: fn(String) -> TerminalReviewError,
) -> Result<(), TerminalReviewError> {
    if value.trim().is_empty() {
        return Err(error(format!("{field} cannot be blank")));
    }
    if value.len() > maximum {
        return Err(error(format!("{field} exceeds {maximum} bytes")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        TerminalReviewError, TerminalReviewVerdict, TerminalReviewerDescriptor,
        TerminalRuleReviewer,
    };

    #[test]
    fn repair_feedback_and_rejection_reasons_are_bounded() {
        assert!(matches!(
            TerminalReviewVerdict::repair(Value::Null),
            Err(TerminalReviewError::InvalidVerdict(_))
        ));
        assert!(matches!(
            TerminalReviewVerdict::repair(json!({"body": "x".repeat(65_537)})),
            Err(TerminalReviewError::InvalidVerdict(_))
        ));
        assert!(matches!(
            TerminalReviewVerdict::reject("   "),
            Err(TerminalReviewError::InvalidVerdict(_))
        ));
    }

    #[test]
    fn rule_reviewer_requires_a_stable_name() {
        let result = TerminalRuleReviewer::new("not a stable name", "v1", |_| {
            Ok(TerminalReviewVerdict::approve())
        });

        assert!(matches!(
            result,
            Err(TerminalReviewError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn descriptor_fingerprint_changes_with_configuration() {
        let first =
            TerminalReviewerDescriptor::new("review", "v1", &json!({"threshold": 1})).unwrap();
        let same =
            TerminalReviewerDescriptor::new("review", "v1", &json!({"threshold": 1})).unwrap();
        let changed =
            TerminalReviewerDescriptor::new("review", "v1", &json!({"threshold": 2})).unwrap();

        assert_eq!(first, same);
        assert_ne!(first, changed);
        assert_eq!(first.configuration_sha256().len(), 64);
    }

    #[test]
    fn reviewer_execution_errors_are_unicode_safely_bounded() {
        let bounded = TerminalReviewError::Execution("界".repeat(2_000)).bounded();

        let TerminalReviewError::Execution(message) = bounded else {
            panic!("execution error kind must be retained");
        };
        assert!(message.len() <= 4_096);
        assert!(message.is_char_boundary(message.len()));
    }
}
