use std::{num::NonZeroU64, time::Duration};

use runifold_core::CheckpointId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

const MAX_SIGNAL_PAYLOAD_BYTES: usize = 1_048_576;
const MAX_INTERRUPT_PROMPT_BYTES: usize = 16_384;
const MAX_INTERRUPT_REJECTION_BYTES: usize = 4_096;
const INTERRUPT_SIGNAL_PREFIX: &str = "__runifold.interrupt.";

/// Invalid durable-wait or external-signal input.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowWaitError {
    /// A portable signal name was required.
    #[error("signal name must contain 1..=128 portable ASCII characters")]
    InvalidSignalName,
    /// Timer durations must fit in positive whole milliseconds.
    #[error("durable timer must fit in a positive whole-millisecond duration")]
    InvalidTimerDuration,
    /// Retention periods must fit in positive whole milliseconds.
    #[error("signal retention must fit in a positive whole-millisecond duration")]
    InvalidRetention,
    /// Signal payloads are deliberately bounded before persistence.
    #[error("signal payload exceeds the 1 MiB durable limit")]
    SignalPayloadTooLarge,
    /// Human-review prompts are deliberately bounded before persistence.
    #[error("interrupt prompt must contain 1..=16384 bytes")]
    InvalidInterruptPrompt,
    /// Edited human-review values are deliberately bounded before persistence.
    #[error("interrupt decision payload exceeds the 1 MiB durable limit")]
    InterruptPayloadTooLarge,
    /// Rejection explanations are deliberately bounded before persistence.
    #[error("interrupt rejection reason must contain 1..=4096 bytes")]
    InvalidInterruptRejection,
}

/// Validated external-signal name.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WorkflowSignalName(String);

impl WorkflowSignalName {
    /// Validates a portable signal name.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or non-portable names.
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkflowWaitError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 128
            && !value.starts_with(INTERRUPT_SIGNAL_PREFIX)
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
        valid
            .then_some(Self(value))
            .ok_or(WorkflowWaitError::InvalidSignalName)
    }

    /// Returns the validated signal name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Globally stable idempotency identity of one external signal publication.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WorkflowSignalId(CheckpointId);

impl WorkflowSignalId {
    /// Generates a time-ordered signal identity.
    pub fn new() -> Self {
        Self(CheckpointId::new())
    }

    /// Uses an existing durable identity.
    pub const fn from_checkpoint_id(id: CheckpointId) -> Self {
        Self(id)
    }

    /// Returns the underlying UUID-backed identity.
    pub const fn as_checkpoint_id(self) -> CheckpointId {
        self.0
    }
}

impl Default for WorkflowSignalId {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable identity of one durable human-review request.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WorkflowInterruptId(CheckpointId);

impl WorkflowInterruptId {
    /// Generates a time-ordered interrupt identity.
    pub fn new() -> Self {
        Self(CheckpointId::new())
    }

    /// Uses an existing durable identity.
    pub const fn from_checkpoint_id(id: CheckpointId) -> Self {
        Self(id)
    }

    /// Returns the underlying UUID-backed identity.
    pub const fn as_checkpoint_id(self) -> CheckpointId {
        self.0
    }

    #[doc(hidden)]
    pub fn signal_name(self) -> WorkflowSignalName {
        WorkflowSignalName(format!("{INTERRUPT_SIGNAL_PREFIX}{}", self.0))
    }
}

impl Default for WorkflowInterruptId {
    fn default() -> Self {
        Self::new()
    }
}

/// Persisted request presented to a human reviewer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowInterruptRequest {
    /// Stable identity used by the decision command.
    pub interrupt_id: WorkflowInterruptId,
    /// Safe application-authored review instruction.
    pub prompt: String,
    /// Canonical value proposed by the preceding workflow node.
    pub proposal: Value,
}

impl WorkflowInterruptRequest {
    /// Creates a new durable review request.
    ///
    /// # Errors
    ///
    /// Rejects blank or oversized prompts and proposals above 1 MiB.
    pub fn new(prompt: impl Into<String>, proposal: Value) -> Result<Self, WorkflowWaitError> {
        Self::with_id(WorkflowInterruptId::new(), prompt, proposal)
    }

    /// Reconstructs a request with an existing durable identity.
    ///
    /// # Errors
    ///
    /// Rejects blank or oversized prompts and proposals above 1 MiB.
    pub fn with_id(
        interrupt_id: WorkflowInterruptId,
        prompt: impl Into<String>,
        proposal: Value,
    ) -> Result<Self, WorkflowWaitError> {
        let prompt = prompt.into();
        Self::validate_prompt(&prompt)?;
        validate_interrupt_payload(&proposal)?;
        Ok(Self {
            interrupt_id,
            prompt,
            proposal,
        })
    }

    pub(crate) fn validate_prompt(prompt: &str) -> Result<(), WorkflowWaitError> {
        if prompt.trim().is_empty() || prompt.len() > MAX_INTERRUPT_PROMPT_BYTES {
            return Err(WorkflowWaitError::InvalidInterruptPrompt);
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn signal_name(&self) -> WorkflowSignalName {
        self.interrupt_id.signal_name()
    }
}

/// Human decision applied to one durable interrupt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowInterruptDecision {
    /// Accept the proposed value without modification.
    Approve,
    /// Replace the proposed value with a reviewed canonical value.
    Edit {
        /// Reviewer-supplied replacement.
        value: Value,
    },
    /// Reject the proposal with a bounded operator-facing explanation.
    Reject {
        /// Safe rejection explanation.
        reason: String,
    },
}

impl WorkflowInterruptDecision {
    /// Creates an approval decision.
    pub const fn approve() -> Self {
        Self::Approve
    }

    /// Creates an edited decision.
    ///
    /// # Errors
    ///
    /// Rejects values above the 1 MiB durable limit.
    pub fn edit(value: Value) -> Result<Self, WorkflowWaitError> {
        validate_interrupt_payload(&value)?;
        Ok(Self::Edit { value })
    }

    /// Creates a rejection decision.
    ///
    /// # Errors
    ///
    /// Rejects blank explanations and values above 4 KiB.
    pub fn reject(reason: impl Into<String>) -> Result<Self, WorkflowWaitError> {
        let reason = reason.into();
        if reason.trim().is_empty() || reason.len() > MAX_INTERRUPT_REJECTION_BYTES {
            return Err(WorkflowWaitError::InvalidInterruptRejection);
        }
        Ok(Self::Reject { reason })
    }

    pub(crate) fn validate(&self) -> Result<(), WorkflowWaitError> {
        match self {
            Self::Approve => Ok(()),
            Self::Edit { value } => validate_interrupt_payload(value),
            Self::Reject { reason } => {
                if reason.trim().is_empty() || reason.len() > MAX_INTERRUPT_REJECTION_BYTES {
                    Err(WorkflowWaitError::InvalidInterruptRejection)
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Idempotent control-plane command for one human-review request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowInterruptCommand {
    /// Stable publication identity used for duplicate detection.
    pub decision_id: WorkflowSignalId,
    /// Workflow task awaiting the decision.
    pub checkpoint_id: CheckpointId,
    /// Exact request being decided.
    pub interrupt_id: WorkflowInterruptId,
    /// Typed reviewer decision.
    pub decision: WorkflowInterruptDecision,
}

impl WorkflowInterruptCommand {
    /// Creates a command with a generated idempotency identity.
    ///
    /// # Errors
    ///
    /// Rejects invalid edited values or rejection explanations.
    pub fn new(
        checkpoint_id: CheckpointId,
        interrupt_id: WorkflowInterruptId,
        decision: WorkflowInterruptDecision,
    ) -> Result<Self, WorkflowWaitError> {
        Self::with_id(
            WorkflowSignalId::new(),
            checkpoint_id,
            interrupt_id,
            decision,
        )
    }

    /// Creates a command with a caller-owned idempotency identity.
    ///
    /// # Errors
    ///
    /// Rejects invalid edited values or rejection explanations.
    pub fn with_id(
        decision_id: WorkflowSignalId,
        checkpoint_id: CheckpointId,
        interrupt_id: WorkflowInterruptId,
        decision: WorkflowInterruptDecision,
    ) -> Result<Self, WorkflowWaitError> {
        decision.validate()?;
        Ok(Self {
            decision_id,
            checkpoint_id,
            interrupt_id,
            decision,
        })
    }

    pub(crate) fn into_signal(self) -> Result<WorkflowSignal, WorkflowWaitError> {
        let payload = serde_json::to_value(self.decision)
            .map_err(|_| WorkflowWaitError::InterruptPayloadTooLarge)?;
        WorkflowSignal::with_id(
            self.decision_id,
            self.checkpoint_id,
            self.interrupt_id.signal_name(),
            payload,
        )
    }
}

/// Result of submitting a typed human-review decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowInterruptDecisionOutcome {
    /// The decision was accepted before the worker committed its wait.
    Buffered,
    /// The decision atomically made the suspended workflow claimable.
    WokeWorkflow,
    /// An identical idempotency identity and decision already existed.
    Duplicate,
    /// The request was stale or the workflow had become terminal.
    DeadLettered,
}

impl From<WorkflowSignalOutcome> for WorkflowInterruptDecisionOutcome {
    fn from(value: WorkflowSignalOutcome) -> Self {
        match value {
            WorkflowSignalOutcome::Buffered => Self::Buffered,
            WorkflowSignalOutcome::WokeWorkflow => Self::WokeWorkflow,
            WorkflowSignalOutcome::Duplicate => Self::Duplicate,
            WorkflowSignalOutcome::DeadLettered => Self::DeadLettered,
        }
    }
}

/// Canonical downstream value produced by a human-review node.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowInterruptOutcome {
    /// The original proposal was approved.
    Approved {
        /// Unmodified proposed value.
        value: Value,
    },
    /// The proposal was replaced by the reviewer.
    Edited {
        /// Reviewed replacement value.
        value: Value,
    },
    /// The proposal was rejected.
    Rejected {
        /// Safe rejection explanation.
        reason: String,
    },
}

/// Durable reason for releasing a worker lease without completing a workflow.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowWait {
    /// Wake after a store-authoritative relative delay.
    Timer {
        /// Positive delay in whole milliseconds.
        delay_ms: u64,
    },
    /// Wake when a signal with this name targets the workflow checkpoint.
    Signal {
        /// Stable signal name.
        name: WorkflowSignalName,
    },
    /// Wake from the named signal or a store-authoritative timeout, whichever wins.
    SignalOrTimeout {
        /// Stable signal name.
        name: WorkflowSignalName,
        /// Positive timeout in whole milliseconds.
        timeout_ms: u64,
    },
    /// Wake when a typed human-review decision targets this request.
    Interrupt {
        /// Persisted prompt, proposal, and stable decision identity.
        request: WorkflowInterruptRequest,
    },
}

impl WorkflowWait {
    /// Creates a durable relative timer.
    ///
    /// # Errors
    ///
    /// Rejects zero, sub-millisecond, or overflowing durations.
    pub fn timer(delay: Duration) -> Result<Self, WorkflowWaitError> {
        let delay_ms = u64::try_from(delay.as_millis())
            .ok()
            .filter(|value| *value > 0)
            .ok_or(WorkflowWaitError::InvalidTimerDuration)?;
        Ok(Self::Timer { delay_ms })
    }

    /// Creates a named signal wait.
    pub const fn signal(name: WorkflowSignalName) -> Self {
        Self::Signal { name }
    }

    /// Creates a named signal wait with a durable timeout.
    ///
    /// # Errors
    ///
    /// Rejects zero, sub-millisecond, or overflowing durations.
    pub fn signal_or_timeout(
        name: WorkflowSignalName,
        timeout: Duration,
    ) -> Result<Self, WorkflowWaitError> {
        let timeout_ms = u64::try_from(timeout.as_millis())
            .ok()
            .filter(|value| *value > 0)
            .ok_or(WorkflowWaitError::InvalidTimerDuration)?;
        Ok(Self::SignalOrTimeout { name, timeout_ms })
    }
}

/// Durable value that caused a suspended workflow to become claimable.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowWake {
    /// A store-authoritative timer elapsed.
    Timer,
    /// The timeout side of a signal-or-timeout wait won.
    Timeout,
    /// A matching external signal was consumed.
    Signal {
        /// Stable publication identity.
        signal_id: WorkflowSignalId,
        /// Matched signal name.
        name: WorkflowSignalName,
        /// Canonical signal payload.
        payload: Value,
    },
}

impl WorkflowWake {
    pub(crate) fn matches(&self, wait: &WorkflowWait) -> bool {
        match (self, wait) {
            (Self::Timer, WorkflowWait::Timer { .. })
            | (Self::Timeout, WorkflowWait::SignalOrTimeout { .. }) => true,
            (Self::Signal { name: actual, .. }, WorkflowWait::Signal { name: expected }) => {
                actual == expected
            }
            (
                Self::Signal { name: actual, .. },
                WorkflowWait::SignalOrTimeout { name: expected, .. },
            ) => actual == expected,
            (Self::Signal { name: actual, .. }, WorkflowWait::Interrupt { request }) => {
                *actual == request.signal_name()
            }
            _ => false,
        }
    }
}

fn validate_interrupt_payload(value: &Value) -> Result<(), WorkflowWaitError> {
    if serde_json::to_vec(value).is_ok_and(|encoded| encoded.len() > MAX_SIGNAL_PAYLOAD_BYTES) {
        Err(WorkflowWaitError::InterruptPayloadTooLarge)
    } else {
        Ok(())
    }
}

/// Canonical output of a signal-or-timeout workflow node.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowWaitOutcome {
    /// The external signal won.
    Signal {
        /// Stable publication identity.
        signal_id: WorkflowSignalId,
        /// Matched signal name.
        name: WorkflowSignalName,
        /// Canonical signal payload.
        payload: Value,
    },
    /// Store-authoritative time elapsed before a matching signal arrived.
    TimedOut,
}

/// Idempotent external event targeted at one workflow checkpoint.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowSignal {
    /// Stable publication identity.
    pub signal_id: WorkflowSignalId,
    /// Target workflow checkpoint.
    pub checkpoint_id: CheckpointId,
    /// Signal name awaited by the workflow definition.
    pub name: WorkflowSignalName,
    /// Canonical payload delivered as the wait-node output.
    pub payload: Value,
}

impl WorkflowSignal {
    /// Creates a signal with a generated idempotency identity.
    ///
    /// # Errors
    ///
    /// Rejects payloads larger than the durable limit.
    pub fn new(
        checkpoint_id: CheckpointId,
        name: WorkflowSignalName,
        payload: Value,
    ) -> Result<Self, WorkflowWaitError> {
        Self::with_id(WorkflowSignalId::new(), checkpoint_id, name, payload)
    }

    /// Creates a signal with a caller-supplied idempotency identity.
    ///
    /// # Errors
    ///
    /// Rejects payloads larger than the durable limit.
    pub fn with_id(
        signal_id: WorkflowSignalId,
        checkpoint_id: CheckpointId,
        name: WorkflowSignalName,
        payload: Value,
    ) -> Result<Self, WorkflowWaitError> {
        if serde_json::to_vec(&payload)
            .is_ok_and(|encoded| encoded.len() > MAX_SIGNAL_PAYLOAD_BYTES)
        {
            return Err(WorkflowWaitError::SignalPayloadTooLarge);
        }
        Ok(Self {
            signal_id,
            checkpoint_id,
            name,
            payload,
        })
    }
}

/// Result of an idempotent signal publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowSignalOutcome {
    /// The signal was buffered before its matching wait was installed.
    Buffered,
    /// The signal atomically made its waiting workflow claimable.
    WokeWorkflow,
    /// An identical signal identity and payload had already been accepted.
    Duplicate,
    /// The target was already terminal or the signal lost its durable timeout race.
    DeadLettered,
}

/// Durable lifecycle state of an accepted signal identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowSignalState {
    /// Accepted and available to a future matching wait.
    Pending,
    /// Atomically consumed by a matching wait.
    Consumed,
    /// Retained for audit but no longer eligible for delivery.
    DeadLettered,
}

/// Safe signal metadata that deliberately excludes the payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowSignalSnapshot {
    /// Stable publication identity.
    pub signal_id: WorkflowSignalId,
    /// Tenant that owns the target workflow and signal identity.
    pub tenant_id: crate::WorkflowTenantId,
    /// Target workflow checkpoint.
    pub checkpoint_id: CheckpointId,
    /// Validated signal name.
    pub name: WorkflowSignalName,
    /// Current delivery lifecycle.
    pub state: WorkflowSignalState,
    /// Store-authoritative acceptance time.
    pub accepted_at_ms: u64,
}

/// Retention period for consumed and dead-letter signal identities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowSignalRetention(NonZeroU64);

impl WorkflowSignalRetention {
    /// Creates a positive whole-millisecond retention period.
    ///
    /// # Errors
    ///
    /// Rejects zero, sub-millisecond, or overflowing durations.
    pub fn new(duration: Duration) -> Result<Self, WorkflowWaitError> {
        let millis = u64::try_from(duration.as_millis())
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or(WorkflowWaitError::InvalidRetention)?;
        Ok(Self(millis))
    }

    /// Returns the normalized retention period.
    pub const fn as_millis(self) -> u64 {
        self.0.get()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn wait_inputs_enforce_portable_bounded_values() {
        assert!(WorkflowSignalName::parse("approval.received").is_ok());
        assert!(WorkflowSignalName::parse("approval received").is_err());
        assert!(WorkflowWait::timer(Duration::ZERO).is_err());
        assert!(WorkflowWait::timer(Duration::from_nanos(1)).is_err());
        assert!(WorkflowWait::timer(Duration::from_millis(1)).is_ok());
        assert!(
            WorkflowWait::signal_or_timeout(
                WorkflowSignalName::parse("approval").unwrap(),
                Duration::ZERO,
            )
            .is_err()
        );
    }

    #[test]
    fn signal_payload_is_bounded_before_persistence() {
        let oversized = Value::String("x".repeat(MAX_SIGNAL_PAYLOAD_BYTES));
        let error = WorkflowSignal::new(
            CheckpointId::new(),
            WorkflowSignalName::parse("payload").unwrap(),
            oversized,
        )
        .unwrap_err();

        assert_eq!(error, WorkflowWaitError::SignalPayloadTooLarge);
        assert!(
            WorkflowSignal::new(
                CheckpointId::new(),
                WorkflowSignalName::parse("payload").unwrap(),
                json!({"small": true}),
            )
            .is_ok()
        );
    }

    #[test]
    fn interrupt_inputs_are_bounded_and_reserved_from_external_signals() {
        assert!(WorkflowSignalName::parse(format!("{INTERRUPT_SIGNAL_PREFIX}forged")).is_err());
        assert!(WorkflowInterruptRequest::new(" ", json!({"amount": 42})).is_err());
        assert!(
            WorkflowInterruptRequest::new(
                "x".repeat(MAX_INTERRUPT_PROMPT_BYTES + 1),
                json!({"amount": 42}),
            )
            .is_err()
        );
        assert!(
            WorkflowInterruptDecision::edit(Value::String("x".repeat(MAX_SIGNAL_PAYLOAD_BYTES)))
                .is_err()
        );
        assert!(WorkflowInterruptDecision::reject(" ").is_err());
        assert!(
            WorkflowInterruptDecision::reject("x".repeat(MAX_INTERRUPT_REJECTION_BYTES + 1))
                .is_err()
        );
    }

    #[test]
    fn interrupt_command_round_trips_for_remote_control_planes() {
        let command = WorkflowInterruptCommand::new(
            CheckpointId::new(),
            WorkflowInterruptId::new(),
            WorkflowInterruptDecision::edit(json!({"amount": 40})).unwrap(),
        )
        .unwrap();

        let encoded = serde_json::to_value(&command).unwrap();
        assert_eq!(
            serde_json::from_value::<WorkflowInterruptCommand>(encoded).unwrap(),
            command
        );
    }
}
