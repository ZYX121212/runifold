//! Budgeted projection of canonical transcripts; never an execution engine.

use std::{collections::BTreeSet, sync::Arc};

use runifold_model::{ContentPart, Message, ModelRequest, Role};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::conversation::{is_transient_context, is_turn_start};

pub(super) const INPUT_MARKER: &str = "runifold.context.input.v1";
pub(super) const PLANS: &str = "runifold.context.plans.v1";
pub(crate) const REFERENCE_IDS: &str = "runifold.context.reference_ids.v1";
pub(crate) const SUMMARY_THROUGH: &str = "runifold.context.summary_through.v1";

/// Local context failures contain counts and categories, never private payloads.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ContextError {
    /// Invalid budget, output maximum or component identity.
    #[error(
        "invalid context configuration; use a nonzero window, output reserve, output maximum and component version"
    )]
    InvalidConfiguration,
    /// Required input cannot be projected into the available input budget.
    #[error(
        "context requires {required} input tokens but only {available} are available; reduce input or use a larger window"
    )]
    Overflow {
        /// Required input accounting after permitted exclusions.
        required: u64,
        /// Input allowance after output reservation and margin.
        available: u64,
    },
    /// Output generation could exceed the reserved space.
    #[error(
        "max_output_tokens exceeds the context output reserve; increase the reserve or lower the output maximum"
    )]
    OutputReserveExceeded,
    /// Rich content needs an explicit, model-aware counter.
    #[error("context contains unsupported rich content; configure a model-aware token counter")]
    UnsupportedContent,
    /// Checked arithmetic or canonical encoding failed.
    #[error("context accounting failed")]
    Accounting,
    /// A strategy attempted to exclude protected or invalid groups.
    #[error("context strategy returned duplicate, protected or unknown group indices")]
    InvalidSelection,
    /// An exclusion would split an original Tool exchange.
    #[error(
        "context selection would split a Tool call/result exchange; retain the complete exchange"
    )]
    SplitToolExchange,
    /// A persisted projection differs from the request being recovered.
    #[error(
        "persisted context plan does not match this request; restore the original execution definition"
    )]
    RecoveryMismatch,
}

/// Stable identity of deterministic context code; bump version when behavior changes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextComponentDescriptor {
    id: String,
    version: u32,
}

impl ContextComponentDescriptor {
    /// Creates a payload-free component identity.
    ///
    /// # Errors
    /// Rejects empty, oversized or non-identifier names and zero versions.
    pub fn new(id: impl Into<String>, version: u32) -> Result<Self, ContextError> {
        let id = id.into();
        if id.is_empty()
            || id.len() > 128
            || version == 0
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(ContextError::InvalidConfiguration);
        }
        Ok(Self { id, version })
    }
    /// Returns the stable non-sensitive component identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the declared behavior version.
    pub const fn version(&self) -> u32 {
        self.version
    }
}

/// Input capacity and output reservation for each model call, not cumulative usage.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextBudget {
    window: u64,
    output_reserve: u64,
    safety_margin: u64,
}

impl ContextBudget {
    /// Reserves output and framing uncertainty before assigning input space.
    ///
    /// # Errors
    /// Rejects zero output reserve, arithmetic overflow and no remaining input space.
    pub fn new(window: u64, output_reserve: u64, safety_margin: u64) -> Result<Self, ContextError> {
        let budget = Self {
            window,
            output_reserve,
            safety_margin,
        };
        budget.validate()?;
        Ok(budget)
    }

    fn validate(self) -> Result<(), ContextError> {
        if self.output_reserve == 0
            || self
                .output_reserve
                .checked_add(self.safety_margin)
                .is_none_or(|reserved| reserved >= self.window)
        {
            return Err(ContextError::InvalidConfiguration);
        }
        Ok(())
    }

    /// Returns the configured model window.
    pub const fn window(self) -> u64 {
        self.window
    }

    /// Returns reserved generation capacity.
    pub const fn output_reserve(self) -> u64 {
        self.output_reserve
    }

    /// Returns additional capacity withheld for accounting uncertainty.
    pub const fn safety_margin(self) -> u64 {
        self.safety_margin
    }

    /// Returns input capacity after output and safety reservations.
    pub fn input_limit(self) -> u64 {
        self.window
            .saturating_sub(self.output_reserve)
            .saturating_sub(self.safety_margin)
    }
}

/// Additive, deterministic local accounting for one model's canonical input.
///
/// Count framing, schemas, provider tools/options and output-format overhead in
/// `count_overhead`; do not count messages there. Count nested content in
/// `count_message`. Implementations must be bounded, free of I/O, and versioned.
/// Unknown media costs must fail explicitly. Estimates are not billed usage.
pub trait ContextTokenCounter: Send + Sync {
    /// Stable accounting behavior identity.
    fn descriptor(&self) -> ContextComponentDescriptor;
    /// Whether this counter estimates rather than measures model input tokens.
    fn is_estimate(&self) -> bool;
    /// Counts all non-message input overhead, including protocol framing.
    ///
    /// # Errors
    /// Reject unsupported request controls and accounting overflow.
    fn count_overhead(&self, request: &ModelRequest) -> Result<u64, ContextError>;
    /// Counts one complete canonical message, including its framing.
    ///
    /// # Errors
    /// Reject unknown content costs and accounting overflow.
    fn count_message(&self, message: &Message) -> Result<u64, ContextError>;
}

/// Approximate text-only accounting: canonical JSON bytes plus framing allowance.
///
/// This is not a provider tokenizer or a guaranteed upper bound. Use a model-aware
/// implementation and a suitable margin for production. Media and opaque content
/// are rejected even when nested in Tool results.
#[derive(Clone, Copy, Debug, Default)]
pub struct SerializedBytesCounter;

impl ContextTokenCounter for SerializedBytesCounter {
    fn descriptor(&self) -> ContextComponentDescriptor {
        ContextComponentDescriptor {
            id: "runifold.serialized-bytes".into(),
            version: 1,
        }
    }

    fn is_estimate(&self) -> bool {
        true
    }

    fn count_overhead(&self, request: &ModelRequest) -> Result<u64, ContextError> {
        encoded_bytes(&RequestOverhead(request))?
            .checked_add(256)
            .ok_or(ContextError::Accounting)
    }

    fn count_message(&self, message: &Message) -> Result<u64, ContextError> {
        fn check(parts: &[ContentPart]) -> Result<(), ContextError> {
            for part in parts {
                match part {
                    ContentPart::Text { .. }
                    | ContentPart::ToolCall(_)
                    | ContentPart::Refusal { .. }
                    | ContentPart::Citation(_)
                    | ContentPart::ResourceLink { .. } => {}
                    ContentPart::ToolResult(result) => check(&result.content)?,
                    _ => return Err(ContextError::UnsupportedContent),
                }
            }
            Ok(())
        }
        check(&message.content)?;
        encoded_bytes(message)?
            .checked_add(16)
            .ok_or(ContextError::Accounting)
    }
}

// Serialize the same canonical request shape with borrowed controls and no history.
// Exhaustive destructuring makes new ModelRequest fields require explicit accounting.
struct RequestOverhead<'a>(&'a ModelRequest);

impl Serialize for RequestOverhead<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let ModelRequest {
            model,
            messages: _,
            tools,
            tool_choice,
            output_format,
            generation,
            feature_policy,
            provider_options,
            metadata,
        } = self.0;
        let mut state = serializer.serialize_struct("ModelRequest", 9)?;
        state.serialize_field("model", model)?;
        state.serialize_field("messages", &[] as &[Message])?;
        state.serialize_field("tools", tools)?;
        state.serialize_field("tool_choice", tool_choice)?;
        state.serialize_field("output_format", output_format)?;
        state.serialize_field("generation", generation)?;
        state.serialize_field("feature_policy", feature_policy)?;
        state.serialize_field("provider_options", provider_options)?;
        state.serialize_field("metadata", metadata)?;
        state.end()
    }
}

fn encoded_bytes(value: &impl Serialize) -> Result<u64, ContextError> {
    struct ByteCount(u64);
    impl std::io::Write for ByteCount {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = u64::try_from(bytes.len()).map_err(std::io::Error::other)?;
            self.0 = self
                .0
                .checked_add(length)
                .ok_or_else(|| std::io::Error::other("context accounting overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = ByteCount(0);
    serde_json::to_writer(&mut writer, value).map_err(|_| ContextError::Accounting)?;
    Ok(writer.0)
}

/// Semantic category of an indivisible input group.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ContextGroupKind {
    /// System instructions, original current input, Tools and repair feedback.
    Required,
    /// Previously committed conversation summary.
    Summary,
    /// A complete older user turn with its model and Tool messages.
    History,
    /// One transient reference document or semantic-memory batch.
    Reference,
    /// A window-truncated historical prefix without its original user input.
    /// Always excluded from transport; the archive is unchanged.
    IncompleteHistory,
}

/// Payload-free accounting and decision for an indivisible group.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextGroup {
    /// Original canonical transcript indices in stable order.
    pub message_indices: Vec<usize>,
    /// Source category.
    pub kind: ContextGroupKind,
    /// Application-supplied opaque reference IDs; never document bodies.
    pub source_ids: Vec<String>,
    /// Last archive sequence represented by a committed summary.
    pub summary_through: Option<u64>,
    /// Additive input accounting for the entire group.
    pub tokens: u64,
    /// Whether exclusions are forbidden.
    pub required: bool,
    /// Whether this group is included in the outgoing request.
    pub selected: bool,
}

/// A versioned, deterministic ordering of optional groups to exclude on overflow.
///
/// Return unique indices into `groups`, never required ones. An empty order means
/// strict rejection. The runtime controls grouping, validates the order and stops
/// excluding as soon as the input fits. Implementations must be local and bounded.
pub trait ContextSelectionStrategy: Send + Sync {
    /// Stable behavior identity, bound into checkpoint recovery.
    fn descriptor(&self) -> ContextComponentDescriptor;
    /// Chooses an eviction order without accessing private message bodies.
    fn eviction_order(&self, groups: &[ContextGroup]) -> Vec<usize>;
}

/// Excludes oldest history first, then reference documents from last to first.
#[derive(Clone, Copy, Debug, Default)]
pub struct RecentContext;

impl ContextSelectionStrategy for RecentContext {
    fn descriptor(&self) -> ContextComponentDescriptor {
        ContextComponentDescriptor {
            id: "runifold.recent-context".into(),
            version: 1,
        }
    }

    fn eviction_order(&self, groups: &[ContextGroup]) -> Vec<usize> {
        groups
            .iter()
            .enumerate()
            .filter(|(_, group)| !group.required && group.kind == ContextGroupKind::History)
            .map(|(index, _)| index)
            .chain(
                groups
                    .iter()
                    .enumerate()
                    .rev()
                    .filter(|(_, group)| {
                        !group.required && group.kind == ContextGroupKind::Reference
                    })
                    .map(|(index, _)| index),
            )
            .collect()
    }
}

/// Safe report of the model-visible projection for one internal model turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContextPlan {
    /// One-based internal model turn.
    pub turn: u32,
    /// Window and reservations used to build this plan.
    pub budget: ContextBudget,
    /// Counter identity.
    pub counter: ContextComponentDescriptor,
    /// Strategy identity.
    pub strategy: ContextComponentDescriptor,
    /// Whether counts are approximate.
    pub estimated: bool,
    /// Non-message input accounting.
    pub overhead_tokens: u64,
    /// Final selected input accounting.
    pub input_tokens: u64,
    /// Complete decisions; exclusions reflect budget or incomplete historical prefixes.
    pub groups: Vec<ContextGroup>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ContextManagerContract {
    budget: ContextBudget,
    counter: ContextComponentDescriptor,
    strategy: ContextComponentDescriptor,
    estimated: bool,
}

/// Per-call input policy, sharing the Agent's lifetime and execution authority.
///
/// ```
/// use runifold_agent::{ContextBudget, ContextManager};
/// # fn main() -> Result<(), runifold_agent::ContextError> {
/// let budget = ContextBudget::new(32_000, 4_000, 1_000)?;
/// assert_eq!(budget.input_limit(), 27_000);
/// let manager = ContextManager::new(budget);
/// // Pass manager to AgentBuilder::context_manager.
/// # let _ = manager;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ContextManager {
    budget: ContextBudget,
    counter: Arc<dyn ContextTokenCounter>,
    strategy: Arc<dyn ContextSelectionStrategy>,
}

impl ContextManager {
    /// Uses approximate text accounting and oldest-history-first selection.
    pub fn new(budget: ContextBudget) -> Self {
        Self {
            budget,
            counter: Arc::new(SerializedBytesCounter),
            strategy: Arc::new(RecentContext),
        }
    }

    /// Installs a model-aware, versioned local counter.
    #[must_use]
    pub fn with_counter(mut self, counter: Arc<dyn ContextTokenCounter>) -> Self {
        self.counter = counter;
        self
    }

    /// Installs a deterministic eviction policy; runtime protection is mandatory.
    #[must_use]
    pub fn with_strategy(mut self, strategy: Arc<dyn ContextSelectionStrategy>) -> Self {
        self.strategy = strategy;
        self
    }

    pub(crate) fn contract(&self) -> ContextManagerContract {
        ContextManagerContract {
            budget: self.budget,
            counter: self.counter.descriptor(),
            strategy: self.strategy.descriptor(),
            estimated: self.counter.is_estimate(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ContextError> {
        self.budget.validate()?;
        for descriptor in [self.counter.descriptor(), self.strategy.descriptor()] {
            ContextComponentDescriptor::new(descriptor.id, descriptor.version)?;
        }
        Ok(())
    }

    pub(super) fn reserve_output(&self, request: &mut ModelRequest) -> Result<(), ContextError> {
        if request.generation.max_output_tokens == Some(0) {
            return Err(ContextError::InvalidConfiguration);
        }
        if request
            .generation
            .max_output_tokens
            .is_some_and(|max| max > self.budget.output_reserve)
        {
            return Err(ContextError::OutputReserveExceeded);
        }
        request
            .generation
            .max_output_tokens
            .get_or_insert(self.budget.output_reserve);
        Ok(())
    }

    pub(super) fn validate_recovered_plan(
        &self,
        request: &ModelRequest,
        current: usize,
        plan: &ContextPlan,
    ) -> Result<(), ContextError> {
        let contract = self.contract();
        if plan.budget != contract.budget
            || plan.counter != contract.counter
            || plan.strategy != contract.strategy
            || plan.estimated != contract.estimated
        {
            return Err(ContextError::RecoveryMismatch);
        }
        let expected = context_groups(request, current, |_| Ok(0))?;
        if expected.len() != plan.groups.len() {
            return Err(ContextError::RecoveryMismatch);
        }
        let mut input = plan.overhead_tokens;
        for (group, expected) in plan.groups.iter().zip(expected) {
            if group.message_indices != expected.message_indices
                || group.kind != expected.kind
                || group.required != expected.required
                || group.source_ids != expected.source_ids
                || group.summary_through != expected.summary_through
                || group.required && !group.selected
                || !expected.selected && group.selected
            {
                return Err(ContextError::RecoveryMismatch);
            }
            if group.selected {
                input = input
                    .checked_add(group.tokens)
                    .ok_or(ContextError::Accounting)?;
            }
        }
        if input != plan.input_tokens || input > self.budget.input_limit() {
            return Err(ContextError::RecoveryMismatch);
        }
        validate_exchanges(request, plan)
    }

    pub(super) fn validate_legacy_accounting(
        &self,
        request: &ModelRequest,
        plan: &ContextPlan,
    ) -> Result<(), ContextError> {
        if self.counter.count_overhead(request)? != plan.overhead_tokens {
            return Err(ContextError::RecoveryMismatch);
        }
        for group in &plan.groups {
            let tokens = group
                .message_indices
                .iter()
                .try_fold(0_u64, |sum, &index| {
                    let message = request
                        .messages
                        .get(index)
                        .ok_or(ContextError::RecoveryMismatch)?;
                    sum.checked_add(self.counter.count_message(message)?)
                        .ok_or(ContextError::Accounting)
                })?;
            if tokens != group.tokens {
                return Err(ContextError::RecoveryMismatch);
            }
        }
        Ok(())
    }

    pub(super) fn plan(
        &self,
        request: &ModelRequest,
        current: usize,
        turn: u32,
    ) -> Result<ContextPlan, ContextError> {
        self.validate()?;
        let mut groups = context_groups(request, current, |message| {
            self.counter.count_message(message)
        })?;
        let overhead = self.counter.count_overhead(request)?;
        let mut input =
            groups
                .iter()
                .filter(|group| group.selected)
                .try_fold(overhead, |sum, group| {
                    sum.checked_add(group.tokens)
                        .ok_or(ContextError::Accounting)
                })?;
        let order = self.strategy.eviction_order(&groups);
        let mut seen = BTreeSet::new();
        for &index in &order {
            if groups.get(index).is_none_or(|group| group.required) || !seen.insert(index) {
                return Err(ContextError::InvalidSelection);
            }
        }
        for index in order {
            if input <= self.budget.input_limit() {
                break;
            }
            if groups[index].selected {
                groups[index].selected = false;
                input -= groups[index].tokens;
            }
        }
        if input > self.budget.input_limit() {
            return Err(ContextError::Overflow {
                required: input,
                available: self.budget.input_limit(),
            });
        }
        let plan = ContextPlan {
            turn,
            budget: self.budget,
            counter: self.counter.descriptor(),
            strategy: self.strategy.descriptor(),
            estimated: self.counter.is_estimate(),
            overhead_tokens: overhead,
            input_tokens: input,
            groups,
        };
        validate_exchanges(request, &plan)?;
        Ok(plan)
    }
}

fn context_groups(
    request: &ModelRequest,
    current: usize,
    mut count: impl FnMut(&Message) -> Result<u64, ContextError>,
) -> Result<Vec<ContextGroup>, ContextError> {
    if request
        .messages
        .get(current)
        .is_none_or(|message| message.role != Role::User || is_transient_context(message))
    {
        return Err(ContextError::RecoveryMismatch);
    }
    let mut groups: Vec<ContextGroup> = Vec::new();
    let mut history: Option<usize> = None;
    for (index, message) in request.messages.iter().enumerate() {
        let summary = message.metadata.contains_key(SUMMARY_THROUGH);
        let reference = is_transient_context(message) && !summary;
        let required = message.role == Role::System || summary || (index >= current && !reference);
        let kind = if summary {
            ContextGroupKind::Summary
        } else if required {
            ContextGroupKind::Required
        } else if reference {
            ContextGroupKind::Reference
        } else {
            ContextGroupKind::History
        };
        let new_turn = is_turn_start(message);
        let tokens = count(message)?;
        // Reference/system messages are independently protected or selected; they
        // never terminate the surrounding historical user/model/Tool exchange.
        if kind == ContextGroupKind::History
            && !new_turn
            && let Some(active) = history
        {
            groups[active].message_indices.push(index);
            groups[active].tokens = groups[active]
                .tokens
                .checked_add(tokens)
                .ok_or(ContextError::Accounting)?;
        } else {
            let incomplete = kind == ContextGroupKind::History && history.is_none() && !new_turn;
            if kind == ContextGroupKind::History {
                history = Some(groups.len());
            }
            let kind = if incomplete {
                ContextGroupKind::IncompleteHistory
            } else {
                kind
            };
            groups.push(ContextGroup {
                message_indices: vec![index],
                kind,
                tokens,
                required,
                selected: !incomplete,
                source_ids: message
                    .metadata
                    .get(REFERENCE_IDS)
                    .and_then(serde_json::Value::as_array)
                    .map(|ids| {
                        ids.iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default(),
                summary_through: message
                    .metadata
                    .get(SUMMARY_THROUGH)
                    .and_then(serde_json::Value::as_u64),
            });
        }
    }
    Ok(groups)
}

impl std::fmt::Debug for ContextManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.contract().fmt(f)
    }
}

fn validate_exchanges(request: &ModelRequest, plan: &ContextPlan) -> Result<(), ContextError> {
    let selected: BTreeSet<usize> = plan
        .groups
        .iter()
        .filter(|g| g.selected)
        .flat_map(|g| g.message_indices.iter().copied())
        .collect();
    let mut pending: std::collections::BTreeMap<&str, std::collections::VecDeque<usize>> =
        std::collections::BTreeMap::new();
    let incomplete: BTreeSet<usize> = plan
        .groups
        .iter()
        .filter(|group| group.kind == ContextGroupKind::IncompleteHistory)
        .flat_map(|group| group.message_indices.iter().copied())
        .collect();
    for (index, message) in request.messages.iter().enumerate() {
        if incomplete.contains(&index) {
            continue;
        }
        for part in &message.content {
            match part {
                ContentPart::ToolCall(call) => {
                    pending.entry(&call.id).or_default().push_back(index);
                }
                ContentPart::ToolResult(result) => {
                    let call = pending
                        .get_mut(result.call_id.as_str())
                        .and_then(std::collections::VecDeque::pop_front);
                    if call.is_some_and(|call| selected.contains(&call))
                        != selected.contains(&index)
                    {
                        return Err(ContextError::SplitToolExchange);
                    }
                }
                _ => {}
            }
        }
    }
    if pending
        .values()
        .flatten()
        .any(|index| selected.contains(index))
    {
        return Err(ContextError::SplitToolExchange);
    }
    Ok(())
}

pub(crate) fn clean_message(message: &Message) -> Message {
    Message {
        role: message.role,
        content: message.content.clone(),
        metadata: message
            .metadata
            .iter()
            .filter(|(key, _)| key.as_str() != INPUT_MARKER && key.as_str() != PLANS)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }
}

pub(super) fn strip_runtime_metadata(message: &mut Message) {
    message.metadata.remove(INPUT_MARKER);
    message.metadata.remove(PLANS);
}
