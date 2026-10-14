//! Serializable correlation for application-owned asynchronous tool jobs.

use std::collections::BTreeMap;

use runifold_model::{
    ContentPart, Message, ModelError, ModelErrorKind, Role, ToolCall, ToolResult,
};
use serde::{Deserialize, Serialize};

const MAX_TRACKED_CALLS: usize = 256;

/// Scoped correlation state for async calls and delayed results.
///
/// This registry never executes a tool, grants authority or starts a task.
/// The host executes work through its normal Tool/Effect path and persists this
/// state with its job records. Dropping it does not cancel external jobs.
#[derive(Clone, Deserialize, Serialize)]
pub struct OpenAiAsyncToolState {
    provider: String,
    scope: String,
    calls: BTreeMap<String, AsyncCall>,
}

#[derive(Clone, Deserialize, Serialize)]
struct AsyncCall {
    call: ToolCall,
    result: Option<ToolResult>,
}

impl std::fmt::Debug for OpenAiAsyncToolState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiAsyncToolState")
            .field("calls", &self.calls.len())
            .finish_non_exhaustive()
    }
}

impl OpenAiAsyncToolState {
    /// Binds a bounded registry to an explicit provider and application session scope.
    ///
    /// # Errors
    /// Rejects blank or oversized identities. The host authenticates the scope.
    pub fn new(provider: impl Into<String>, scope: impl Into<String>) -> Result<Self, ModelError> {
        let state = Self {
            provider: provider.into(),
            scope: scope.into(),
            calls: BTreeMap::new(),
        };
        state.validate(&state.scope)?;
        Ok(state)
    }

    /// Registers a completed async call before the host starts its job.
    ///
    /// Returns false for identical replay. A reused call ID with different input
    /// fails before dispatch. Registration does not prove a job started or finished.
    ///
    /// # Errors
    /// Rejects scope mismatch, invalid state, non-async calls and identity conflicts.
    pub fn register(&mut self, scope: &str, call: ToolCall) -> Result<bool, ModelError> {
        self.validate(scope)?;
        if call.id.is_empty()
            || call.name.is_empty()
            || call
                .metadata
                .get(&format!("{}.async", self.provider))
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            return Err(invalid(
                "register only identified async calls from the bound provider",
            ));
        }
        if let Some(existing) = self.calls.get(&call.id) {
            if existing.call != call {
                return Err(invalid(
                    "async call identity was reused with different input",
                ));
            }
            return Ok(false);
        }
        if self.calls.len() >= MAX_TRACKED_CALLS {
            return Err(invalid(
                "async registry reached 256 calls; retain this scope and start a new bounded session",
            ));
        }
        self.calls
            .insert(call.id.clone(), AsyncCall { call, result: None });
        Ok(true)
    }

    /// Returns outstanding calls for host inspection without changing job ownership.
    pub fn pending(&self) -> impl Iterator<Item = &ToolCall> {
        self.calls
            .values()
            .filter(|record| record.result.is_none())
            .map(|record| &record.call)
    }

    /// Binds a completed job result to its original call and produces model input.
    ///
    /// Identical result replay returns the same message; conflicting completion
    /// fails. The host must first verify its Effect receipt and authority policy.
    ///
    /// # Errors
    /// Rejects scope mismatch, unknown calls, changed tool names and result conflicts.
    pub fn resolve(&mut self, scope: &str, mut result: ToolResult) -> Result<Message, ModelError> {
        self.validate(scope)?;
        let record = self
            .calls
            .get_mut(&result.call_id)
            .ok_or_else(|| invalid("async result has no registered call"))?;
        if result
            .name
            .as_deref()
            .is_some_and(|name| name != record.call.name)
        {
            return Err(invalid("async result changed the registered tool name"));
        }
        result.name = Some(record.call.name.clone());
        for field in ["caller", "tool_type"] {
            let key = format!("{}.{field}", self.provider);
            let compatible = format!("openai-compatible.{field}");
            result.metadata.remove(&compatible);
            if let Some(value) = record
                .call
                .metadata
                .get(&key)
                .or_else(|| record.call.metadata.get(&compatible))
            {
                result.metadata.insert(key, value.clone());
            } else {
                result.metadata.remove(&key);
            }
        }
        if record
            .result
            .as_ref()
            .is_some_and(|existing| existing != &result)
        {
            return Err(invalid(
                "async call already has a different completed result",
            ));
        }
        record.result = Some(result.clone());
        Message::new(Role::Tool, vec![ContentPart::ToolResult(result)])
    }

    /// Validates restored correlation state against the authenticated host scope.
    ///
    /// # Errors
    /// Rejects malformed snapshots, scope drift and inconsistent call identities.
    pub fn validate(&self, scope: &str) -> Result<(), ModelError> {
        if self.provider.trim().is_empty()
            || self.provider.len() > 128
            || self.scope.trim().is_empty()
            || self.scope.len() > 512
            || self.scope != scope
            || self.calls.len() > MAX_TRACKED_CALLS
        {
            return Err(invalid(
                "async correlation state does not match its bounded provider/session scope",
            ));
        }
        if self.calls.iter().any(|(id, record)| {
            id != &record.call.id
                || id.is_empty()
                || record.call.name.is_empty()
                || record
                    .call
                    .metadata
                    .get(&format!("{}.async", self.provider))
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
                || record.result.as_ref().is_some_and(|result| {
                    result.call_id != *id
                        || result.name.as_deref() != Some(record.call.name.as_str())
                        || ["caller", "tool_type"].into_iter().any(|field| {
                            let key = format!("{}.{field}", self.provider);
                            let compatible = format!("openai-compatible.{field}");
                            (compatible != key && result.metadata.contains_key(&compatible))
                                || result.metadata.get(&key)
                                    != record
                                        .call
                                        .metadata
                                        .get(&key)
                                        .or_else(|| record.call.metadata.get(&compatible))
                        })
                })
        }) {
            return Err(invalid(
                "async correlation snapshot contains inconsistent identities",
            ));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> ModelError {
    ModelError::local(ModelErrorKind::InvalidRequest, message)
}
