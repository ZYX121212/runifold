use std::collections::BTreeMap;

use runifold_core::RetrySafety;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// A normalized model-layer error category.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ModelErrorKind {
    /// Request data failed local validation.
    InvalidRequest,
    /// The selected model or provider does not support a required feature.
    UnsupportedFeature,
    /// A provider transport failed.
    Transport,
    /// A provider response violated its protocol.
    Protocol,
    /// Stream events violated the canonical lifecycle.
    StreamState,
    /// Accumulated tool arguments were not valid JSON.
    MalformedToolArguments,
    /// A provider rejected or failed a request.
    Provider,
    /// The model call was cancelled.
    Cancelled,
    /// The model call exceeded its deadline.
    DeadlineExceeded,
    /// Local model admission rejected the request before provider execution.
    AdmissionDenied,
}

/// A structured model-layer error.
#[derive(Clone, Debug, Deserialize, Error, PartialEq, Serialize)]
#[error("{kind:?}: {message}")]
pub struct ModelError {
    /// Normalized category.
    pub kind: ModelErrorKind,
    /// Safe human-readable explanation.
    pub message: String,
    /// Provider namespace, when known.
    pub provider: Option<String>,
    /// Retry-safety classification.
    pub retry_safety: RetrySafety,
    /// Namespaced diagnostic metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl ModelError {
    /// Projects bounded operational metadata without messages or private payloads.
    ///
    /// Only validated numeric fields, lifecycle facts, and recognized provider
    /// error classifications are retained. Unknown classifications are omitted;
    /// request IDs, parameter names, response bodies and arbitrary metadata are
    /// never copied. This projection grants no additional retry authority and
    /// does not establish remote execution or billing from a local cancellation.
    pub fn diagnostic_metadata(&self) -> BTreeMap<String, Value> {
        let mut result = BTreeMap::new();
        for key in [
            "http.status",
            "retry.after_ms",
            "retry.delay_ms",
            "retry.remaining_ms",
            "runifold.request.queue_wait_ms",
            "runifold.request.execution_ms",
        ] {
            if let Some(value) = self.metadata.get(key).and_then(Value::as_u64)
                && (key != "http.status" || (100..=599).contains(&value))
            {
                result.insert(key.into(), value.into());
            }
        }
        for key in [
            "runifold.request.provider_invoked",
            "runifold.request.local_capacity_released",
            "runifold.request.output_observed",
        ] {
            if let Some(value) = self.metadata.get(key).and_then(Value::as_bool) {
                result.insert(key.into(), value.into());
            }
        }
        for (key, allowed) in [
            (
                "runifold.request.phase",
                &["admission", "opening", "waiting_first_event", "streaming"][..],
            ),
            (
                "runifold.request.remote_outcome",
                &["not_started", "unknown"][..],
            ),
            ("runifold.scheduler.phase", &["queue", "attempt"][..]),
            (
                "runifold.scheduler.reason",
                &[
                    "queue_full",
                    "missing_token_reservation",
                    "reservation_too_large",
                    "identity_exhausted",
                    "nested_admission",
                ][..],
            ),
        ] {
            if let Some(value) = self.metadata.get(key).and_then(Value::as_str)
                && allowed.contains(&value)
            {
                result.insert(key.into(), value.into());
            }
        }
        // Providers control these values. Restrict values as well as keys:
        // even an apparently identifier-shaped string may contain a secret.
        for field in ["type", "code"] {
            let selected = [self.provider.as_deref(), Some("provider")]
                .into_iter()
                .flatten()
                .filter(|namespace| namespace.len() <= 64)
                .find_map(|namespace| {
                    let value = self
                        .metadata
                        .get(&format!("{namespace}.error.{field}"))?
                        .as_str()?;
                    recognized_classification(value).then_some(value)
                });
            if let Some(value) = selected {
                result.insert(format!("provider.error.{field}"), value.into());
            }
        }
        result
    }

    /// Returns a stable diagnostic identifier without exposing error payloads.
    ///
    /// Resolve causes and corrective actions with `runifold ai explain <code>`.
    /// This does not change the error's retry safety, Display or serialization.
    pub const fn diagnostic_code(&self) -> &'static str {
        match self.kind {
            ModelErrorKind::AdmissionDenied => "RF-PROVIDER-010",
            ModelErrorKind::InvalidRequest => "RF-PROVIDER-001",
            ModelErrorKind::UnsupportedFeature => "RF-PROVIDER-002",
            ModelErrorKind::Transport => "RF-PROVIDER-003",
            ModelErrorKind::Protocol => "RF-PROVIDER-004",
            ModelErrorKind::StreamState => "RF-PROVIDER-005",
            ModelErrorKind::MalformedToolArguments => "RF-PROVIDER-006",
            ModelErrorKind::Provider => "RF-PROVIDER-007",
            ModelErrorKind::Cancelled => "RF-PROVIDER-008",
            ModelErrorKind::DeadlineExceeded => "RF-PROVIDER-009",
        }
    }

    /// Creates a non-retryable local validation or state error.
    pub fn local(kind: ModelErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            provider: None,
            retry_safety: RetrySafety::Unknown,
            metadata: BTreeMap::new(),
        }
    }
}

fn recognized_classification(value: &str) -> bool {
    matches!(
        value,
        "invalid_request_error"
            | "authentication_error"
            | "permission_error"
            | "not_found_error"
            | "rate_limit_error"
            | "rate_limit"
            | "slow_down"
            | "rate_limit_exceeded"
            | "insufficient_quota"
            | "server_error"
            | "service_unavailable"
            | "internal_error"
            | "api_error"
            | "overloaded_error"
            | "context_length_exceeded"
            | "content_policy_violation"
            | "invalid_api_key"
            | "model_not_found"
            | "upstream_error"
            | "provider_unavailable"
            | "timeout"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn existing_error_category_discriminants_remain_stable() {
        let existing = [
            ModelErrorKind::InvalidRequest,
            ModelErrorKind::UnsupportedFeature,
            ModelErrorKind::Transport,
            ModelErrorKind::Protocol,
            ModelErrorKind::StreamState,
            ModelErrorKind::MalformedToolArguments,
            ModelErrorKind::Provider,
            ModelErrorKind::Cancelled,
            ModelErrorKind::DeadlineExceeded,
        ];
        for (index, kind) in existing.into_iter().enumerate() {
            assert_eq!(kind as usize, index);
        }
    }

    #[test]
    fn diagnostic_projection_is_bounded_and_does_not_change_retry_authority() {
        let mut error = ModelError::local(ModelErrorKind::Provider, "private prompt");
        error.provider = Some("gateway".into());
        error.metadata = BTreeMap::from([
            ("http.status".into(), json!(429)),
            ("retry.after_ms".into(), json!(2_000)),
            ("gateway.error.type".into(), json!("rate_limit_error")),
            ("gateway.error.code".into(), json!("insufficient_quota")),
            ("gateway.error.param".into(), json!("private prompt")),
            ("gateway.request_id".into(), json!("private request")),
            ("private".into(), json!({"large": "x".repeat(100_000)})),
        ]);
        let before = error.clone();
        let projected = error.diagnostic_metadata();
        assert_eq!(projected.len(), 4);
        assert_eq!(projected["http.status"], 429);
        assert_eq!(projected["provider.error.type"], "rate_limit_error");
        assert!(
            !serde_json::to_string(&projected)
                .unwrap()
                .contains("private")
        );
        assert_eq!(error, before);
        assert_eq!(error.retry_safety, RetrySafety::Unknown);
    }

    #[test]
    fn diagnostic_projection_rejects_wrong_types_unknown_values_and_namespaces() {
        for status in [json!("429"), json!(-1), json!(600), json!({"secret": 429})] {
            let mut error = ModelError::local(ModelErrorKind::Provider, "private");
            error.provider = Some("gateway".into());
            error.metadata = BTreeMap::from([
                ("http.status".into(), status),
                ("retry.after_ms".into(), json!(-1)),
                ("gateway.error.type".into(), json!("sk_private_token")),
                ("gateway.error.code".into(), json!(["server_error"])),
                ("unrelated.error.type".into(), json!("server_error")),
                ("runifold.request.phase".into(), json!("private")),
                ("runifold.request.provider_invoked".into(), json!("true")),
            ]);
            assert!(error.diagnostic_metadata().is_empty());
        }
    }
}
