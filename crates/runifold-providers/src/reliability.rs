//! Conservative retry-safety classification shared by HTTP providers.

/// Retains upstream text only in explicit, private diagnostic metadata.
#[cfg(any(
    feature = "anthropic",
    feature = "gemini",
    feature = "ollama",
    feature = "openai"
))]
pub(crate) fn retain_error_message(
    error: &mut runifold_model::ModelError,
    detail: &serde_json::Value,
) {
    const LIMIT: usize = 16 * 1024;
    let Some(message) = detail
        .as_str()
        .or_else(|| detail.get("message").and_then(serde_json::Value::as_str))
    else {
        return;
    };
    if message.is_empty() {
        return;
    }
    let mut end = message.len().min(LIMIT);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    error
        .metadata
        .insert("provider.error.message".into(), message[..end].into());
    if end < message.len() {
        error
            .metadata
            .insert("provider.error.message_truncated".into(), true.into());
    }
}

#[cfg(any(
    feature = "anthropic",
    feature = "gemini",
    feature = "ollama",
    feature = "openai"
))]
use reqwest::StatusCode;
use std::time::Duration;

#[cfg(any(
    feature = "anthropic",
    feature = "gemini",
    feature = "ollama",
    feature = "openai"
))]
pub(crate) fn http_failure_message(status: StatusCode) -> String {
    let action = match status.as_u16() {
        401 => "verify provider authentication",
        403 => "verify provider access policy",
        400 | 422 => {
            "review the request; inspect ModelError::provider_message() for the upstream reason"
        }
        429 => "honor retry timing and review shared provider limits",
        500..=599 => "inspect provider availability; remote execution and usage may be unknown",
        _ => "inspect the status and structured error classification",
    };
    format!("provider returned HTTP {}; {action}", status.as_u16())
}

#[cfg(any(
    feature = "anthropic",
    feature = "gemini",
    feature = "ollama",
    feature = "openai"
))]
pub(crate) fn transport_failure_message(error: &reqwest::Error) -> &'static str {
    #[cfg(not(target_arch = "wasm32"))]
    if error.is_connect() {
        return "provider connection failed; inspect network and endpoint configuration";
    }
    if error.is_timeout() {
        "provider transport timed out; remote execution and usage may be unknown"
    } else {
        "provider transport failed; remote execution and usage may be unknown"
    }
}

pub(crate) fn request_timeout(remaining: Duration) -> Duration {
    #[cfg(target_arch = "wasm32")]
    {
        browser_timeout(remaining)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        remaining
    }
}

#[cfg(any(target_arch = "wasm32", test))]
fn browser_timeout(remaining: Duration) -> Duration {
    // Fetch AbortGuard truncates to signed 32-bit milliseconds. Rounding up
    // prevents an abort just before the canonical deadline from looking like
    // a transport failure; bounding the timer also avoids a conversion panic.
    let milliseconds = remaining.as_nanos().saturating_add(999_999) / 1_000_000;
    let milliseconds = u64::try_from(milliseconds.min(i32::MAX as u128))
        .expect("the bounded browser timeout fits in u64");
    Duration::from_millis(milliseconds)
}

#[cfg(any(
    feature = "anthropic",
    feature = "gemini",
    feature = "ollama",
    feature = "openai"
))]
pub(crate) fn classify_transport(
    error: &reqwest::Error,
    model_error: &mut runifold_model::ModelError,
) {
    #[cfg(not(target_arch = "wasm32"))]
    if error.is_connect() {
        model_error.retry_safety = runifold_core::RetrySafety::Safe;
    }

    #[cfg(target_arch = "wasm32")]
    let _ = (error, model_error);
}

#[cfg(any(
    feature = "anthropic",
    feature = "gemini",
    feature = "ollama",
    feature = "openai"
))]
pub(crate) fn classify_status(
    status: reqwest::StatusCode,
    model_error: &mut runifold_model::ModelError,
) {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        model_error.retry_safety = runifold_core::RetrySafety::Safe;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(any(
        feature = "anthropic",
        feature = "gemini",
        feature = "ollama",
        feature = "openai"
    ))]
    fn upstream_message_is_bounded_and_absent_fields_do_not_invent_a_reason() {
        use runifold_model::{ModelError, ModelErrorKind};
        use serde_json::json;
        for payload in [
            json!(null),
            json!({}),
            json!({"message":42}),
            json!({"message":""}),
        ] {
            let mut error = ModelError::local(ModelErrorKind::Provider, "safe");
            retain_error_message(&mut error, &payload);
            assert_eq!(error.provider_message(), None);
        }
        let message = "中".repeat(6000);
        let mut error = ModelError::local(ModelErrorKind::Provider, "safe");
        retain_error_message(&mut error, &json!({"message":message}));
        let retained = error
            .provider_message()
            .expect("upstream message was supplied");
        assert_eq!(retained.len(), 16383);
        assert!(message.starts_with(retained));
        assert_eq!(error.metadata["provider.error.message_truncated"], true);
        assert_eq!(error.to_string(), "Provider: safe");
        assert!(
            !error
                .diagnostic_metadata()
                .contains_key("provider.error.message")
        );
    }

    #[test]
    fn browser_timeouts_round_up_without_overflowing_the_fetch_timer() {
        assert_eq!(browser_timeout(Duration::ZERO), Duration::ZERO);
        assert_eq!(
            browser_timeout(Duration::from_micros(150_001)),
            Duration::from_millis(151)
        );
        assert_eq!(
            browser_timeout(Duration::from_millis(150)),
            Duration::from_millis(150)
        );
        assert_eq!(
            browser_timeout(Duration::from_secs(u64::MAX)),
            Duration::from_millis(i32::MAX as u64)
        );
        #[cfg(not(target_arch = "wasm32"))]
        assert_eq!(
            request_timeout(Duration::from_nanos(1)),
            Duration::from_nanos(1)
        );
    }

    #[test]
    #[cfg(any(
        feature = "anthropic",
        feature = "gemini",
        feature = "ollama",
        feature = "openai"
    ))]
    fn only_explicit_rejection_is_safe_by_default() {
        use reqwest::StatusCode;
        use runifold_core::RetrySafety;
        use runifold_model::{ModelError, ModelErrorKind};
        let mut rate_limited = ModelError::local(ModelErrorKind::Provider, "rate limited");
        classify_status(StatusCode::TOO_MANY_REQUESTS, &mut rate_limited);
        assert_eq!(rate_limited.retry_safety, RetrySafety::Safe);

        let mut server_error = ModelError::local(ModelErrorKind::Provider, "failed");
        classify_status(StatusCode::INTERNAL_SERVER_ERROR, &mut server_error);
        assert_eq!(server_error.retry_safety, RetrySafety::Unknown);
    }
}
