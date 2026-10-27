//! Conservative retry-safety classification shared by HTTP providers.

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
        400 | 422 => "review the request and structured error classification",
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
