//! Conservative retry-safety classification shared by HTTP providers.

use reqwest::StatusCode;
use runifold_core::RetrySafety;
use runifold_model::ModelError;

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

pub(crate) fn classify_transport(error: &reqwest::Error, model_error: &mut ModelError) {
    #[cfg(not(target_arch = "wasm32"))]
    if error.is_connect() {
        model_error.retry_safety = RetrySafety::Safe;
    }

    #[cfg(target_arch = "wasm32")]
    let _ = (error, model_error);
}

pub(crate) fn classify_status(status: StatusCode, model_error: &mut ModelError) {
    if status == StatusCode::TOO_MANY_REQUESTS {
        model_error.retry_safety = RetrySafety::Safe;
    }
}

#[cfg(test)]
mod tests {
    use runifold_model::{ModelError, ModelErrorKind};

    use super::*;

    #[test]
    fn only_explicit_rejection_is_safe_by_default() {
        let mut rate_limited = ModelError::local(ModelErrorKind::Provider, "rate limited");
        classify_status(StatusCode::TOO_MANY_REQUESTS, &mut rate_limited);
        assert_eq!(rate_limited.retry_safety, RetrySafety::Safe);

        let mut server_error = ModelError::local(ModelErrorKind::Provider, "failed");
        classify_status(StatusCode::INTERNAL_SERVER_ERROR, &mut server_error);
        assert_eq!(server_error.retry_safety, RetrySafety::Unknown);
    }

    #[tokio::test]
    async fn transport_messages_do_not_export_endpoint_credentials_or_query_values() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let failure = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap()
            .get(format!(
                "http://{address}/private-path?api_key=private-query-secret"
            ))
            .send()
            .await
            .unwrap_err();
        assert!(!transport_failure_message(&failure).contains("private"));
        assert!(!transport_failure_message(&failure).contains("api_key"));
        assert!(http_failure_message(StatusCode::TOO_MANY_REQUESTS).contains("429"));
    }
}
