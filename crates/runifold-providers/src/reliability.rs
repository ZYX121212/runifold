//! Conservative retry-safety classification shared by HTTP providers.

use reqwest::StatusCode;
use runifold_core::RetrySafety;
use runifold_model::ModelError;

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
}
