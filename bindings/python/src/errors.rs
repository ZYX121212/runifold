//! Payload-free Python error envelopes over native error classifications.
use runifold::{AgentError, RunContext};
use serde_json::{Value, json};

pyo3::create_exception!(_native, NativeError, pyo3::exceptions::PyRuntimeError);

pub fn failure(code: &str, kind: &str, safety: &str, message: &str, run: &RunContext) -> Value {
    json!({"type":"error", "error": {"code":code,"kind":kind,"retry_safety":safety,
        "message":message,"run_id":run.run_id().to_string(),"usage":run.budget().usage(),"diagnostics":{}}})
}

pub fn agent_error(error: &AgentError, run: &RunContext) -> Value {
    let classification = error.run_error_kind();
    let kind = format!("{classification:?}");
    let mut value = failure(
        error.diagnostic_code(),
        &kind,
        &format!("{:?}", error.retry_safety()),
        classification.recommendation(),
        run,
    );
    if let AgentError::Model(model) = error {
        let diagnostics = model.diagnostic_metadata();
        value["error"]["diagnostics"] = json!(diagnostics);
        value["error"]["diagnostics"]["sdk.model_error_kind"] = json!(model.kind);
        match diagnostics.get("http.status").and_then(Value::as_u64) {
            Some(401 | 403) => {
                value["error"]["message"] = json!(
                    "Check provider credentials and endpoint permissions before starting another operation"
                )
            }
            Some(429) => {
                value["error"]["message"] = json!(
                    "Review provider quota and retry-after diagnostics; preserve the native retry-safety decision"
                )
            }
            _ => {}
        }
    }
    value
}
