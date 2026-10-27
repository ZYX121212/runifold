//! Payload-free Python error envelopes over native error classifications.
use runifold::{AgentError, RunContext};
use serde_json::{Value, json};

pyo3::create_exception!(_native, NativeError, pyo3::exceptions::PyRuntimeError);

pub fn failure(code: &str, kind: &str, safety: &str, message: &str, run: &RunContext) -> Value {
    json!({"type":"error", "error": {"code":code,"kind":kind,"retry_safety":safety,
        "message":message,"run_id":run.run_id().to_string(),"usage":run.budget().usage(),"diagnostics":{}}})
}

pub fn agent_error(error: &AgentError, run: &RunContext) -> Value {
    let kind = format!("{:?}", error.run_error_kind());
    let mut value = failure(
        error.diagnostic_code(),
        &kind,
        &format!("{:?}", error.retry_safety()),
        "Native Agent execution failed; inspect the error code and safe diagnostics",
        run,
    );
    if let AgentError::Model(model) = error {
        value["error"]["diagnostics"] = json!(model.diagnostic_metadata());
    }
    value
}
