use runifold_tool::{ToolError, ToolErrorKind, ToolOutput};
use serde_json::{Value, json};

use crate::{
    CallToolResult, CompletionError, CompletionErrorKind, ContentBlock, JsonRpcResponse,
    PromptError, PromptErrorKind, RequestId, ResourceError, ResourceErrorKind,
};

const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;

pub(crate) fn serialize_result<T: serde::Serialize>(id: RequestId, result: &T) -> JsonRpcResponse {
    match serde_json::to_value(result) {
        Ok(result) => JsonRpcResponse::success(id, result),
        Err(error) => JsonRpcResponse::error(
            id,
            INTERNAL_ERROR,
            "failed to encode result",
            Some(json!({"error": error.to_string()})),
        ),
    }
}

pub(crate) fn tool_invocation_response(
    id: RequestId,
    invocation: Result<ToolOutput, ToolError>,
) -> JsonRpcResponse {
    match invocation {
        Ok(output) if output.model_visible => {
            let text = match &output.value {
                Value::String(text) => text.clone(),
                value => value.to_string(),
            };
            let structured_content = output.value.is_object().then_some(output.value);
            serialize_result(
                id,
                &CallToolResult {
                    content: vec![ContentBlock::text(text)],
                    structured_content,
                    is_error: false,
                },
            )
        }
        Ok(_) => serialize_result(
            id,
            &CallToolResult {
                content: vec![ContentBlock::text(
                    "tool output is not permitted for model visibility",
                )],
                structured_content: None,
                is_error: true,
            },
        ),
        Err(error)
            if matches!(
                error.kind,
                ToolErrorKind::NotFound | ToolErrorKind::CapabilityDenied
            ) =>
        {
            JsonRpcResponse::error(id, METHOD_NOT_FOUND, "tool not found", None)
        }
        Err(error) => serialize_result(
            id,
            &CallToolResult {
                content: vec![ContentBlock::text(error.message)],
                structured_content: None,
                is_error: true,
            },
        ),
    }
}

pub(crate) fn resource_error_response(id: RequestId, error: &ResourceError) -> JsonRpcResponse {
    match error.kind {
        ResourceErrorKind::NotFound | ResourceErrorKind::CapabilityDenied => {
            JsonRpcResponse::error(id, -32002, "resource not found", None)
        }
        ResourceErrorKind::Cancelled => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "resource read was cancelled", None)
        }
        ResourceErrorKind::DeadlineExceeded => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "resource read deadline elapsed", None)
        }
        ResourceErrorKind::InvalidOutput | ResourceErrorKind::Execution => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "resource read failed", None)
        }
    }
}

pub(crate) fn prompt_error_response(id: RequestId, error: PromptError) -> JsonRpcResponse {
    match error.kind {
        PromptErrorKind::NotFound | PromptErrorKind::CapabilityDenied => {
            JsonRpcResponse::error(id, INVALID_PARAMS, "invalid prompt name", None)
        }
        PromptErrorKind::InvalidArguments => {
            JsonRpcResponse::error(id, INVALID_PARAMS, error.message, None)
        }
        PromptErrorKind::Cancelled => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "prompt rendering was cancelled", None)
        }
        PromptErrorKind::DeadlineExceeded => JsonRpcResponse::error(
            id,
            INTERNAL_ERROR,
            "prompt rendering deadline elapsed",
            None,
        ),
        PromptErrorKind::InvalidOutput | PromptErrorKind::Execution => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "prompt rendering failed", None)
        }
    }
}

pub(crate) fn completion_error_response(id: RequestId, error: CompletionError) -> JsonRpcResponse {
    match error.kind {
        CompletionErrorKind::NotFound | CompletionErrorKind::CapabilityDenied => {
            JsonRpcResponse::error(id, INVALID_PARAMS, "completion not found", None)
        }
        CompletionErrorKind::InvalidInput => {
            JsonRpcResponse::error(id, INVALID_PARAMS, error.message, None)
        }
        CompletionErrorKind::Cancelled => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "completion was cancelled", None)
        }
        CompletionErrorKind::DeadlineExceeded => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "completion deadline elapsed", None)
        }
        CompletionErrorKind::InvalidOutput | CompletionErrorKind::Execution => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "completion failed", None)
        }
    }
}
