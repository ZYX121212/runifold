//! Canonical request translation for Amazon Bedrock Converse Stream.

use std::collections::HashMap;

use aws_sdk_bedrockruntime::types::{
    AnyToolChoice, AutoToolChoice, ContentBlock, ConversationRole, InferenceConfiguration, Message,
    SpecificToolChoice, SystemContentBlock, Tool, ToolConfiguration, ToolInputSchema,
    ToolResultBlock, ToolResultContentBlock, ToolResultStatus, ToolSpecification, ToolUseBlock,
};
use aws_smithy_types::{Document, Number};
use runifold_model::{
    ContentPart, ModelError, ModelErrorKind, ModelRequest, OutputFormat, Role, ToolChoice,
};
use serde_json::Value;

pub(crate) struct EncodedRequest {
    pub(crate) messages: Vec<Message>,
    pub(crate) system: Vec<SystemContentBlock>,
    pub(crate) inference: InferenceConfiguration,
    pub(crate) tools: Option<ToolConfiguration>,
    pub(crate) additional_fields: Option<Document>,
}

pub(crate) fn encode_request(request: &ModelRequest) -> Result<EncodedRequest, ModelError> {
    if request.generation.seed.is_some() {
        return Err(unsupported(
            "Amazon Bedrock Converse does not expose a portable deterministic seed",
        ));
    }
    if !matches!(request.output_format, OutputFormat::Text) {
        return Err(unsupported(
            "structured output requires a model-specific Bedrock strategy",
        ));
    }

    let mut system = Vec::new();
    let mut messages = Vec::new();
    for message in &request.messages {
        if message.role == Role::System {
            for part in &message.content {
                let ContentPart::Text { text } = part else {
                    return Err(unsupported(
                        "Bedrock system instructions currently support text only",
                    ));
                };
                system.push(SystemContentBlock::Text(text.clone()));
            }
            continue;
        }

        let role = match message.role {
            Role::User | Role::Tool => ConversationRole::User,
            Role::Assistant => ConversationRole::Assistant,
            Role::System => unreachable!("system messages are handled above"),
            _ => {
                return Err(unsupported(
                    "message role is newer than this Bedrock adapter",
                ));
            }
        };
        let content = message
            .content
            .iter()
            .map(|part| encode_content(part, message.role))
            .collect::<Result<Vec<_>, _>>()?;
        messages.push(
            Message::builder()
                .role(role)
                .set_content(Some(content))
                .build()
                .map_err(build_error)?,
        );
    }
    if messages.is_empty() {
        return Err(invalid(
            "Bedrock requests require at least one user or assistant message",
        ));
    }

    let max_tokens = request
        .generation
        .max_output_tokens
        .map(|value| {
            i32::try_from(value)
                .map_err(|_| invalid("maximum output tokens exceed the Bedrock i32 limit"))
        })
        .transpose()?;
    let inference = InferenceConfiguration::builder()
        .set_max_tokens(max_tokens)
        .set_temperature(
            request
                .generation
                .temperature
                .map(|value| finite_f32(value, "temperature"))
                .transpose()?,
        )
        .set_top_p(
            request
                .generation
                .top_p
                .map(|value| finite_f32(value, "top_p"))
                .transpose()?,
        )
        .set_stop_sequences(
            (!request.generation.stop.is_empty()).then(|| request.generation.stop.clone()),
        )
        .build();

    Ok(EncodedRequest {
        messages,
        system,
        inference,
        tools: encode_tools(request)?,
        additional_fields: encode_additional_fields(request)?,
    })
}

fn encode_content(part: &ContentPart, role: Role) -> Result<ContentBlock, ModelError> {
    match part {
        ContentPart::Text { text } => Ok(ContentBlock::Text(text.clone())),
        ContentPart::ToolCall(call) if role == Role::Assistant => Ok(ContentBlock::ToolUse(
            ToolUseBlock::builder()
                .tool_use_id(&call.id)
                .name(&call.name)
                .input(json_to_document(&call.arguments)?)
                .build()
                .map_err(build_error)?,
        )),
        ContentPart::ToolResult(result) if matches!(role, Role::User | Role::Tool) => {
            let content = result
                .content
                .iter()
                .map(|part| match part {
                    ContentPart::Text { text } => Ok(ToolResultContentBlock::Text(text.clone())),
                    _ => Err(unsupported(
                        "Bedrock tool results currently support text content only",
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let status = if result.is_error {
                ToolResultStatus::Error
            } else {
                ToolResultStatus::Success
            };
            Ok(ContentBlock::ToolResult(
                ToolResultBlock::builder()
                    .tool_use_id(&result.call_id)
                    .set_content(Some(content))
                    .status(status)
                    .build()
                    .map_err(build_error)?,
            ))
        }
        ContentPart::Reasoning(_) => Err(unsupported(
            "Bedrock reasoning input requires an unmodified model-specific round trip",
        )),
        ContentPart::Image { .. } => Err(unsupported(
            "Bedrock image input is model-specific and is not enabled in this layer",
        )),
        ContentPart::Audio { .. } => {
            Err(unsupported("Bedrock Converse does not accept audio here"))
        }
        ContentPart::Document { .. } => Err(unsupported(
            "Bedrock document input requires resolved bytes and a validated document name",
        )),
        ContentPart::Refusal { .. } | ContentPart::Citation(_) => {
            Err(unsupported("output-only content cannot be sent to Bedrock"))
        }
        ContentPart::ProviderOpaque(_) => Err(unsupported(
            "opaque provider content is not accepted without a typed Bedrock contract",
        )),
        _ => Err(unsupported(
            "content variant is newer than this Bedrock adapter",
        )),
    }
}

fn encode_tools(request: &ModelRequest) -> Result<Option<ToolConfiguration>, ModelError> {
    if request.tools.is_empty() {
        if matches!(
            request.tool_choice,
            ToolChoice::Required | ToolChoice::Named { .. }
        ) {
            return Err(invalid("tool choice requires at least one Bedrock tool"));
        }
        return Ok(None);
    }
    if matches!(request.tool_choice, ToolChoice::None) {
        return Ok(None);
    }

    let tools = request
        .tools
        .iter()
        .map(|tool| {
            ToolSpecification::builder()
                .name(&tool.name)
                .description(&tool.description)
                .input_schema(ToolInputSchema::Json(json_to_document(&tool.input_schema)?))
                .build()
                .map(Tool::ToolSpec)
                .map_err(build_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let choice = match &request.tool_choice {
        ToolChoice::Auto => {
            aws_sdk_bedrockruntime::types::ToolChoice::Auto(AutoToolChoice::builder().build())
        }
        ToolChoice::Required => {
            aws_sdk_bedrockruntime::types::ToolChoice::Any(AnyToolChoice::builder().build())
        }
        ToolChoice::Named { name } => aws_sdk_bedrockruntime::types::ToolChoice::Tool(
            SpecificToolChoice::builder()
                .name(name)
                .build()
                .map_err(build_error)?,
        ),
        ToolChoice::None => unreachable!("tool choice none is handled above"),
        _ => {
            return Err(unsupported(
                "tool choice is newer than this Bedrock adapter",
            ));
        }
    };
    Ok(Some(
        ToolConfiguration::builder()
            .set_tools(Some(tools))
            .tool_choice(choice)
            .build()
            .map_err(build_error)?,
    ))
}

fn encode_additional_fields(request: &ModelRequest) -> Result<Option<Document>, ModelError> {
    let Some(value) = request.provider_options.get("bedrock") else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| invalid("`provider_options.bedrock` must be a JSON object"))?;
    let Some(fields) = object.get("additional_model_request_fields") else {
        if object.is_empty() {
            return Ok(None);
        }
        return Err(invalid(
            "unknown Bedrock provider option; only `additional_model_request_fields` is accepted",
        ));
    };
    if object.len() != 1 {
        return Err(invalid(
            "unknown Bedrock provider option beside `additional_model_request_fields`",
        ));
    }
    json_to_document(fields).map(Some)
}

pub(crate) fn json_to_document(value: &Value) -> Result<Document, ModelError> {
    match value {
        Value::Null => Ok(Document::Null),
        Value::Bool(value) => Ok(Document::Bool(*value)),
        Value::String(value) => Ok(Document::String(value.clone())),
        Value::Array(values) => values
            .iter()
            .map(json_to_document)
            .collect::<Result<Vec<_>, _>>()
            .map(Document::Array),
        Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), json_to_document(value)?)))
            .collect::<Result<HashMap<_, _>, ModelError>>()
            .map(Document::Object),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Document::Number(Number::NegInt(value)))
            } else if let Some(value) = value.as_u64() {
                Ok(Document::Number(Number::PosInt(value)))
            } else if let Some(value) = value.as_f64() {
                Ok(Document::Number(Number::Float(value)))
            } else {
                Err(invalid("JSON number cannot be represented by Bedrock"))
            }
        }
    }
}

fn build_error(error: impl std::fmt::Display) -> ModelError {
    invalid(format!("failed to build Bedrock request: {error}"))
}

fn finite_f32(value: f64, name: &str) -> Result<f32, ModelError> {
    if !value.is_finite() {
        return Err(invalid(format!("Bedrock `{name}` must be finite")));
    }
    value
        .to_string()
        .parse::<f32>()
        .map_err(|_| invalid(format!("Bedrock `{name}` is outside the f32 range")))
}

fn invalid(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::InvalidRequest, message)
}

fn unsupported(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::UnsupportedFeature, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runifold_model::{Message, ModelRef, ModelRequest, ToolChoice, ToolSpec};

    use super::encode_request;

    #[test]
    fn encodes_messages_tools_and_additional_fields_natively() {
        let mut request =
            ModelRequest::new(ModelRef::new("bedrock", "model"), Message::user("hello"));
        request.tools.push(ToolSpec {
            name: "lookup".into(),
            description: "Look up a value".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {"id": {"type": "integer"}}
            }),
            output_schema: None,
            metadata: BTreeMap::new(),
        });
        request.tool_choice = ToolChoice::Required;
        request.provider_options.insert(
            "bedrock".into(),
            serde_json::json!({
                "additional_model_request_fields": {"thinking": {"type": "enabled"}}
            }),
        );

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded.messages.len(), 1);
        assert_eq!(
            encoded
                .tools
                .as_ref()
                .expect("tool configuration should exist")
                .tools()
                .len(),
            1
        );
        assert!(encoded.additional_fields.is_some());
    }
}
