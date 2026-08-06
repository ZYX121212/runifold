//! Gemini request encoding.

use serde_json::{Map, Value, json};

use runifold_model::{
    ContentPart, MediaSource, ModelError, ModelErrorKind, ModelRequest, OutputFormat, Role,
    ToolChoice, ToolResult,
};

/// Encodes a canonical request as Gemini `GenerateContentRequest`.
///
/// # Errors
///
/// Returns an error when content cannot be represented without loss.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ModelError> {
    if request.messages.is_empty() {
        return Err(invalid("a model request must contain messages"));
    }
    if request.generation.seed.is_some() {
        return Err(unsupported("Gemini adapter does not support seed"));
    }
    let mut system_parts = Vec::new();
    let mut contents = Vec::new();
    for message in &request.messages {
        let parts = message
            .content
            .iter()
            .map(|part| encode_part(part, message.role))
            .collect::<Result<Vec<_>, _>>()?;
        if message.role == Role::System {
            system_parts.extend(parts);
        } else {
            contents.push(json!({"role": role(message.role)?, "parts": parts}));
        }
    }
    if contents.is_empty() {
        return Err(invalid("Gemini requires non-system content"));
    }

    let mut body = Map::new();
    body.insert("contents".into(), Value::Array(contents));
    if !system_parts.is_empty() {
        body.insert(
            "systemInstruction".into(),
            json!({"role":"user","parts":system_parts}),
        );
    }
    let mut generation = Map::new();
    optional(
        &mut generation,
        "temperature",
        request.generation.temperature,
    );
    optional(&mut generation, "topP", request.generation.top_p);
    if let Some(limit) = request.generation.max_output_tokens {
        generation.insert("maxOutputTokens".into(), Value::from(limit));
    }
    if !request.generation.stop.is_empty() {
        generation.insert(
            "stopSequences".into(),
            Value::Array(
                request
                    .generation
                    .stop
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    match &request.output_format {
        OutputFormat::Text => {}
        OutputFormat::Json => {
            generation.insert(
                "responseMimeType".into(),
                Value::String("application/json".into()),
            );
        }
        OutputFormat::JsonSchema { schema, .. } => {
            generation.insert(
                "responseMimeType".into(),
                Value::String("application/json".into()),
            );
            generation.insert("responseJsonSchema".into(), schema.clone());
        }
        _ => {
            return Err(unsupported(
                "output format is newer than this Gemini adapter",
            ));
        }
    }
    if !generation.is_empty() {
        body.insert("generationConfig".into(), Value::Object(generation));
    }
    if !request.tools.is_empty() && !matches!(request.tool_choice, ToolChoice::None) {
        body.insert(
            "tools".into(),
            json!([{"functionDeclarations": request.tools.iter().map(|tool| json!({
                "name":tool.name,
                "description":tool.description,
                "parametersJsonSchema":tool.input_schema
            })).collect::<Vec<_>>()}]),
        );
        body.insert(
            "toolConfig".into(),
            json!({"functionCallingConfig": tool_choice(&request.tool_choice)?}),
        );
    }
    merge_options(&mut body, request)?;
    Ok(Value::Object(body))
}

fn encode_part(part: &ContentPart, role: Role) -> Result<Value, ModelError> {
    match part {
        ContentPart::Text { text } => Ok(json!({"text":text})),
        ContentPart::Image { source }
        | ContentPart::Audio { source }
        | ContentPart::Document { source, .. } => encode_media(source),
        ContentPart::ToolCall(call) if role == Role::Assistant => Ok(json!({
            "functionCall":{"id":call.id,"name":call.name,"args":call.arguments}
        })),
        ContentPart::ToolResult(result) if matches!(role, Role::Tool | Role::User) => {
            encode_tool_result(result)
        }
        ContentPart::Reasoning(reasoning) if role == Role::Assistant => {
            let text = reasoning
                .text
                .as_ref()
                .ok_or_else(|| unsupported("Gemini thought round trip requires text"))?;
            let mut part = json!({"text":text,"thought":true});
            if let Some(signature) = &reasoning.signature {
                part["thoughtSignature"] = Value::String(signature.clone());
            }
            Ok(part)
        }
        ContentPart::ProviderOpaque(data) if data.provider == "gemini" && data.kind == "part" => {
            Ok(data.value.clone())
        }
        _ => Err(unsupported(
            "content cannot be represented by the Gemini adapter",
        )),
    }
}

fn encode_media(source: &MediaSource) -> Result<Value, ModelError> {
    match source {
        MediaSource::Base64 { media_type, data } => {
            Ok(json!({"inlineData":{"mimeType":media_type,"data":data}}))
        }
        MediaSource::Url { url, media_type } => Ok(json!({"fileData":{
            "mimeType":media_type,
            "fileUri":url
        }})),
        MediaSource::Artifact { .. } => Err(unsupported(
            "artifact media must be resolved before Gemini invocation",
        )),
        _ => Err(unsupported(
            "media source is newer than this Gemini adapter",
        )),
    }
}

fn encode_tool_result(result: &ToolResult) -> Result<Value, ModelError> {
    let name = result
        .name
        .as_deref()
        .ok_or_else(|| invalid("Gemini function responses require ToolResult.name"))?;
    let mut text = Vec::new();
    let mut media = Vec::new();
    for part in &result.content {
        match part {
            ContentPart::Text { text: value } => text.push(Value::String(value.clone())),
            ContentPart::Image { source }
            | ContentPart::Audio { source }
            | ContentPart::Document { source, .. } => media.push(encode_media(source)?),
            ContentPart::ResourceLink {
                uri, media_type, ..
            } => media.push(encode_media(&MediaSource::Url {
                url: uri.clone(),
                media_type: media_type.clone(),
            })?),
            _ => {
                return Err(unsupported(
                    "Gemini function results support text, images, audio, documents, and resource links",
                ));
            }
        }
    }
    let response = result.structured_content.clone().unwrap_or_else(|| {
        json!({
            "output": text,
            "isError": result.is_error
        })
    });
    let mut function_response = serde_json::Map::from_iter([
        ("id".into(), Value::String(result.call_id.clone())),
        ("name".into(), Value::String(name.into())),
        ("response".into(), response),
    ]);
    if !media.is_empty() {
        function_response.insert("parts".into(), Value::Array(media));
    }
    Ok(json!({"functionResponse":function_response}))
}

fn role(role: Role) -> Result<&'static str, ModelError> {
    match role {
        Role::User | Role::Tool => Ok("user"),
        Role::Assistant => Ok("model"),
        Role::System => Err(invalid("system content uses systemInstruction")),
        _ => Err(unsupported("role is newer than this Gemini adapter")),
    }
}

fn tool_choice(choice: &ToolChoice) -> Result<Value, ModelError> {
    match choice {
        ToolChoice::Auto => Ok(json!({"mode":"AUTO"})),
        ToolChoice::Required => Ok(json!({"mode":"ANY"})),
        ToolChoice::Named { name } => Ok(json!({"mode":"ANY","allowedFunctionNames":[name]})),
        ToolChoice::None => Ok(json!({"mode":"NONE"})),
        _ => Err(unsupported("tool choice is newer than this Gemini adapter")),
    }
}

fn optional(body: &mut Map<String, Value>, key: &str, value: Option<f64>) {
    if let Some(value) = value {
        body.insert(key.into(), Value::from(value));
    }
}

fn merge_options(body: &mut Map<String, Value>, request: &ModelRequest) -> Result<(), ModelError> {
    let Some(options) = request.provider_options.get("gemini") else {
        return Ok(());
    };
    let options = options
        .as_object()
        .ok_or_else(|| invalid("provider_options.gemini must be an object"))?;
    for (key, value) in options {
        if body.contains_key(key)
            || matches!(
                key.as_str(),
                "contents" | "systemInstruction" | "tools" | "toolConfig" | "generationConfig"
            )
        {
            return Err(invalid(format!("provider option `{key}` is adapter-owned")));
        }
        body.insert(key.clone(), value.clone());
    }
    Ok(())
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

    use runifold_model::{
        ContentPart, MediaSource, Message, ModelRef, ModelRequest, Role, ToolResult,
    };

    use super::encode_request;

    #[test]
    fn encodes_native_contents_and_system_instruction() {
        let request = ModelRequest::new(
            ModelRef::new("gemini", "gemini-test"),
            Message::system("be exact"),
        )
        .message(Message::user("hello"));

        let body = encode_request(&request).unwrap();

        assert_eq!(body["systemInstruction"]["parts"][0]["text"], "be exact");
        assert_eq!(body["contents"][0]["role"], "user");
    }

    #[test]
    fn encodes_multimodal_function_response_parts() {
        let message = Message::new(
            Role::Tool,
            vec![ContentPart::ToolResult(ToolResult {
                call_id: "call-rich".into(),
                name: Some("inspect".into()),
                content: vec![ContentPart::Image {
                    source: MediaSource::Base64 {
                        media_type: "image/png".into(),
                        data: "cG5n".into(),
                    },
                }],
                structured_content: Some(serde_json::json!({"objects":1})),
                is_error: false,
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();
        let body = encode_request(&ModelRequest::new(
            ModelRef::new("gemini", "gemini-3"),
            message,
        ))
        .unwrap();

        let response = &body["contents"][0]["parts"][0]["functionResponse"];
        assert_eq!(response["response"], serde_json::json!({"objects":1}));
        assert_eq!(response["parts"][0]["inlineData"]["mimeType"], "image/png");
    }
}
