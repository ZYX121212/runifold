use std::collections::HashSet;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Map, Value};

const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeaderPrimitive {
    Boolean,
    Integer,
    String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ToolHeaderRule {
    suffix: String,
    path: Vec<String>,
    primitive: HeaderPrimitive,
}

impl ToolHeaderRule {
    pub(crate) fn header_name(&self) -> String {
        format!("mcp-param-{}", self.suffix)
    }

    fn value<'a>(&self, arguments: &'a Map<String, Value>) -> Option<&'a Value> {
        let (first, rest) = self.path.split_first()?;
        let mut value = arguments.get(first)?;
        for segment in rest {
            value = value.as_object()?.get(segment)?;
        }
        Some(value)
    }

    pub(crate) fn encoded_value(
        &self,
        arguments: &Map<String, Value>,
    ) -> Result<Option<String>, ToolHeaderError> {
        self.plain_value(arguments)
            .map(|value| value.map(|value| encode_header_value(&value)))
    }

    pub(crate) fn matches(
        &self,
        arguments: &Map<String, Value>,
        encoded_header: Option<&str>,
    ) -> Result<bool, ToolHeaderError> {
        let expected = self.plain_value(arguments)?;
        let Some(encoded_header) = encoded_header else {
            return Ok(expected.is_none());
        };
        let Some(actual) = decode_header_value(encoded_header) else {
            return Ok(false);
        };
        match (self.primitive, expected, actual) {
            (_, None, _) => Ok(false),
            (HeaderPrimitive::Integer, Some(expected), actual) => {
                Ok(parse_safe_integer(&expected) == parse_safe_integer(&actual))
            }
            (_, Some(expected), actual) => Ok(expected == actual),
        }
    }

    fn plain_value(
        &self,
        arguments: &Map<String, Value>,
    ) -> Result<Option<String>, ToolHeaderError> {
        let Some(value) = self.value(arguments) else {
            return Ok(None);
        };
        if value.is_null() {
            return Ok(None);
        }
        match self.primitive {
            HeaderPrimitive::Boolean => value
                .as_bool()
                .map(|value| Some(value.to_string()))
                .ok_or_else(|| ToolHeaderError::ValueType {
                    path: self.path.join("."),
                    expected: "boolean",
                }),
            HeaderPrimitive::Integer => {
                safe_integer(value)
                    .map(Some)
                    .ok_or_else(|| ToolHeaderError::ValueType {
                        path: self.path.join("."),
                        expected: "safe integer",
                    })
            }
            HeaderPrimitive::String => value
                .as_str()
                .map(|value| Some(value.to_owned()))
                .ok_or_else(|| ToolHeaderError::ValueType {
                    path: self.path.join("."),
                    expected: "string",
                }),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ToolHeaderError {
    #[error("x-mcp-header must be attached to a statically reachable property")]
    Unreachable,
    #[error("x-mcp-header must be a non-empty HTTP token")]
    InvalidName,
    #[error("x-mcp-header names must be case-insensitively unique")]
    DuplicateName,
    #[error("x-mcp-header property must have type string, integer, or boolean")]
    InvalidPrimitive,
    #[error("Tool argument `{path}` must be a {expected}")]
    ValueType {
        path: String,
        expected: &'static str,
    },
}

pub(crate) fn compile_tool_header_rules(
    schema: &Value,
) -> Result<Vec<ToolHeaderRule>, ToolHeaderError> {
    if !schema.is_object() {
        reject_unreachable_headers(schema)?;
    }
    let mut rules = Vec::new();
    let mut names = HashSet::new();
    visit_schema(schema, &mut Vec::new(), true, &mut names, &mut rules)?;
    Ok(rules)
}

fn visit_schema(
    schema: &Value,
    path: &mut Vec<String>,
    reachable: bool,
    names: &mut HashSet<String>,
    rules: &mut Vec<ToolHeaderRule>,
) -> Result<(), ToolHeaderError> {
    let Some(object) = schema.as_object() else {
        return Ok(());
    };
    if let Some(header) = object.get("x-mcp-header") {
        if !reachable || path.is_empty() {
            return Err(ToolHeaderError::Unreachable);
        }
        let suffix = header
            .as_str()
            .filter(|suffix| is_http_token(suffix))
            .ok_or(ToolHeaderError::InvalidName)?;
        if !names.insert(suffix.to_ascii_lowercase()) {
            return Err(ToolHeaderError::DuplicateName);
        }
        let primitive = match object.get("type").and_then(Value::as_str) {
            Some("boolean") => HeaderPrimitive::Boolean,
            Some("integer") => HeaderPrimitive::Integer,
            Some("string") => HeaderPrimitive::String,
            _ => return Err(ToolHeaderError::InvalidPrimitive),
        };
        rules.push(ToolHeaderRule {
            suffix: suffix.to_owned(),
            path: path.clone(),
            primitive,
        });
    }

    for (keyword, value) in object {
        if keyword == "x-mcp-header" {
            continue;
        }
        if keyword == "properties" && reachable {
            if let Some(properties) = value.as_object() {
                for (name, property_schema) in properties {
                    path.push(name.clone());
                    visit_schema(property_schema, path, true, names, rules)?;
                    path.pop();
                }
            } else {
                reject_unreachable_headers(value)?;
            }
            continue;
        }
        reject_unreachable_headers(value)?;
    }
    Ok(())
}

fn reject_unreachable_headers(value: &Value) -> Result<(), ToolHeaderError> {
    match value {
        Value::Array(values) => {
            for value in values {
                reject_unreachable_headers(value)?;
            }
        }
        Value::Object(values) => {
            if values.contains_key("x-mcp-header") {
                return Err(ToolHeaderError::Unreachable);
            }
            for value in values.values() {
                reject_unreachable_headers(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn encode_header_value(value: &str) -> String {
    let is_plain_ascii = !value.is_empty()
        && value.bytes().all(|byte| matches!(byte, 0x20..=0x7e))
        && value.trim() == value
        && !(value.starts_with("=?base64?") && value.ends_with("?="));
    if is_plain_ascii {
        value.to_owned()
    } else {
        format!("=?base64?{}?=", STANDARD.encode(value))
    }
}

pub(crate) fn decode_header_value(value: &str) -> Option<String> {
    let Some(encoded) = value
        .strip_prefix("=?base64?")
        .and_then(|value| value.strip_suffix("?="))
    else {
        return Some(value.to_owned());
    };
    STANDARD
        .decode(encoded)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
}

fn is_http_token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn safe_integer(value: &Value) -> Option<String> {
    if let Some(value) = value.as_i64() {
        return (value.unsigned_abs() <= MAX_SAFE_INTEGER).then(|| value.to_string());
    }
    value
        .as_u64()
        .filter(|value| *value <= MAX_SAFE_INTEGER)
        .map(|value| value.to_string())
}

fn parse_safe_integer(value: &str) -> Option<i128> {
    value
        .parse::<i128>()
        .ok()
        .filter(|value| value.unsigned_abs() <= u128::from(MAX_SAFE_INTEGER))
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, json};

    use super::{
        ToolHeaderError, compile_tool_header_rules, decode_header_value, encode_header_value,
    };

    #[test]
    fn compiles_nested_reachable_properties_and_encodes_values() {
        let schema = json!({
            "type": "object",
            "properties": {
                "routing": {
                    "type": "object",
                    "properties": {
                        "region": {"type": "string", "x-mcp-header": "Region"},
                        "shard": {"type": "integer", "x-mcp-header": "Shard"},
                        "active": {"type": "boolean", "x-mcp-header": "Active"}
                    }
                }
            }
        });
        let rules = compile_tool_header_rules(&schema).unwrap();
        let arguments = Map::from_iter([(
            "routing".to_owned(),
            json!({"region": "华东", "shard": 7, "active": true}),
        )]);
        let encoded = rules
            .iter()
            .map(|rule| {
                (
                    rule.header_name(),
                    rule.encoded_value(&arguments).unwrap().unwrap(),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(
            encoded
                .get("mcp-param-Region")
                .and_then(|value| decode_header_value(value)),
            Some("华东".to_owned())
        );
        assert_eq!(encoded["mcp-param-Shard"], "7");
        assert_eq!(encoded["mcp-param-Active"], "true");
    }

    #[test]
    fn rejects_unreachable_duplicate_and_non_primitive_declarations() {
        let unreachable = json!({
            "type": "object",
            "properties": {
                "values": {
                    "type": "array",
                    "items": {"type": "string", "x-mcp-header": "Item"}
                }
            }
        });
        assert!(matches!(
            compile_tool_header_rules(&unreachable),
            Err(ToolHeaderError::Unreachable)
        ));

        let malformed_properties = json!({
            "type": "object",
            "properties": [
                {"type": "string", "x-mcp-header": "Escaped"}
            ]
        });
        assert!(matches!(
            compile_tool_header_rules(&malformed_properties),
            Err(ToolHeaderError::Unreachable)
        ));

        let duplicate = json!({
            "type": "object",
            "properties": {
                "first": {"type": "string", "x-mcp-header": "Route"},
                "second": {"type": "string", "x-mcp-header": "route"}
            }
        });
        assert!(matches!(
            compile_tool_header_rules(&duplicate),
            Err(ToolHeaderError::DuplicateName)
        ));

        let number = json!({
            "type": "object",
            "properties": {
                "score": {"type": "number", "x-mcp-header": "Score"}
            }
        });
        assert!(matches!(
            compile_tool_header_rules(&number),
            Err(ToolHeaderError::InvalidPrimitive)
        ));
    }

    #[test]
    fn safely_encodes_sentinel_unicode_and_whitespace() {
        for value in ["=?base64?literal?=", "你好", " padded "] {
            let encoded = encode_header_value(value);
            assert_ne!(encoded, value);
            assert_eq!(decode_header_value(&encoded).as_deref(), Some(value));
        }
        assert_eq!(encode_header_value("plain-value"), "plain-value");
    }

    #[test]
    fn rejects_integer_values_outside_the_javascript_safe_range() {
        let schema = json!({
            "type": "object",
            "properties": {
                "shard": {"type": "integer", "x-mcp-header": "Shard"}
            }
        });
        let rule = compile_tool_header_rules(&schema).unwrap().remove(0);
        let arguments = Map::from_iter([("shard".to_owned(), json!(9_007_199_254_740_992_u64))]);

        assert!(matches!(
            rule.encoded_value(&arguments),
            Err(ToolHeaderError::ValueType { .. })
        ));
    }
}
