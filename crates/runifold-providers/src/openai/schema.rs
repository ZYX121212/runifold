//! `OpenAI` strict JSON Schema normalization and validation.

use runifold_model::{ModelError, ModelErrorKind};
use serde_json::{Map, Value};

/// Produces the provider-wire form of a strict schema without mutating the
/// provider-neutral canonical request.
pub(crate) fn prepare_strict_schema(schema: &Value) -> Result<Value, ModelError> {
    let mut wire_schema = schema.clone();
    normalize_strict_schema(&mut wire_schema)?;
    validate_strict_schema(&wire_schema)?;
    Ok(wire_schema)
}

fn normalize_strict_schema(schema: &mut Value) -> Result<(), ModelError> {
    let Some(root) = schema.as_object_mut() else {
        return Err(invalid("strict schema root must be an object"));
    };
    root.remove("$schema");
    normalize_strict_schema_node(schema)
}

fn normalize_strict_schema_node(schema: &mut Value) -> Result<(), ModelError> {
    let Some(object) = schema.as_object_mut() else {
        return Err(invalid("strict schema nodes must be objects"));
    };

    for annotation in ["default", "deprecated", "examples", "readOnly", "writeOnly"] {
        object.remove(annotation);
    }
    if object
        .get("format")
        .and_then(Value::as_str)
        .is_some_and(|format| {
            matches!(
                format,
                "float"
                    | "double"
                    | "int8"
                    | "int16"
                    | "int32"
                    | "int64"
                    | "int128"
                    | "int"
                    | "uint8"
                    | "uint16"
                    | "uint32"
                    | "uint64"
                    | "uint128"
                    | "uint"
            )
        })
    {
        object.remove("format");
    }

    if schema_object_has_type(object, "object") {
        normalize_object_schema(object)?;
    }

    for keyword in ["$defs", "properties"] {
        if let Some(children) = object.get_mut(keyword).and_then(Value::as_object_mut) {
            for child in children.values_mut() {
                normalize_strict_schema_node(child)?;
            }
        }
    }
    if let Some(items) = object.get_mut("items") {
        normalize_strict_schema_node(items)?;
    }
    if let Some(branches) = object.get_mut("anyOf").and_then(Value::as_array_mut) {
        for branch in branches {
            normalize_strict_schema_node(branch)?;
        }
    }
    Ok(())
}

fn normalize_object_schema(object: &mut Map<String, Value>) -> Result<(), ModelError> {
    match object.get("additionalProperties") {
        None => {
            object.insert("additionalProperties".into(), Value::Bool(false));
        }
        Some(Value::Bool(false)) => {}
        Some(_) => {
            return Err(invalid(
                "strict schema cannot safely close an object with open `additionalProperties`",
            ));
        }
    }

    let property_names = object
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("strict schema objects require `properties`"))?
        .keys()
        .cloned()
        .collect::<Vec<_>>();

    let required = object
        .entry("required")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| invalid("strict schema `required` must be an array"))?;
    for name in property_names {
        if !required.iter().any(|value| value.as_str() == Some(&name)) {
            required.push(Value::String(name));
        }
    }
    Ok(())
}

#[derive(Default)]
struct StrictSchemaStats {
    properties: usize,
    enum_values: usize,
    string_chars: usize,
}

pub(crate) fn validate_strict_schema(schema: &Value) -> Result<(), ModelError> {
    if schema.get("type").and_then(Value::as_str) != Some("object") || schema.get("anyOf").is_some()
    {
        return Err(invalid(
            "strict schema root must be an object and cannot use anyOf",
        ));
    }
    let mut stats = StrictSchemaStats::default();
    validate_strict_schema_node(schema, 1, &mut stats)?;
    if stats.properties > 5_000 {
        return Err(invalid("strict schema exceeds 5000 object properties"));
    }
    if stats.enum_values > 1_000 {
        return Err(invalid("strict schema exceeds 1000 enum values"));
    }
    if stats.string_chars > 120_000 {
        return Err(invalid("strict schema exceeds the 120000 character limit"));
    }
    Ok(())
}

const STRICT_SCHEMA_KEYWORDS: &[&str] = &[
    "$defs",
    "$ref",
    "additionalProperties",
    "anyOf",
    "const",
    "description",
    "enum",
    "exclusiveMaximum",
    "exclusiveMinimum",
    "format",
    "items",
    "maxItems",
    "maximum",
    "minItems",
    "minimum",
    "multipleOf",
    "pattern",
    "properties",
    "required",
    "title",
    "type",
];

fn validate_strict_schema_node(
    schema: &Value,
    object_depth: usize,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    let Some(object) = schema.as_object() else {
        return Err(invalid("strict schema nodes must be objects"));
    };
    validate_schema_header(object)?;
    validate_schema_constraints(object)?;
    validate_schema_subschemas(object, object_depth, stats)?;
    let is_object = schema_has_type(schema, "object");
    let is_array = schema_has_type(schema, "array");
    validate_keyword_placements(schema, object)?;
    if is_object {
        validate_object_schema(object, object_depth, stats)?;
    }
    if is_array {
        validate_array_schema(object, object_depth, stats)?;
    }
    accumulate_schema_values(object, stats)
}

fn validate_schema_header(object: &Map<String, Value>) -> Result<(), ModelError> {
    if let Some(keyword) = object
        .keys()
        .find(|keyword| !STRICT_SCHEMA_KEYWORDS.contains(&keyword.as_str()))
    {
        return Err(invalid(format!(
            "strict schema keyword `{keyword}` is not supported"
        )));
    }
    let Some(types) = object.get("type") else {
        if ["$ref", "anyOf", "enum", "const"]
            .iter()
            .any(|keyword| object.contains_key(*keyword))
        {
            return Ok(());
        }
        return Err(invalid(
            "strict schema node must declare type, $ref, anyOf, enum, or const",
        ));
    };
    let valid = match types {
        Value::String(kind) => strict_schema_type(kind),
        Value::Array(kinds) => {
            !kinds.is_empty()
                && kinds
                    .iter()
                    .all(|kind| kind.as_str().is_some_and(strict_schema_type))
                && kinds
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    == kinds.len()
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid("strict schema contains an unsupported `type`"))
    }
}

fn validate_schema_constraints(object: &Map<String, Value>) -> Result<(), ModelError> {
    for keyword in ["description", "title", "pattern"] {
        if object.get(keyword).is_some_and(|value| !value.is_string()) {
            return Err(invalid(format!(
                "strict schema `{keyword}` must be a string"
            )));
        }
    }
    if let Some(reference) = object.get("$ref") {
        let reference = reference
            .as_str()
            .ok_or_else(|| invalid("strict schema `$ref` must be a string"))?;
        if reference != "#" && !reference.starts_with("#/") {
            return Err(invalid("strict schema `$ref` must be a local reference"));
        }
    }
    validate_schema_format(object)?;
    for keyword in [
        "exclusiveMaximum",
        "exclusiveMinimum",
        "maximum",
        "minimum",
        "multipleOf",
    ] {
        if object.get(keyword).is_some_and(|value| !value.is_number()) {
            return Err(invalid(format!(
                "strict schema `{keyword}` must be a number"
            )));
        }
    }
    for keyword in ["maxItems", "minItems"] {
        if object
            .get(keyword)
            .is_some_and(|value| value.as_u64().is_none())
        {
            return Err(invalid(format!(
                "strict schema `{keyword}` must be a non-negative integer"
            )));
        }
    }
    if object
        .get("multipleOf")
        .and_then(Value::as_f64)
        .is_some_and(|value| value <= 0.0)
    {
        return Err(invalid("strict schema `multipleOf` must be positive"));
    }
    if let (Some(minimum), Some(maximum)) = (
        object.get("minItems").and_then(Value::as_u64),
        object.get("maxItems").and_then(Value::as_u64),
    ) && minimum > maximum
    {
        return Err(invalid("strict schema `minItems` cannot exceed `maxItems`"));
    }
    Ok(())
}

fn validate_schema_format(object: &Map<String, Value>) -> Result<(), ModelError> {
    let Some(format) = object.get("format") else {
        return Ok(());
    };
    let supported = format.as_str().is_some_and(|format| {
        matches!(
            format,
            "date-time"
                | "time"
                | "date"
                | "duration"
                | "email"
                | "hostname"
                | "ipv4"
                | "ipv6"
                | "uuid"
        )
    });
    if supported {
        Ok(())
    } else {
        Err(invalid("strict schema contains an unsupported `format`"))
    }
}

fn validate_schema_subschemas(
    object: &Map<String, Value>,
    object_depth: usize,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    if let Some(definitions) = object.get("$defs") {
        let definitions = definitions
            .as_object()
            .ok_or_else(|| invalid("strict schema `$defs` must be an object"))?;
        for (name, value) in definitions {
            stats.string_chars = stats.string_chars.saturating_add(name.chars().count());
            validate_strict_schema_node(value, object_depth, stats)?;
        }
    }
    if let Some(branches) = object.get("anyOf") {
        let branches = branches
            .as_array()
            .filter(|branches| !branches.is_empty())
            .ok_or_else(|| invalid("strict schema `anyOf` must be a non-empty array"))?;
        for branch in branches {
            validate_strict_schema_node(branch, object_depth, stats)?;
        }
    }
    Ok(())
}

fn validate_keyword_placements(
    schema: &Value,
    object: &Map<String, Value>,
) -> Result<(), ModelError> {
    let placements = [
        (
            schema_has_type(schema, "object"),
            &["additionalProperties", "properties", "required"][..],
            "object",
        ),
        (
            schema_has_type(schema, "array"),
            &["items", "maxItems", "minItems"][..],
            "array",
        ),
        (
            schema_has_type(schema, "string"),
            &["format", "pattern"][..],
            "string",
        ),
        (
            schema_has_type(schema, "number") || schema_has_type(schema, "integer"),
            &[
                "exclusiveMaximum",
                "exclusiveMinimum",
                "maximum",
                "minimum",
                "multipleOf",
            ][..],
            "numeric",
        ),
    ];
    for (valid_type, keywords, kind) in placements {
        if !valid_type && keywords.iter().any(|keyword| object.contains_key(*keyword)) {
            return Err(invalid(format!(
                "strict schema {kind} keywords require a compatible type"
            )));
        }
    }
    Ok(())
}

fn validate_object_schema(
    object: &Map<String, Value>,
    object_depth: usize,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    if object_depth > 10 {
        return Err(invalid("strict schema exceeds 10 object nesting levels"));
    }
    if object.get("additionalProperties") != Some(&Value::Bool(false)) {
        return Err(invalid(
            "strict schema objects require `additionalProperties: false`",
        ));
    }
    let properties = object
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("strict schema objects require `properties`"))?;
    let required = object
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("strict schema objects require `required`"))?;
    let required_count = required.len();
    let required = required
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| invalid("strict schema required names must be strings"))
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if required.len() != required_count
        || required.len() != properties.len()
        || properties
            .keys()
            .any(|name| !required.contains(name.as_str()))
    {
        return Err(invalid("strict schema requires every object property"));
    }
    stats.properties = stats.properties.saturating_add(properties.len());
    for (name, value) in properties {
        stats.string_chars = stats.string_chars.saturating_add(name.chars().count());
        validate_strict_schema_node(value, object_depth + 1, stats)?;
    }
    Ok(())
}

fn validate_array_schema(
    object: &Map<String, Value>,
    object_depth: usize,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    let items = object
        .get("items")
        .ok_or_else(|| invalid("strict schema arrays require `items`"))?;
    validate_strict_schema_node(items, object_depth, stats)
}

fn accumulate_schema_values(
    object: &Map<String, Value>,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    if let Some(values) = object.get("enum") {
        let values = values
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or_else(|| invalid("strict schema `enum` must be a non-empty array"))?;
        stats.enum_values = stats.enum_values.saturating_add(values.len());
        let enum_string_chars = values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| value.chars().count())
            .sum::<usize>();
        if values.len() > 250 && enum_string_chars > 15_000 {
            return Err(invalid(
                "strict schema enum exceeds the 15000 character limit",
            ));
        }
        stats.string_chars = stats.string_chars.saturating_add(enum_string_chars);
    }
    if let Some(value) = object.get("const").and_then(Value::as_str) {
        stats.string_chars = stats.string_chars.saturating_add(value.chars().count());
    }
    Ok(())
}

fn strict_schema_type(kind: &str) -> bool {
    matches!(
        kind,
        "string" | "number" | "integer" | "boolean" | "object" | "array" | "null"
    )
}

fn schema_has_type(schema: &Value, expected: &str) -> bool {
    schema
        .as_object()
        .is_some_and(|object| schema_object_has_type(object, expected))
}

fn schema_object_has_type(object: &Map<String, Value>, expected: &str) -> bool {
    object.get("type").is_some_and(|kind| {
        kind.as_str() == Some(expected)
            || kind
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some(expected)))
    })
}

fn invalid(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::prepare_strict_schema;

    #[test]
    fn normalizes_generated_schema_without_mutating_canonical_value() {
        let canonical = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Answer",
            "type": "object",
            "properties": {
                "value": {"type": "integer", "default": 0},
                "note": {"anyOf": [{"type": "string"}, {"type": "null"}]}
            },
            "required": ["value"]
        });

        let wire = prepare_strict_schema(&canonical).unwrap();

        assert!(canonical.get("$schema").is_some());
        assert!(canonical["properties"]["value"].get("default").is_some());
        assert!(wire.get("$schema").is_none());
        assert!(wire["properties"]["value"].get("default").is_none());
        assert_eq!(wire["additionalProperties"], false);
        assert_eq!(wire["required"], json!(["value", "note"]));
    }

    #[test]
    fn closes_nested_objects_and_definitions() {
        let canonical = json!({
            "type": "object",
            "properties": {
                "nested": {"$ref": "#/$defs/Nested"}
            },
            "$defs": {
                "Nested": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}}
                }
            }
        });

        let wire = prepare_strict_schema(&canonical).unwrap();

        assert_eq!(wire["additionalProperties"], false);
        assert_eq!(wire["required"], json!(["nested"]));
        assert_eq!(wire["$defs"]["Nested"]["additionalProperties"], false);
        assert_eq!(wire["$defs"]["Nested"]["required"], json!(["value"]));
    }

    #[test]
    fn rejects_open_map_objects_instead_of_changing_their_meaning() {
        let schema = json!({
            "type": "object",
            "properties": {},
            "additionalProperties": {"type": "string"}
        });

        let error = prepare_strict_schema(&schema).unwrap_err();

        assert!(error.message.contains("cannot safely close"));
    }
}
