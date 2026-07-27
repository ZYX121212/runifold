use std::collections::BTreeMap;

use runifold_core::{CapabilityDescriptor, CapabilityId, CapabilityKind, EffectClass, RiskLevel};
use runifold_model::ToolSpec;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Versioned contract for invoking an agent through a gateway.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AgentDescriptor {
    /// Stable capability identity.
    pub id: CapabilityId,
    /// Model-facing delegation name.
    pub name: String,
    /// Semantic contract version.
    pub version: String,
    /// Model-facing description of when this agent should be used.
    pub description: String,
    /// Coarse risk classification for policy engines.
    pub risk: RiskLevel,
    /// Host-only namespaced metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl AgentDescriptor {
    /// Creates an agent contract with the canonical delegation schema.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            id: CapabilityId::new(),
            name: name.into(),
            version: "1".into(),
            description: description.into(),
            risk: RiskLevel::Medium,
            metadata: BTreeMap::new(),
        }
    }

    /// Converts this descriptor into a grantable agent capability.
    pub fn capability(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            id: self.id,
            name: self.name.clone(),
            version: self.version.clone(),
            kind: CapabilityKind::Agent,
            input_schema: input_schema(),
            output_schema: output_schema(),
            effect: EffectClass::Unknown,
            risk: self.risk,
            metadata: self.metadata.clone(),
        }
    }

    /// Converts this descriptor into the callable shape exposed to a model.
    pub fn model_spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: input_schema(),
            output_schema: Some(output_schema()),
            metadata: self.metadata.clone(),
        }
    }
}

fn input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "input": {
                "type": "string",
                "description": "Task or question delegated to the agent"
            }
        },
        "required": ["input"],
        "additionalProperties": false
    })
}

fn output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "agent": {"type": "string"},
            "content": {"type": "array"},
            "turns": {"type": "integer", "minimum": 0},
            "tool_calls": {"type": "integer", "minimum": 0},
            "delegations": {"type": "integer", "minimum": 0}
        },
        "required": ["agent", "content", "turns", "tool_calls", "delegations"],
        "additionalProperties": false
    })
}
