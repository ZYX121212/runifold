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
    /// Creates an agent contract with a fresh ephemeral capability identity.
    ///
    /// Rebuilding this descriptor produces a different identity. Applications
    /// that persist grants, policies, or audit records must restore the same
    /// [`CapabilityId`] and call [`Self::with_id`].
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

    /// Replaces the capability identity with an application-owned stable ID.
    ///
    /// The ID should be loaded from durable configuration or storage and reused
    /// across process restarts whenever grants or audit records outlive one
    /// process.
    #[must_use]
    pub const fn with_id(mut self, id: CapabilityId) -> Self {
        self.id = id;
        self
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

#[cfg(test)]
mod tests {
    use runifold_core::CapabilityId;

    use super::AgentDescriptor;

    #[test]
    fn configured_identity_survives_descriptor_reconstruction() {
        let id: CapabilityId = "018f6f7e-6f1d-7f2a-9c40-7f4f8f0a3d21"
            .parse()
            .expect("configured UUID is valid");

        let first = AgentDescriptor::new("researcher", "delegate research").with_id(id);
        let second = AgentDescriptor::new("researcher", "delegate research").with_id(id);

        assert_eq!(first.id, second.id);
        assert_eq!(first.capability().id, second.capability().id);
    }

    #[test]
    fn default_construction_remains_ephemeral() {
        let first = AgentDescriptor::new("researcher", "delegate research");
        let second = AgentDescriptor::new("researcher", "delegate research");

        assert_ne!(first.id, second.id);
    }
}
