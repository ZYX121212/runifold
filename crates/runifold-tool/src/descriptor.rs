use std::collections::BTreeMap;

use runifold_core::{CapabilityDescriptor, CapabilityId, CapabilityKind, EffectClass, RiskLevel};
use runifold_model::ToolSpec;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Versioned model-facing and policy-facing description of a tool.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolDescriptor {
    /// Stable capability identity.
    pub id: CapabilityId,
    /// Model-facing tool name.
    pub name: String,
    /// Semantic contract version.
    pub version: String,
    /// Model-facing description.
    pub description: String,
    /// JSON Schema for invocation input.
    pub input_schema: Value,
    /// JSON Schema for successful output.
    pub output_schema: Value,
    /// External side-effect classification.
    pub effect: EffectClass,
    /// Risk classification used by policy and approval middleware.
    pub risk: RiskLevel,
    /// Host-only namespaced metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl ToolDescriptor {
    /// Converts this descriptor into a grantable runtime capability.
    pub fn capability(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            id: self.id,
            name: self.name.clone(),
            version: self.version.clone(),
            kind: CapabilityKind::Tool,
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
            effect: self.effect,
            risk: self.risk,
            metadata: self.metadata.clone(),
        }
    }

    /// Converts this descriptor into the subset exposed to a model.
    pub fn model_spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: Some(self.output_schema.clone()),
            metadata: self.metadata.clone(),
        }
    }
}
