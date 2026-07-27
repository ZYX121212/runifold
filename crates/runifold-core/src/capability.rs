use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::CapabilityId;

/// A capability category.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum CapabilityKind {
    /// A language or multimodal model.
    Model,
    /// A callable tool.
    Tool,
    /// Another agent.
    Agent,
    /// A readable or writable resource.
    Resource,
    /// A renderable prompt contract.
    Prompt,
    /// A namespaced extension capability.
    Extension(String),
}

/// The external-effect behavior of a capability.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum EffectClass {
    /// No externally visible effect.
    Pure,
    /// Reads external state without modifying it.
    ReadOnly,
    /// Writes external state and is safe to repeat with the same key.
    IdempotentWrite,
    /// Writes external state and may not be safe to repeat.
    NonIdempotentWrite,
    /// May destroy or irreversibly mutate state.
    Destructive,
    /// Effect behavior is unknown.
    Unknown,
}

/// A coarse capability risk classification.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[non_exhaustive]
pub enum RiskLevel {
    /// No known meaningful external risk.
    Low,
    /// Requires normal policy evaluation.
    Medium,
    /// Requires elevated scrutiny or approval.
    High,
    /// Should be denied unless explicitly approved.
    Critical,
}

/// A versioned description of a grantable capability.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CapabilityDescriptor {
    /// Stable identity of this capability instance.
    pub id: CapabilityId,
    /// Human-readable name exposed to operators and possibly models.
    pub name: String,
    /// Semantic contract version.
    pub version: String,
    /// Capability category.
    pub kind: CapabilityKind,
    /// JSON Schema for invocation input.
    pub input_schema: Value,
    /// JSON Schema for invocation output.
    pub output_schema: Value,
    /// External-effect classification.
    pub effect: EffectClass,
    /// Risk classification.
    pub risk: RiskLevel,
    /// Namespaced extension metadata.
    pub metadata: BTreeMap<String, Value>,
}

/// An explicit set of capabilities granted to a run.
#[derive(Clone, Debug, Default)]
pub struct CapabilitySet {
    entries: BTreeMap<CapabilityId, CapabilityDescriptor>,
}

impl CapabilitySet {
    /// Creates an empty capability set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Grants a capability, replacing a descriptor with the same identity.
    pub fn grant(&mut self, capability: CapabilityDescriptor) {
        self.entries.insert(capability.id, capability);
    }

    /// Revokes a capability by identity.
    pub fn revoke(&mut self, id: CapabilityId) -> Option<CapabilityDescriptor> {
        self.entries.remove(&id)
    }

    /// Returns a granted capability.
    pub fn get(&self, id: CapabilityId) -> Option<&CapabilityDescriptor> {
        self.entries.get(&id)
    }

    /// Returns whether the set contains a capability.
    pub fn contains(&self, id: CapabilityId) -> bool {
        self.entries.contains_key(&id)
    }

    /// Returns whether every capability in this set is also granted by
    /// `authority`.
    pub fn is_subset_of(&self, authority: &Self) -> bool {
        self.entries.keys().all(|id| authority.contains(*id))
    }

    /// Returns the first capability not granted by `authority`.
    pub fn first_missing_from(&self, authority: &Self) -> Option<&CapabilityDescriptor> {
        self.entries
            .values()
            .find(|capability| !authority.contains(capability.id))
    }

    /// Iterates over granted capabilities.
    pub fn iter(&self) -> impl Iterator<Item = &CapabilityDescriptor> {
        self.entries.values()
    }

    /// Returns the number of granted capabilities.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no capabilities are granted.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet, EffectClass, RiskLevel,
    };

    fn capability(name: &str) -> CapabilityDescriptor {
        CapabilityDescriptor {
            id: CapabilityId::new(),
            name: name.into(),
            version: "1".into(),
            kind: CapabilityKind::Tool,
            input_schema: json!({}),
            output_schema: json!({}),
            effect: EffectClass::Pure,
            risk: RiskLevel::Low,
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn subset_checks_use_stable_capability_identity() {
        let granted = capability("granted");
        let missing = capability("missing");
        let mut authority = CapabilitySet::new();
        authority.grant(granted.clone());
        let mut requested = CapabilitySet::new();
        requested.grant(granted);

        assert!(requested.is_subset_of(&authority));

        requested.grant(missing.clone());

        assert!(!requested.is_subset_of(&authority));
        assert_eq!(
            requested.first_missing_from(&authority).map(|item| item.id),
            Some(missing.id)
        );
    }
}
