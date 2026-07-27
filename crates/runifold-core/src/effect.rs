use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{CapabilityId, EffectClass, EffectId, InvocationId};

/// The operation requested by an external effect.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum EffectKind {
    /// Invoke a model.
    Model,
    /// Invoke a tool.
    Tool,
    /// Invoke another agent.
    Agent,
    /// Ask an external authority for approval.
    Approval,
    /// Wait for a timer or external wakeup.
    Wait,
    /// Invoke a namespaced extension.
    Extension(String),
}

/// A runtime request to perform external work.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EffectRequest {
    /// Unique effect identity.
    pub effect_id: EffectId,
    /// Stable invocation identity used for correlation and idempotency.
    pub invocation_id: InvocationId,
    /// Operation category.
    pub kind: EffectKind,
    /// Capability required to perform the effect.
    pub capability_id: CapabilityId,
    /// Input after local validation.
    pub input: Value,
    /// Side-effect classification at invocation time.
    pub effect_class: EffectClass,
    /// Optional stable key for an idempotent external system.
    pub idempotency_key: Option<String>,
}
