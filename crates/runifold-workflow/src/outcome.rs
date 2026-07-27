use std::collections::BTreeMap;

use runifold_core::Usage;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::StepId;

/// Successful terminal workflow state.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowOutcome {
    /// Output of the final executed node.
    pub output: Value,
    /// Stable output captured after every completed node.
    pub steps: BTreeMap<StepId, Value>,
    /// Shared run-tree usage snapshot at completion.
    pub usage: Usage,
}
