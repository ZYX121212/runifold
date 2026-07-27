use std::{fmt, sync::Arc};

use runifold_agent::Agent;
use runifold_core::{CapabilitySet, EffectClass, Usage};

use crate::{
    AgentStep, StepId, WorkflowBuildError, WorkflowCondition, WorkflowStep, WorkflowStepError,
};

pub(crate) enum WorkflowNodeKind {
    Step(Arc<dyn WorkflowStep>),
    Branch {
        condition: Arc<dyn WorkflowCondition>,
        when_true: Arc<dyn WorkflowStep>,
        when_false: Arc<dyn WorkflowStep>,
    },
    Parallel(Arc<[ParallelBranch]>),
    Race(Arc<[ParallelBranch]>),
}

pub(crate) struct WorkflowNode {
    pub(crate) id: StepId,
    pub(crate) capabilities: CapabilitySet,
    pub(crate) kind: WorkflowNodeKind,
}

impl WorkflowNode {
    pub(crate) async fn execute(
        &self,
        input: serde_json::Value,
        run: &runifold_core::RunContext,
    ) -> Result<(serde_json::Value, Option<bool>), WorkflowStepError> {
        match &self.kind {
            WorkflowNodeKind::Step(step) => {
                step.execute(input, run).await.map(|output| (output, None))
            }
            WorkflowNodeKind::Branch {
                condition,
                when_true,
                when_false,
            } => {
                let selected = condition.evaluate(&input)?;
                let step = if selected { when_true } else { when_false };
                step.execute(input, run)
                    .await
                    .map(|output| (output, Some(selected)))
            }
            WorkflowNodeKind::Parallel(_) | WorkflowNodeKind::Race(_) => {
                unreachable!("concurrent nodes use their dedicated scheduler")
            }
        }
    }
}

/// One explicitly budgeted branch of a parallel workflow node.
pub struct ParallelBranch {
    pub(crate) id: String,
    pub(crate) step: Arc<dyn WorkflowStep>,
    pub(crate) capabilities: CapabilitySet,
    pub(crate) reservation: Usage,
}

impl ParallelBranch {
    /// Creates a custom parallel branch with an explicit resource reservation.
    pub fn step<S>(
        id: impl Into<String>,
        step: S,
        capabilities: CapabilitySet,
        reservation: Usage,
    ) -> Self
    where
        S: WorkflowStep + 'static,
    {
        Self {
            id: id.into(),
            step: Arc::new(step),
            capabilities,
            reservation,
        }
    }

    /// Creates an Agent-backed parallel branch.
    pub fn agent(
        id: impl Into<String>,
        agent: Arc<Agent>,
        capabilities: CapabilitySet,
        reservation: Usage,
    ) -> Self {
        Self::step(id, AgentStep::new(agent), capabilities, reservation)
    }
}

impl fmt::Debug for ParallelBranch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParallelBranch")
            .field("id", &self.id)
            .field("capabilities", &self.capabilities)
            .field("reservation", &self.reservation)
            .finish_non_exhaustive()
    }
}

/// Validated, immutable workflow definition.
#[derive(Clone)]
pub struct Workflow {
    pub(crate) name: String,
    pub(crate) version: u32,
    pub(crate) nodes: Arc<[WorkflowNode]>,
}

impl Workflow {
    /// Starts a fluent workflow definition.
    pub fn builder(name: impl Into<String>) -> WorkflowBuilder {
        WorkflowBuilder::new(name)
    }

    /// Returns the stable workflow name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the definition version used to validate checkpoints.
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Returns stable node identifiers in execution order.
    pub fn step_ids(&self) -> impl ExactSizeIterator<Item = &StepId> {
        self.nodes.iter().map(|node| &node.id)
    }
}

impl fmt::Debug for Workflow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Workflow")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("steps", &self.step_ids().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// Fluent, validation-preserving workflow assembly.
pub struct WorkflowBuilder {
    name: String,
    version: u32,
    nodes: Vec<WorkflowNode>,
    error: Option<WorkflowBuildError>,
}

impl WorkflowBuilder {
    /// Creates an empty version-one workflow definition.
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let error = name
            .trim()
            .is_empty()
            .then_some(WorkflowBuildError::EmptyName);
        Self {
            name,
            version: 1,
            nodes: Vec::new(),
            error,
        }
    }

    /// Sets the durable workflow definition version.
    #[must_use]
    pub fn version(mut self, version: u32) -> Self {
        if version == 0 && self.error.is_none() {
            self.error = Some(WorkflowBuildError::InvalidVersion);
        } else {
            self.version = version;
        }
        self
    }

    /// Appends one custom executable step.
    #[must_use]
    pub fn step<S>(mut self, id: impl Into<String>, step: S, capabilities: CapabilitySet) -> Self
    where
        S: WorkflowStep + 'static,
    {
        self.push_node(id, capabilities, WorkflowNodeKind::Step(Arc::new(step)));
        self
    }

    /// Appends one Agent-backed step.
    #[must_use]
    pub fn agent(
        mut self,
        id: impl Into<String>,
        agent: Arc<Agent>,
        capabilities: CapabilitySet,
    ) -> Self {
        self.push_node(
            id,
            capabilities,
            WorkflowNodeKind::Step(Arc::new(AgentStep::new(agent))),
        );
        self
    }

    /// Appends a condition that executes exactly one of two steps.
    #[must_use]
    pub fn branch<C, T, F>(
        mut self,
        id: impl Into<String>,
        condition: C,
        when_true: T,
        when_false: F,
        capabilities: CapabilitySet,
    ) -> Self
    where
        C: WorkflowCondition + 'static,
        T: WorkflowStep + 'static,
        F: WorkflowStep + 'static,
    {
        self.push_node(
            id,
            capabilities,
            WorkflowNodeKind::Branch {
                condition: Arc::new(condition),
                when_true: Arc::new(when_true),
                when_false: Arc::new(when_false),
            },
        );
        self
    }

    /// Appends a deterministic fan-out/fan-in parallel node.
    ///
    /// Every branch receives the same canonical input. Outputs are joined into
    /// an object keyed by branch identifier, independent of completion order.
    #[must_use]
    pub fn parallel(
        mut self,
        id: impl Into<String>,
        branches: impl IntoIterator<Item = ParallelBranch>,
    ) -> Self {
        if let Some((id, branches)) = self.validate_concurrent_branches(id, branches) {
            if branches.len() < 2 {
                self.error = Some(WorkflowBuildError::TooFewParallelBranches(id));
                return self;
            }
            self.nodes.push(WorkflowNode {
                id,
                capabilities: CapabilitySet::new(),
                kind: WorkflowNodeKind::Parallel(branches.into()),
            });
        }
        self
    }

    /// Appends a budget-bounded, first-success race.
    ///
    /// Race branches may request only `Pure` or `ReadOnly` capabilities.
    /// Losing reservations are conservatively forfeited because remote work
    /// may outlive local cancellation.
    #[must_use]
    pub fn race(
        mut self,
        id: impl Into<String>,
        branches: impl IntoIterator<Item = ParallelBranch>,
    ) -> Self {
        if let Some((id, branches)) = self.validate_concurrent_branches(id, branches) {
            if branches.len() < 2 {
                self.error = Some(WorkflowBuildError::TooFewRaceBranches(id));
                return self;
            }
            for branch in &branches {
                if let Some(capability) = branch.capabilities.iter().find(|capability| {
                    !matches!(capability.effect, EffectClass::Pure | EffectClass::ReadOnly)
                }) {
                    let branch = match StepId::parse(branch.id.clone()) {
                        Ok(branch) => branch,
                        Err(branch) => {
                            self.error = Some(WorkflowBuildError::InvalidParallelBranchId(branch));
                            return self;
                        }
                    };
                    self.error = Some(WorkflowBuildError::UnsafeRaceCapability {
                        step: id,
                        branch,
                        capability: capability.name.clone(),
                    });
                    return self;
                }
            }
            self.nodes.push(WorkflowNode {
                id,
                capabilities: CapabilitySet::new(),
                kind: WorkflowNodeKind::Race(branches.into()),
            });
        }
        self
    }

    /// Validates and freezes this workflow definition.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowBuildError`] for invalid identity, version, duplicate
    /// steps, or an empty definition.
    pub fn build(self) -> Result<Workflow, WorkflowBuildError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.nodes.is_empty() {
            return Err(WorkflowBuildError::NoSteps);
        }
        Ok(Workflow {
            name: self.name,
            version: self.version,
            nodes: self.nodes.into(),
        })
    }

    fn push_node(
        &mut self,
        id: impl Into<String>,
        capabilities: CapabilitySet,
        kind: WorkflowNodeKind,
    ) {
        if self.error.is_some() {
            return;
        }
        let id = match StepId::parse(id) {
            Ok(id) => id,
            Err(id) => {
                self.error = Some(WorkflowBuildError::InvalidStepId(id));
                return;
            }
        };
        if self.nodes.iter().any(|node| node.id == id) {
            self.error = Some(WorkflowBuildError::DuplicateStep(id));
            return;
        }
        self.nodes.push(WorkflowNode {
            id,
            capabilities,
            kind,
        });
    }

    fn validate_concurrent_branches(
        &mut self,
        id: impl Into<String>,
        branches: impl IntoIterator<Item = ParallelBranch>,
    ) -> Option<(StepId, Vec<ParallelBranch>)> {
        if self.error.is_some() {
            return None;
        }
        let id = match StepId::parse(id) {
            Ok(id) => id,
            Err(id) => {
                self.error = Some(WorkflowBuildError::InvalidStepId(id));
                return None;
            }
        };
        if self.nodes.iter().any(|node| node.id == id) {
            self.error = Some(WorkflowBuildError::DuplicateStep(id));
            return None;
        }
        let mut validated = Vec::new();
        for branch in branches {
            let branch_id = match StepId::parse(branch.id) {
                Ok(branch_id) => branch_id,
                Err(branch_id) => {
                    self.error = Some(WorkflowBuildError::InvalidParallelBranchId(branch_id));
                    return None;
                }
            };
            if validated
                .iter()
                .any(|existing: &ParallelBranch| existing.id == branch_id.as_str())
            {
                self.error = Some(WorkflowBuildError::DuplicateParallelBranch {
                    step: id,
                    branch: branch_id,
                });
                return None;
            }
            validated.push(ParallelBranch {
                id: branch_id.to_string(),
                ..branch
            });
        }
        Some((id, validated))
    }
}

impl fmt::Debug for WorkflowBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowBuilder")
            .field("name", &self.name)
            .field("version", &self.version)
            .field(
                "steps",
                &self.nodes.iter().map(|node| &node.id).collect::<Vec<_>>(),
            )
            .field("error", &self.error)
            .finish()
    }
}
