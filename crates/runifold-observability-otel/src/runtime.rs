use std::{fmt, sync::Arc};

use opentelemetry::{global, global::BoxedTracer, metrics::Meter};
use runifold_core::Journal;
use runifold_model::Model;
use runifold_workflow::{
    WorkerId, WorkflowBudgetAuditProjectionId, WorkflowStore, WorkflowTaskCleanupSupervisor,
    WorkflowTaskCleanupSupervisorConfig, WorkflowTaskGovernanceAuthorizer,
    WorkflowTaskGovernanceControlPlane, WorkflowTaskRetentionStore,
    WorkflowTaskTombstoneGovernanceStore, WorkflowTenantId,
};

use crate::workflow_budget::{OtelWorkflowBudgetMetrics, OtelWorkflowBudgetProjector};
use crate::workflow_budget_coordinator::{
    OtelWorkflowBudgetCoordinator, OtelWorkflowBudgetCoordinatorConfig,
};
use crate::workflow_budget_supervisor::{
    OtelWorkflowBudgetSupervisor, OtelWorkflowBudgetSupervisorConfig,
};
use crate::workflow_task_cleanup::OtelWorkflowTaskCleanupMetrics;
use crate::workflow_task_governance::OtelWorkflowTaskGovernanceMetrics;
use crate::{CorrelationRegistry, OtelConfig, OtelJournal, OtelModel};

const SCOPE: &str = "runifold.observability.otel";

/// Shared OpenTelemetry instrumentation boundary for one Runifold runtime.
///
/// Models and journals instrumented by the same value share causal Run
/// correlation without introducing OpenTelemetry types into the runtime
/// kernel.
pub struct OtelRuntime {
    tracer: Arc<BoxedTracer>,
    meter: Meter,
    config: OtelConfig,
    correlation: Arc<CorrelationRegistry>,
}

impl OtelRuntime {
    /// Uses the globally configured OpenTelemetry providers and safe capture
    /// defaults.
    pub fn new() -> Self {
        Self::from_parts(
            global::tracer(SCOPE),
            global::meter(SCOPE),
            OtelConfig::default(),
        )
    }

    /// Uses explicit providers, primarily for isolated applications and tests.
    pub fn from_parts(tracer: BoxedTracer, meter: Meter, config: OtelConfig) -> Self {
        Self {
            tracer: Arc::new(tracer),
            meter,
            config,
            correlation: Arc::new(CorrelationRegistry::default()),
        }
    }

    /// Replaces the safe-default content capture policy.
    #[must_use]
    pub fn with_config(mut self, config: OtelConfig) -> Self {
        self.config = config;
        self
    }

    /// Instruments a canonical Model using this runtime's causal registry.
    pub fn model<M>(&self, model: M) -> OtelModel
    where
        M: Model + 'static,
    {
        self.model_from_arc(Arc::new(model))
    }

    /// Instruments an existing object-safe Model.
    pub fn model_from_arc(&self, model: Arc<dyn Model>) -> OtelModel {
        OtelModel::from_shared_parts(
            model,
            Arc::clone(&self.tracer),
            &self.meter,
            self.config.clone(),
            Arc::clone(&self.correlation),
        )
    }

    /// Instruments a durable Journal using this runtime's causal registry.
    pub fn journal<J>(&self, journal: J) -> OtelJournal<J>
    where
        J: Journal,
    {
        OtelJournal::from_shared_parts(
            journal,
            Arc::clone(&self.tracer),
            &self.meter,
            Arc::clone(&self.correlation),
        )
    }

    /// Creates a projection for durable workflow tenant-budget audit events.
    pub fn workflow_budget_metrics(&self) -> OtelWorkflowBudgetMetrics {
        OtelWorkflowBudgetMetrics::new(&self.meter)
    }

    /// Creates low-cardinality terminal Task cleanup instruments.
    pub fn workflow_task_cleanup_metrics(&self) -> OtelWorkflowTaskCleanupMetrics {
        OtelWorkflowTaskCleanupMetrics::new(&self.meter)
    }

    /// Creates low-cardinality tombstone governance instruments.
    pub fn workflow_task_governance_metrics(&self) -> OtelWorkflowTaskGovernanceMetrics {
        OtelWorkflowTaskGovernanceMetrics::new(&self.meter)
    }

    /// Creates an authorized tombstone control plane with `OTel` outcomes.
    pub fn workflow_task_governance_control_plane<S, A>(
        &self,
        store: Arc<S>,
        authorizer: Arc<A>,
    ) -> WorkflowTaskGovernanceControlPlane<S, A>
    where
        S: WorkflowTaskTombstoneGovernanceStore,
        A: WorkflowTaskGovernanceAuthorizer,
    {
        WorkflowTaskGovernanceControlPlane::new(store, authorizer)
            .with_observer(Arc::new(self.workflow_task_governance_metrics()))
    }

    /// Creates a dynamically sharded cleanup supervisor with `OTel` observation.
    pub fn workflow_task_cleanup_supervisor<S>(
        &self,
        store: Arc<S>,
        owner: WorkerId,
        config: WorkflowTaskCleanupSupervisorConfig,
    ) -> WorkflowTaskCleanupSupervisor<S>
    where
        S: WorkflowTaskRetentionStore + 'static,
    {
        WorkflowTaskCleanupSupervisor::new(store, owner, config)
            .with_observer(Arc::new(self.workflow_task_cleanup_metrics()))
    }

    /// Creates a restart-safe projector for one tenant and named consumer.
    pub fn workflow_budget_projector<S>(
        &self,
        store: Arc<S>,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
    ) -> OtelWorkflowBudgetProjector<S>
    where
        S: WorkflowStore,
    {
        OtelWorkflowBudgetProjector::new(
            store,
            tenant_id,
            projection_id,
            self.workflow_budget_metrics(),
        )
    }

    /// Creates a continuously supervised, exclusively leased projector.
    pub fn workflow_budget_projection_supervisor<S>(
        &self,
        store: Arc<S>,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        config: OtelWorkflowBudgetSupervisorConfig,
    ) -> OtelWorkflowBudgetSupervisor<S>
    where
        S: WorkflowStore + 'static,
    {
        OtelWorkflowBudgetSupervisor::new(
            store,
            tenant_id,
            projection_id,
            owner,
            self.workflow_budget_metrics(),
            config,
        )
    }

    /// Creates a bounded coordinator for every budget-enabled tenant assigned
    /// to one deterministic shard.
    pub fn workflow_budget_projection_coordinator<S>(
        &self,
        store: Arc<S>,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        config: OtelWorkflowBudgetCoordinatorConfig,
    ) -> OtelWorkflowBudgetCoordinator<S>
    where
        S: WorkflowStore + 'static,
    {
        OtelWorkflowBudgetCoordinator::new(
            store,
            projection_id,
            owner,
            self.workflow_budget_metrics(),
            config,
        )
    }
}

impl Default for OtelRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for OtelRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtelRuntime")
            .field("tracer", &self.tracer)
            .field("meter", &self.meter)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, future::pending, num::NonZeroU32, sync::Arc, time::Duration};

    use opentelemetry::{Value, global, metrics::MeterProvider as _, trace::SpanId};
    use opentelemetry_sdk::{
        metrics::{
            InMemoryMetricExporter, SdkMeterProvider,
            data::{AggregatedMetrics, MetricData, ResourceMetrics},
        },
        trace::SpanData,
    };
    use runifold_core::{
        Budget, BudgetTracker, CancellationToken, CapabilitySet, CheckpointId, DomainEvent,
        InMemoryJournal, LifecycleEvent, RetrySafety, RunContext, RunError, RunErrorKind,
        RunEventKind, Usage,
    };
    use runifold_model::{
        ContentPart, FinishReason, Message, Model, ModelCallContext, ModelRef, ModelRequest,
        ModelStreamEvent,
    };
    use runifold_testkit::ScriptedModel;
    use runifold_workflow::{
        InMemoryWorkflowStore, LeaseDuration, WorkerId, WorkflowBudgetAuditCursor,
        WorkflowBudgetAuditEvent, WorkflowBudgetAuditKind, WorkflowBudgetAuditLimit,
        WorkflowBudgetAuditProjectionId, WorkflowBudgetForfeitReason, WorkflowStore,
        WorkflowTenantBudgetPolicy, WorkflowTenantId, WorkflowWorkerSleepFuture,
        WorkflowWorkerSleeper,
    };

    use super::{OtelRuntime, OtelWorkflowBudgetSupervisorConfig};
    use crate::{
        OtelConfig,
        slo::{AGENT_OPERATION_DURATION_SECONDS, metric_names},
        test_support::TraceFixture,
    };

    #[tokio::test]
    async fn shared_runtime_parents_model_calls_and_child_runs() {
        let fixture = TraceFixture::new();
        let runtime = OtelRuntime::from_parts(
            fixture.tracer,
            global::meter("runifold.test"),
            OtelConfig::default(),
        );
        let journal = Arc::new(runtime.journal(InMemoryJournal::new()));
        let parent = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
            .with_journal(journal);
        start_agent(&parent, "planner");

        let scripted = ScriptedModel::new();
        scripted.enqueue([
            ModelStreamEvent::ResponseStarted {
                id: Some("response-1".into()),
                model: ModelRef::new("openai", "gpt-test"),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::text("done"),
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: BTreeMap::new(),
            },
        ]);
        let model = runtime.model(scripted);
        let model_context = ModelCallContext::for_run(&parent);
        let invocation_id = model_context.invocation_id().to_string();
        model
            .invoke(
                ModelRequest::new(
                    ModelRef::new("openai", "gpt-test"),
                    Message::user("private input"),
                ),
                model_context,
            )
            .await
            .unwrap();

        let child = parent.child(CapabilitySet::new()).unwrap();
        start_agent(&child, "researcher");
        finish(&child);
        finish(&parent);

        let spans = fixture.exporter.get_finished_spans().unwrap();
        let parent_span = named(&spans, "invoke_agent planner");
        let parent_turn = named_for_run(&spans, "agent.turn 1", parent.run_id());
        let model_span = named(&spans, "chat gpt-test");
        let child_span = named(&spans, "invoke_agent researcher");
        assert_eq!(parent_span.parent_span_id, SpanId::INVALID);
        assert_eq!(
            model_span.parent_span_id,
            parent_turn.span_context.span_id()
        );
        assert_eq!(
            child_span.parent_span_id,
            parent_span.span_context.span_id()
        );
        assert_eq!(
            attribute(model_span, "runifold.run.id"),
            Some(parent.run_id().to_string())
        );
        assert_eq!(
            attribute(model_span, "runifold.model.invocation.id"),
            Some(invocation_id)
        );
        assert!(!format!("{spans:?}").contains("private input"));
    }

    #[test]
    fn journal_exports_low_cardinality_agent_and_sampling_metrics() {
        let fixture = TraceFixture::new();
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let runtime = OtelRuntime::from_parts(
            fixture.tracer,
            meter_provider.meter("runifold.test"),
            OtelConfig::default(),
        );
        let journal = Arc::new(runtime.journal(InMemoryJournal::new()));

        let completed =
            RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
                .with_journal(Arc::clone(&journal) as Arc<dyn runifold_core::Journal>);
        start_agent(&completed, "planner");
        completed
            .record(
                RunEventKind::Domain(DomainEvent {
                    namespace: "runifold.mcp".into(),
                    name: "sampling.started".into(),
                    payload: serde_json::json!({
                        "call_id": "sampling-private-id",
                        "message_count": 1,
                        "max_tokens": 128
                    }),
                }),
                completed.caused_by(),
            )
            .unwrap();
        completed
            .record(
                RunEventKind::Domain(DomainEvent {
                    namespace: "runifold.mcp".into(),
                    name: "sampling.failed".into(),
                    payload: serde_json::json!({
                        "call_id": "sampling-private-id",
                        "error_type": "remote",
                        "stage": "response_review"
                    }),
                }),
                completed.caused_by(),
            )
            .unwrap();
        completed
            .record(
                RunEventKind::Lifecycle(LifecycleEvent::Completed {
                    output: serde_json::json!({
                        "agent": "planner",
                        "turns": 2,
                        "tool_calls": 3,
                        "delegations": 1,
                        "usage": {"cost_microusd": 250_000}
                    }),
                }),
                completed.caused_by(),
            )
            .unwrap();

        let exhausted =
            RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
                .with_journal(journal);
        start_agent(&exhausted, "budgeted");
        exhausted
            .record(
                RunEventKind::Lifecycle(LifecycleEvent::Failed {
                    error: RunError {
                        kind: RunErrorKind::BudgetExceeded,
                        message: "private budget details".into(),
                        retry_safety: RetrySafety::Unknown,
                        metadata: BTreeMap::new(),
                    },
                }),
                exhausted.caused_by(),
            )
            .unwrap();

        meter_provider.force_flush().unwrap();
        let metrics = exporter.get_finished_metrics().unwrap();
        let names = metrics
            .iter()
            .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .map(opentelemetry_sdk::metrics::data::Metric::name)
            .collect::<Vec<_>>();
        assert_operational_metric_names(&names);
        assert_histogram_bounds(
            &metrics,
            metric_names::AGENT_OPERATION_DURATION,
            AGENT_OPERATION_DURATION_SECONDS,
        );
        let metric_dump = format!("{metrics:?}");
        assert!(!metric_dump.contains(&completed.run_id().to_string()));
        assert!(!metric_dump.contains(&exhausted.run_id().to_string()));
        assert!(!metric_dump.contains("sampling-private-id"));
        assert!(!metric_dump.contains("private budget details"));
    }

    #[test]
    fn workflow_budget_metrics_project_durable_events_without_tenant_identity() {
        let fixture = TraceFixture::new();
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let runtime = OtelRuntime::from_parts(
            fixture.tracer,
            meter_provider.meter("runifold.test"),
            OtelConfig::default(),
        );
        let instruments = runtime.workflow_budget_metrics();
        let checkpoint_id = CheckpointId::new();
        let tenant_id = WorkflowTenantId::parse("private-tenant-42").unwrap();
        instruments.observe(&WorkflowBudgetAuditEvent {
            cursor: WorkflowBudgetAuditCursor::new(7),
            tenant_id: tenant_id.clone(),
            checkpoint_id: Some(checkpoint_id),
            occurred_at_ms: 10,
            kind: WorkflowBudgetAuditKind::Forfeited(WorkflowBudgetForfeitReason::RecoveryExpired),
            usage: Usage {
                tokens: 25,
                ..Usage::default()
            },
            reservation_age_ms: Some(2_500),
            limit: Budget {
                tokens: Some(100),
                ..Budget::default()
            },
            committed: Usage {
                tokens: 75,
                ..Usage::default()
            },
            reserved: Usage::default(),
        });

        meter_provider.force_flush().unwrap();
        let metrics = exporter.get_finished_metrics().unwrap();
        let names = metrics
            .iter()
            .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .map(opentelemetry_sdk::metrics::data::Metric::name)
            .collect::<Vec<_>>();
        for expected in [
            metric_names::WORKFLOW_BUDGET_DECISIONS,
            metric_names::WORKFLOW_BUDGET_AMOUNT,
            metric_names::WORKFLOW_BUDGET_UTILIZATION,
            metric_names::WORKFLOW_BUDGET_RESERVATION_AGE,
        ] {
            assert!(names.contains(&expected), "missing metric {expected}");
        }
        let dump = format!("{metrics:?}");
        assert!(!dump.contains(tenant_id.as_str()));
        assert!(!dump.contains(&checkpoint_id.to_string()));
        assert!(dump.contains("recovery_expired"));
        assert!(dump.contains("tokens"));
    }

    #[tokio::test]
    async fn workflow_budget_projector_resumes_from_durable_cursor() {
        let fixture = TraceFixture::new();
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter)
            .build();
        let runtime = OtelRuntime::from_parts(
            fixture.tracer,
            meter_provider.meter("runifold.test"),
            OtelConfig::default(),
        );
        let store = Arc::new(InMemoryWorkflowStore::new());
        let tenant_id = WorkflowTenantId::parse("private-projector-tenant").unwrap();
        let policy = WorkflowTenantBudgetPolicy::new(
            Budget {
                tokens: Some(100),
                ..Budget::default()
            },
            Duration::from_secs(60),
            Duration::from_secs(1),
        )
        .unwrap();
        store
            .set_tenant_budget_policy(tenant_id.clone(), policy)
            .await
            .unwrap();
        store
            .set_tenant_budget_policy(tenant_id.clone(), policy)
            .await
            .unwrap();
        let projection_id = WorkflowBudgetAuditProjectionId::parse("otel-primary").unwrap();
        let projector = runtime
            .workflow_budget_projector(store.clone(), tenant_id.clone(), projection_id.clone())
            .with_page_limit(WorkflowBudgetAuditLimit::new(1).unwrap());
        let report = projector
            .project_available(NonZeroU32::new(4).unwrap())
            .await
            .unwrap();
        assert_eq!(report.events_projected, 2);
        assert_eq!(report.batches_projected, 2);
        assert!(report.caught_up);

        let restarted = runtime.workflow_budget_projector(
            store.clone(),
            tenant_id.clone(),
            projection_id.clone(),
        );
        let resumed = restarted.project_once().await.unwrap();
        assert_eq!(resumed.events_projected, 0);
        assert!(resumed.caught_up);
        assert_eq!(
            store
                .load_or_create_tenant_budget_audit_projection(tenant_id, projection_id)
                .await
                .unwrap(),
            report.cursor.unwrap()
        );
    }

    #[derive(Debug)]
    struct CancelOnIdle {
        idle_interval: Duration,
        shutdown: CancellationToken,
    }

    impl WorkflowWorkerSleeper for CancelOnIdle {
        fn sleep(&self, duration: Duration) -> WorkflowWorkerSleepFuture<'_> {
            if duration == self.idle_interval {
                let shutdown = self.shutdown.clone();
                return Box::pin(async move {
                    shutdown.cancel();
                });
            }
            Box::pin(pending())
        }
    }

    #[tokio::test]
    async fn workflow_budget_supervisor_projects_under_exclusive_lease_and_releases() {
        let fixture = TraceFixture::new();
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let runtime = OtelRuntime::from_parts(
            fixture.tracer,
            meter_provider.meter("runifold.test"),
            OtelConfig::default(),
        );
        let store = Arc::new(InMemoryWorkflowStore::new());
        let tenant_id = WorkflowTenantId::parse("supervised-tenant").unwrap();
        store
            .set_tenant_budget_policy(
                tenant_id.clone(),
                WorkflowTenantBudgetPolicy::new(
                    Budget {
                        tokens: Some(100),
                        ..Budget::default()
                    },
                    Duration::from_secs(60),
                    Duration::from_secs(1),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let projection_id = WorkflowBudgetAuditProjectionId::parse("otel-primary").unwrap();
        let idle_interval = Duration::from_millis(1);
        let config = OtelWorkflowBudgetSupervisorConfig::new(
            LeaseDuration::new(Duration::from_millis(100)).unwrap(),
            Duration::from_millis(50),
        )
        .unwrap()
        .with_idle_interval(idle_interval)
        .unwrap();
        let shutdown = CancellationToken::new();
        let supervisor = runtime
            .workflow_budget_projection_supervisor(
                store.clone(),
                tenant_id.clone(),
                projection_id.clone(),
                WorkerId::parse("projector-a").unwrap(),
                config,
            )
            .with_sleeper(Arc::new(CancelOnIdle {
                idle_interval,
                shutdown: shutdown.clone(),
            }));
        let report = supervisor.run(&shutdown).await;
        assert_eq!(report.claims, 1);
        assert_eq!(report.events_projected, 1);
        assert_eq!(report.batches_projected, 1);
        assert_eq!(report.leases_lost, 0);
        let health = supervisor.metrics().snapshot();
        assert!(!health.lease_active);
        assert!(health.caught_up);
        assert_eq!(health.claims, 1);
        assert_eq!(health.events_projected, 1);
        assert_eq!(health.batches_projected, 1);
        assert_eq!(health.leases_lost, 0);
        assert!(health.last_cursor.is_some());
        meter_provider.force_flush().unwrap();
        let metric_dump = format!("{:?}", exporter.get_finished_metrics().unwrap());
        assert!(metric_dump.contains(metric_names::WORKFLOW_BUDGET_PROJECTION_OPERATIONS));
        assert!(metric_dump.contains("claimed"));
        assert!(metric_dump.contains("completed"));
        assert!(
            store
                .claim_tenant_budget_audit_projection(
                    tenant_id,
                    projection_id,
                    WorkerId::parse("projector-b").unwrap(),
                    LeaseDuration::new(Duration::from_millis(100)).unwrap(),
                )
                .await
                .unwrap()
                .is_some()
        );
    }

    fn assert_operational_metric_names(names: &[&str]) {
        for expected in [
            "runifold.agent.operation.duration",
            "runifold.agent.turn.duration",
            "runifold.agent.turns",
            "runifold.agent.tool.calls",
            "runifold.agent.delegations",
            "runifold.agent.cost",
            "runifold.agent.errors",
            "runifold.agent.budget.exhaustions",
            "runifold.mcp.sampling.duration",
            "runifold.mcp.sampling.requests",
            "runifold.mcp.sampling.failures",
        ] {
            assert!(names.contains(&expected), "missing metric {expected}");
        }
    }

    fn assert_histogram_bounds(metrics: &[ResourceMetrics], metric_name: &str, expected: &[f64]) {
        let metric = metrics
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .find(|metric| metric.name() == metric_name)
            .unwrap();
        let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
            panic!("{metric_name} is not an f64 histogram");
        };
        let bounds = histogram
            .data_points()
            .next()
            .unwrap()
            .bounds()
            .collect::<Vec<_>>();
        assert_eq!(bounds, expected);
    }

    fn start_agent(run: &RunContext, agent: &str) {
        run.record(
            RunEventKind::Lifecycle(LifecycleEvent::Started),
            run.caused_by(),
        )
        .unwrap();
        run.record(
            RunEventKind::Domain(DomainEvent {
                namespace: "runifold.agent".into(),
                name: "turn.started".into(),
                payload: serde_json::json!({"agent": agent, "turn": 1}),
            }),
            run.caused_by(),
        )
        .unwrap();
    }

    fn finish(run: &RunContext) {
        run.record(
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: serde_json::json!({"ok": true}),
            }),
            run.caused_by(),
        )
        .unwrap();
    }

    fn named<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
        spans.iter().find(|span| span.name == name).unwrap()
    }

    fn named_for_run<'a>(
        spans: &'a [SpanData],
        name: &str,
        run_id: runifold_core::RunId,
    ) -> &'a SpanData {
        spans
            .iter()
            .find(|span| {
                span.name == name && attribute(span, "runifold.run.id") == Some(run_id.to_string())
            })
            .unwrap()
    }

    fn attribute(span: &SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .map(|attribute| match &attribute.value {
                Value::String(value) => value.to_string(),
                value => value.to_string(),
            })
    }
}
