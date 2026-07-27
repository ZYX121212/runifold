use std::{sync::Arc, time::Instant};

use futures_util::future::{Either, select};
use runifold_core::{
    EffectClass, EffectEvent, EffectId, EffectRequest, RunContext, RunError, RunEventKind,
};
use serde_json::Value;

use crate::{
    EffectExecutionContext, EffectExecutorError, EffectExecutorErrorKind, EffectHandler,
    EffectRecord, EffectStatus, EffectStore,
};

/// Recovery behavior for a record whose handler may have executed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum EffectRecoveryPolicy {
    /// Never retry an ambiguous started effect automatically.
    #[default]
    RejectAmbiguous,
    /// Retry only effects whose class and idempotency contract make it safe.
    RetrySafe,
}

/// Controls whether effect outputs are copied into Journal events.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum EffectEventPayloadPolicy {
    /// Keep durable output only in the `EffectStore`.
    #[default]
    Redacted,
    /// Copy the complete output into `EffectEvent::Completed`.
    Full,
}

/// Successful coordinated effect result.
#[derive(Clone, Debug, PartialEq)]
pub struct EffectOutcome {
    /// Canonical logical effect identity.
    pub effect_id: EffectId,
    /// Canonical output.
    pub output: Value,
    /// Whether the output came from a completed durable record.
    pub replayed: bool,
    /// Durable record revision containing the output.
    pub revision: u64,
}

/// Capability-gated write-ahead external-effect coordinator.
#[derive(Clone)]
pub struct EffectExecutor {
    store: Arc<dyn EffectStore>,
    event_payload_policy: EffectEventPayloadPolicy,
}

impl EffectExecutor {
    /// Creates an executor using the given durable-state boundary.
    pub fn new(store: Arc<dyn EffectStore>) -> Self {
        Self {
            store,
            event_payload_policy: EffectEventPayloadPolicy::Redacted,
        }
    }

    /// Sets explicit Journal output-capture behavior.
    #[must_use]
    pub const fn with_event_payload_policy(mut self, policy: EffectEventPayloadPolicy) -> Self {
        self.event_payload_policy = policy;
        self
    }

    /// Executes or recovers one logical effect.
    ///
    /// # Errors
    ///
    /// Returns [`EffectExecutorError`] for authority, persistence, lifecycle,
    /// ambiguity, handler, or observability failures.
    pub async fn execute(
        &self,
        request: EffectRequest,
        run: &RunContext,
        handler: &dyn EffectHandler,
        recovery: EffectRecoveryPolicy,
    ) -> Result<EffectOutcome, EffectExecutorError> {
        preflight(&request, run)?;
        let (record, created) = self.resolve(request)?;
        if created {
            record_event(
                run,
                RunEventKind::Effect(EffectEvent::Requested {
                    effect_id: record.request.effect_id,
                }),
            )?;
        }
        match &record.status {
            EffectStatus::Completed { output } => Ok(EffectOutcome {
                effect_id: record.request.effect_id,
                output: output.clone(),
                replayed: true,
                revision: record.revision,
            }),
            EffectStatus::Failed { error } => Err(EffectExecutorError::handler(error.clone())),
            EffectStatus::Prepared => self.start(record, run, handler).await,
            EffectStatus::Started => {
                if recovery != EffectRecoveryPolicy::RetrySafe || !retry_safe(&record.request) {
                    return Err(EffectExecutorError::new(
                        EffectExecutorErrorKind::Ambiguous,
                        format!(
                            "effect `{}` may already have executed",
                            record.request.effect_id
                        ),
                    ));
                }
                self.start(record, run, handler).await
            }
        }
    }

    fn resolve(&self, request: EffectRequest) -> Result<(EffectRecord, bool), EffectExecutorError> {
        if let Some(record) = self.store.load(request.effect_id)? {
            validate_same_effect(&record.request, &request)?;
            return Ok((record, false));
        }
        if let Some(key) = &request.idempotency_key {
            if let Some(record) = self.store.find_by_idempotency(request.capability_id, key)? {
                validate_same_effect(&record.request, &request)?;
                return Ok((record, false));
            }
        }
        let record = EffectRecord::prepared(request);
        self.store.compare_and_swap(&record, None)?;
        Ok((record, true))
    }

    async fn start(
        &self,
        record: EffectRecord,
        run: &RunContext,
        handler: &dyn EffectHandler,
    ) -> Result<EffectOutcome, EffectExecutorError> {
        let started = record.next(EffectStatus::Started)?;
        self.store
            .compare_and_swap(&started, Some(record.revision))?;
        record_event(
            run,
            RunEventKind::Effect(EffectEvent::Started {
                effect_id: started.request.effect_id,
            }),
        )?;

        let context = EffectExecutionContext::for_run(run);
        let cancellation = context.cancellation().clone();
        let execution = handler.execute(&started.request, context);
        let result = match select(Box::pin(cancellation.cancelled()), Box::pin(execution)).await {
            Either::Left(_) => {
                return Err(EffectExecutorError::new(
                    EffectExecutorErrorKind::Cancelled,
                    "effect execution was cancelled and remains ambiguous",
                ));
            }
            Either::Right((result, _)) => result,
        };

        match result {
            Ok(output) => self.complete(&started, output, run),
            Err(error) => self.fail(&started, error, run),
        }
    }

    fn complete(
        &self,
        started: &EffectRecord,
        output: Value,
        run: &RunContext,
    ) -> Result<EffectOutcome, EffectExecutorError> {
        let completed = started.next(EffectStatus::Completed {
            output: output.clone(),
        })?;
        self.store
            .compare_and_swap(&completed, Some(started.revision))?;
        record_event(
            run,
            RunEventKind::Effect(EffectEvent::Completed {
                effect_id: completed.request.effect_id,
                output: match self.event_payload_policy {
                    EffectEventPayloadPolicy::Redacted => {
                        serde_json::json!({"runifold": {"content_recorded": false}})
                    }
                    EffectEventPayloadPolicy::Full => output.clone(),
                },
            }),
        )?;
        Ok(EffectOutcome {
            effect_id: completed.request.effect_id,
            output,
            replayed: false,
            revision: completed.revision,
        })
    }

    fn fail(
        &self,
        started: &EffectRecord,
        error: RunError,
        run: &RunContext,
    ) -> Result<EffectOutcome, EffectExecutorError> {
        let failed = started.next(EffectStatus::Failed {
            error: error.clone(),
        })?;
        self.store
            .compare_and_swap(&failed, Some(started.revision))?;
        record_event(
            run,
            RunEventKind::Effect(EffectEvent::Failed {
                effect_id: failed.request.effect_id,
                error: error.clone(),
            }),
        )?;
        Err(EffectExecutorError::handler(error))
    }
}

impl std::fmt::Debug for EffectExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("EffectExecutor(..)")
    }
}

fn preflight(request: &EffectRequest, run: &RunContext) -> Result<(), EffectExecutorError> {
    if !run.capabilities().contains(request.capability_id) {
        return Err(EffectExecutorError::new(
            EffectExecutorErrorKind::CapabilityDenied,
            "Run is not granted the effect capability",
        ));
    }
    if run.cancellation().is_cancelled() {
        return Err(EffectExecutorError::new(
            EffectExecutorErrorKind::Cancelled,
            "effect was cancelled before preparation",
        ));
    }
    if run
        .deadline()
        .is_some_and(|deadline| deadline <= Instant::now())
    {
        return Err(EffectExecutorError::new(
            EffectExecutorErrorKind::DeadlineExceeded,
            "effect deadline elapsed before preparation",
        ));
    }
    Ok(())
}

fn validate_same_effect(
    existing: &EffectRequest,
    requested: &EffectRequest,
) -> Result<(), EffectExecutorError> {
    let same = existing.kind == requested.kind
        && existing.capability_id == requested.capability_id
        && existing.input == requested.input
        && existing.effect_class == requested.effect_class
        && existing.idempotency_key == requested.idempotency_key;
    if !same {
        return Err(EffectExecutorError::new(
            EffectExecutorErrorKind::IdempotencyConflict,
            "effect identity or idempotency key was reused for different work",
        ));
    }
    Ok(())
}

fn retry_safe(request: &EffectRequest) -> bool {
    matches!(
        request.effect_class,
        EffectClass::Pure | EffectClass::ReadOnly
    ) || matches!(request.effect_class, EffectClass::IdempotentWrite)
        && request.idempotency_key.is_some()
}

fn record_event(run: &RunContext, kind: RunEventKind) -> Result<(), EffectExecutorError> {
    run.record(kind, None)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use runifold_core::{
        Budget, BudgetTracker, CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet,
        EffectClass, EffectEvent, EffectId, EffectKind, EffectRequest, InMemoryJournal,
        InvocationId, RiskLevel, RunContext, RunError, RunEventKind,
    };
    use serde_json::{Value, json};

    use crate::{
        EffectEventPayloadPolicy, EffectExecutionContext, EffectExecutor, EffectExecutorErrorKind,
        EffectFuture, EffectHandler, EffectRecord, EffectRecoveryPolicy, EffectStatus, EffectStore,
        InMemoryEffectStore,
    };

    struct CountingHandler {
        calls: AtomicUsize,
    }

    impl CountingHandler {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl EffectHandler for CountingHandler {
        fn execute(
            &self,
            request: &EffectRequest,
            _context: EffectExecutionContext,
        ) -> EffectFuture<'_, Result<Value, RunError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let input = request.input.clone();
            Box::pin(async move { Ok(json!({"echo": input})) })
        }
    }

    #[test]
    fn completed_idempotent_effect_is_replayed_without_handler_execution() {
        let capability = capability(EffectClass::IdempotentWrite);
        let journal = InMemoryJournal::new();
        let run = run_with(&capability, Some(journal.clone()));
        let store = Arc::new(InMemoryEffectStore::new());
        let executor = EffectExecutor::new(store);
        let handler = CountingHandler::new();
        let first_request = request(&capability, Some("order-7"), json!({"value": 7}));

        let first = futures_executor::block_on(executor.execute(
            first_request.clone(),
            &run,
            &handler,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .unwrap();
        let mut duplicate = first_request;
        duplicate.effect_id = EffectId::new();
        duplicate.invocation_id = InvocationId::new();
        let second = futures_executor::block_on(executor.execute(
            duplicate,
            &run,
            &handler,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .unwrap();

        assert!(!first.replayed);
        assert!(second.replayed);
        assert_eq!(second.effect_id, first.effect_id);
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
        let events = journal.events();
        assert!(matches!(
            events[0].kind,
            RunEventKind::Effect(EffectEvent::Requested { .. })
        ));
        assert!(matches!(
            events.last().unwrap().kind,
            RunEventKind::Effect(EffectEvent::Completed { .. })
        ));
        assert!(matches!(
            &events.last().unwrap().kind,
            RunEventKind::Effect(EffectEvent::Completed { output, .. })
                if output == &json!({"runifold": {"content_recorded": false}})
        ));
    }

    #[test]
    fn full_event_payload_capture_requires_explicit_opt_in() {
        let capability = capability(EffectClass::Pure);
        let journal = InMemoryJournal::new();
        let run = run_with(&capability, Some(journal.clone()));
        let executor = EffectExecutor::new(Arc::new(InMemoryEffectStore::new()))
            .with_event_payload_policy(EffectEventPayloadPolicy::Full);
        let handler = CountingHandler::new();

        futures_executor::block_on(executor.execute(
            request(&capability, None, json!("secret")),
            &run,
            &handler,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .unwrap();

        assert!(matches!(
            &journal.events().last().unwrap().kind,
            RunEventKind::Effect(EffectEvent::Completed { output, .. })
                if output == &json!({"echo": "secret"})
        ));
    }

    #[test]
    fn idempotency_key_cannot_be_reused_for_different_input() {
        let capability = capability(EffectClass::IdempotentWrite);
        let run = run_with(&capability, None);
        let executor = EffectExecutor::new(Arc::new(InMemoryEffectStore::new()));
        let handler = CountingHandler::new();
        let first = request(&capability, Some("same-key"), json!({"value": 1}));
        futures_executor::block_on(executor.execute(
            first,
            &run,
            &handler,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .unwrap();
        let second = request(&capability, Some("same-key"), json!({"value": 2}));

        let error = futures_executor::block_on(executor.execute(
            second,
            &run,
            &handler,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .unwrap_err();

        assert_eq!(error.kind, EffectExecutorErrorKind::IdempotencyConflict);
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ambiguous_non_idempotent_effect_is_never_retried() {
        let capability = capability(EffectClass::NonIdempotentWrite);
        let run = run_with(&capability, None);
        let store = Arc::new(InMemoryEffectStore::new());
        let request = request(&capability, None, json!({"charge": 10}));
        let prepared = EffectRecord::prepared(request.clone());
        store.compare_and_swap(&prepared, None).unwrap();
        let started = prepared.next(EffectStatus::Started).unwrap();
        store.compare_and_swap(&started, Some(0)).unwrap();
        let executor = EffectExecutor::new(store);
        let handler = CountingHandler::new();

        let error = futures_executor::block_on(executor.execute(
            request,
            &run,
            &handler,
            EffectRecoveryPolicy::RetrySafe,
        ))
        .unwrap_err();

        assert_eq!(error.kind, EffectExecutorErrorKind::Ambiguous);
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn started_idempotent_effect_can_be_explicitly_reconciled_by_retry() {
        let capability = capability(EffectClass::IdempotentWrite);
        let run = run_with(&capability, None);
        let store = Arc::new(InMemoryEffectStore::new());
        let request = request(&capability, Some("safe-key"), json!({"write": 1}));
        let prepared = EffectRecord::prepared(request.clone());
        store.compare_and_swap(&prepared, None).unwrap();
        let started = prepared.next(EffectStatus::Started).unwrap();
        store.compare_and_swap(&started, Some(0)).unwrap();
        let executor = EffectExecutor::new(store);
        let handler = CountingHandler::new();

        let outcome = futures_executor::block_on(executor.execute(
            request,
            &run,
            &handler,
            EffectRecoveryPolicy::RetrySafe,
        ))
        .unwrap();

        assert_eq!(outcome.revision, 3);
        assert_eq!(handler.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn missing_capability_rejects_before_persistence_or_execution() {
        let capability = capability(EffectClass::Pure);
        let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
        let store = Arc::new(InMemoryEffectStore::new());
        let effect = request(&capability, None, json!({}));
        let executor = EffectExecutor::new(store.clone());
        let handler = CountingHandler::new();

        let error = futures_executor::block_on(executor.execute(
            effect.clone(),
            &run,
            &handler,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .unwrap_err();

        assert_eq!(error.kind, EffectExecutorErrorKind::CapabilityDenied);
        assert!(store.load(effect.effect_id).unwrap().is_none());
        assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
    }

    fn capability(effect: EffectClass) -> CapabilityDescriptor {
        CapabilityDescriptor {
            id: CapabilityId::new(),
            name: "test-effect".into(),
            version: "1".into(),
            kind: CapabilityKind::Resource,
            input_schema: json!({}),
            output_schema: json!({}),
            effect,
            risk: RiskLevel::Low,
            metadata: BTreeMap::new(),
        }
    }

    fn request(
        capability: &CapabilityDescriptor,
        key: Option<&str>,
        input: Value,
    ) -> EffectRequest {
        EffectRequest {
            effect_id: EffectId::new(),
            invocation_id: InvocationId::new(),
            kind: EffectKind::Extension("test".into()),
            capability_id: capability.id,
            input,
            effect_class: capability.effect,
            idempotency_key: key.map(str::to_owned),
        }
    }

    fn run_with(capability: &CapabilityDescriptor, journal: Option<InMemoryJournal>) -> RunContext {
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(capability.clone());
        let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
        match journal {
            Some(journal) => run.with_journal(Arc::new(journal)),
            None => run,
        }
    }
}
