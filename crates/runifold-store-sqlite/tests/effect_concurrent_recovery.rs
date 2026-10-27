//! Independent `SQLite` recovery callers must not both advance a stale attempt.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use runifold_core::{
    Budget, BudgetTracker, CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet,
    EffectClass, EffectId, EffectKind, EffectRequest, InvocationId, RiskLevel, RunContext,
    RunError,
};
use runifold_effect::{
    EffectExecutionContext, EffectExecutor, EffectExecutorErrorKind, EffectFuture, EffectHandler,
    EffectReconciler, EffectReconciliation, EffectRecord, EffectRecoveryPolicy, EffectStatus,
    EffectStore,
};
use runifold_store_sqlite::SqliteStore;
use serde_json::{Value, json};

struct CountingHandler(AtomicUsize);

impl EffectHandler for CountingHandler {
    fn execute(
        &self,
        _request: &EffectRequest,
        _context: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<Value, RunError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(json!({"receipt": "accepted"})) })
    }
}

struct SynchronizedReconciler {
    arrivals: Mutex<u8>,
    ready: Condvar,
    result: EffectReconciliation,
}

impl EffectReconciler for SynchronizedReconciler {
    fn reconcile(
        &self,
        _request: &EffectRequest,
        _context: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<EffectReconciliation, RunError>> {
        Box::pin(async {
            // Each caller has already loaded the same Started revision. The
            // bounded barrier forces their subsequent writes to contend.
            let mut arrivals = self.arrivals.lock().expect("barrier lock");
            *arrivals += 1;
            self.ready.notify_all();
            let (arrivals, _) = self
                .ready
                .wait_timeout_while(arrivals, Duration::from_secs(10), |count| *count < 2)
                .expect("barrier wait");
            assert_eq!(*arrivals, 2, "both recovery callers must arrive");
            Ok(self.result.clone())
        })
    }
}

#[test]
fn concurrent_reconciliation_from_one_revision_has_one_commit_winner() {
    for result in [
        EffectReconciliation::Completed(json!({"receipt": "accepted"})),
        EffectReconciliation::NotExecuted,
    ] {
        let directory = std::env::temp_dir().join(format!("runifold-recover-{}", EffectId::new()));
        std::fs::create_dir(&directory).expect("fixture directory");
        let database = directory.join("runtime.sqlite");
        let capability = CapabilityDescriptor {
            id: CapabilityId::new(),
            name: "publish".into(),
            version: "1".into(),
            kind: CapabilityKind::Resource,
            input_schema: json!({}),
            output_schema: json!({}),
            effect: EffectClass::NonIdempotentWrite,
            risk: RiskLevel::High,
            metadata: BTreeMap::new(),
        };
        let request = EffectRequest {
            effect_id: EffectId::new(),
            invocation_id: InvocationId::new(),
            kind: EffectKind::Extension("publish".into()),
            capability_id: capability.id,
            input: json!({"publish": true}),
            effect_class: capability.effect,
            idempotency_key: Some("publication-1".into()),
        };
        let mut grants = CapabilitySet::new();
        grants.grant(capability);
        let run = RunContext::root(BudgetTracker::new(Budget::default()), grants);
        {
            let store = SqliteStore::open(&database).expect("seed connection");
            store
                .compare_and_swap(&EffectRecord::prepared(request.clone()), None)
                .expect("prepared");
            store
                .compare_and_swap(
                    &EffectRecord {
                        request: request.clone(),
                        revision: 1,
                        status: EffectStatus::Started,
                    },
                    Some(0),
                )
                .expect("started");
        }
        let expected_calls = usize::from(matches!(result, EffectReconciliation::NotExecuted));
        let reconciler = SynchronizedReconciler {
            arrivals: Mutex::new(0),
            ready: Condvar::new(),
            result,
        };
        let handler = CountingHandler(AtomicUsize::new(0));
        let recover = || {
            let store = Arc::new(SqliteStore::open(&database).expect("independent connection"));
            futures_executor::block_on(EffectExecutor::new(store).execute_reconciled(
                request.clone(),
                &run,
                &handler,
                &reconciler,
                EffectRecoveryPolicy::RejectAmbiguous,
            ))
        };
        let results = std::thread::scope(|scope| {
            let first = scope.spawn(recover);
            let second = scope.spawn(recover);
            [
                first.join().expect("first worker"),
                second.join().expect("second worker"),
            ]
        });
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let loser = results
            .iter()
            .find_map(|result| result.as_ref().err())
            .expect("stale worker loses");
        assert_eq!(loser.kind, EffectExecutorErrorKind::Store);
        assert_eq!(handler.0.load(Ordering::SeqCst), expected_calls);
        {
            let store = Arc::new(SqliteStore::open(&database).expect("fresh replay connection"));
            let outcome = futures_executor::block_on(EffectExecutor::new(store).execute(
                request,
                &run,
                &handler,
                EffectRecoveryPolicy::RejectAmbiguous,
            ))
            .expect("winner completion replays");
            assert!(outcome.replayed);
            assert_eq!(outcome.output, json!({"receipt": "accepted"}));
            assert_eq!(
                outcome.revision,
                2 + u64::try_from(expected_calls).expect("bounded count")
            );
        }
        assert_eq!(handler.0.load(Ordering::SeqCst), expected_calls);
        std::fs::remove_dir_all(directory).expect("remove fixture");
    }
}
