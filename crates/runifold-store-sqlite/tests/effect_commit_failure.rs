//! A real `SQLite` completion failure must not authorize a second external write.

use std::{collections::BTreeMap, sync::Arc};

use runifold_core::{
    Budget, BudgetTracker, CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet,
    EffectClass, EffectId, EffectKind, EffectRequest, InvocationId, RiskLevel, RunContext,
    RunError,
};
use runifold_effect::{
    EffectExecutionContext, EffectExecutor, EffectExecutorErrorKind, EffectFuture, EffectHandler,
    EffectReconciler, EffectReconciliation, EffectRecoveryPolicy, EffectStatus, EffectStore,
};
use runifold_store_sqlite::SqliteStore;
use serde_json::{Value, json};

struct FilePublisher(std::path::PathBuf);

impl EffectHandler for FilePublisher {
    fn execute(
        &self,
        _request: &EffectRequest,
        _context: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<Value, RunError>> {
        Box::pin(async {
            // create_new makes a duplicate physical publication fail visibly.
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.0)
                .expect("external publication must execute exactly once in this fixture");
            std::io::Write::write_all(&mut file, b"published").expect("publish receipt");
            file.sync_all().expect("sync external receipt");
            Ok(json!({"published": true}))
        })
    }
}

impl EffectReconciler for FilePublisher {
    fn reconcile(
        &self,
        _request: &EffectRequest,
        _context: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<EffectReconciliation, RunError>> {
        Box::pin(async {
            assert_eq!(
                std::fs::read(&self.0).expect("remote receipt"),
                b"published"
            );
            Ok(EffectReconciliation::Completed(json!({"published": true})))
        })
    }
}

#[test]
fn completed_external_write_survives_local_commit_failure_and_reopen() {
    let directory = std::env::temp_dir().join(format!("runifold-effect-{}", EffectId::new()));
    std::fs::create_dir(&directory).expect("fixture directory");
    let database = directory.join("runtime.sqlite");
    let publisher = FilePublisher(directory.join("receipt"));
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
    let effect = EffectRequest {
        effect_id: EffectId::new(),
        invocation_id: InvocationId::new(),
        kind: EffectKind::Extension("publish".into()),
        capability_id: capability.id,
        input: json!({"report": "accepted"}),
        effect_class: capability.effect,
        idempotency_key: Some("report-1".into()),
    };
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(capability);
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    {
        let store = Arc::new(SqliteStore::open(&database).expect("store"));
        let connection = rusqlite::Connection::open(&database).expect("fault connection");
        connection
            .execute_batch(
                "CREATE TRIGGER fail_effect_completion BEFORE UPDATE ON runifold_effects
                 WHEN json_extract(NEW.record_json, '$.status.state') = 'completed'
                 BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;",
            )
            .expect("fault trigger");
        let error = futures_executor::block_on(EffectExecutor::new(store.clone()).execute(
            effect.clone(),
            &run,
            &publisher,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .expect_err("local completion must fail after external publication");
        assert_eq!(error.kind, EffectExecutorErrorKind::Store);
        let record = store.load(effect.effect_id).expect("load").expect("record");
        assert_eq!(record.status, EffectStatus::Started);
        assert_eq!(record.revision, 1);
        connection
            .execute_batch("DROP TRIGGER fail_effect_completion;")
            .expect("remove fault");
    }
    {
        let store = Arc::new(SqliteStore::open(&database).expect("fresh connection"));
        let executor = EffectExecutor::new(store.clone());
        let error = futures_executor::block_on(executor.execute(
            effect.clone(),
            &run,
            &publisher,
            EffectRecoveryPolicy::RetrySafe,
        ))
        .expect_err("a stable key alone cannot make a non-idempotent write retryable");
        assert_eq!(error.kind, EffectExecutorErrorKind::Ambiguous);
        assert_eq!(
            store
                .load(effect.effect_id)
                .expect("load")
                .expect("record")
                .revision,
            1
        );

        let recovered = futures_executor::block_on(executor.execute_reconciled(
            effect.clone(),
            &run,
            &publisher,
            &publisher,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .expect("receipt resolves the failed completion");
        assert!(recovered.replayed);
        assert_eq!(recovered.revision, 2);
        assert_eq!(recovered.output, json!({"published": true}));
    }
    {
        let store = Arc::new(SqliteStore::open(&database).expect("reopen completed record"));
        let replay = futures_executor::block_on(EffectExecutor::new(store).execute(
            effect,
            &run,
            &publisher,
            EffectRecoveryPolicy::RejectAmbiguous,
        ))
        .expect("completed record replays without publication");
        assert!(replay.replayed);
        assert_eq!(replay.revision, 2);
    }
    assert_eq!(
        std::fs::read(&publisher.0).expect("final receipt"),
        b"published"
    );
    std::fs::remove_dir_all(directory).expect("remove fixture");
}
