//! Application regressions use real native Agents and local HTTP transport.
#![cfg(all(feature = "runtime", not(target_arch = "wasm32")))]

use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use runifold::{
    AgentError, CompletionRequirement, ModelCapacityLimits, ModelCapacityPoolId, ModelScheduler,
    ModelSchedulerConfig, ProviderModelExt, ProviderRuntimeProfile, ResponseMode, RunContext,
    core::{Budget, BudgetTracker, CapabilitySet, InMemoryJournal, Instant, RunEventKind},
    model::{ModelRequestOutcome, ModelRouteReceipt},
};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ScriptedResponse};
use runifold_providers::openai::{OpenAiClient, OpenAiConfig};
use serde_json::{Value, json};

fn run(journal: &InMemoryJournal) -> RunContext {
    RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
        .with_journal(Arc::new(journal.clone()))
}

fn receipts(journal: &InMemoryJournal) -> Vec<ModelRouteReceipt> {
    journal
        .events()
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::Domain(domain)
                if domain.name == "model.completed" || domain.name == "model.failed" =>
            {
                Some(
                    serde_json::from_value(domain.payload["route_receipt"].clone())
                        .expect("native Agent must persist the real route receipt"),
                )
            }
            _ => None,
        })
        .collect()
}

fn response(status: &str, text: &str) -> Value {
    json!({"id":"resp_native", "model":"gpt-test", "status":status,
        "output":[{"type":"message", "id":"msg_native", "role":"assistant", "status":status,
            "content":[{"type":"output_text", "text":text, "annotations":[]}]}],
        "incomplete_details": if status == "incomplete" { json!({"reason":"max_output_tokens"}) } else { Value::Null },
        "usage":{"input_tokens":7,"output_tokens":11,"total_tokens":18}})
}

fn runtime(server: &CassetteServer) -> runifold::ProviderRuntime {
    let pool = ModelCapacityPoolId::new("native-account").unwrap();
    let scheduler = ModelScheduler::new(
        ModelSchedulerConfig::new(NonZeroUsize::new(8).unwrap())
            .pool(
                pool.clone(),
                ModelCapacityLimits::new(NonZeroUsize::new(1).unwrap()),
            )
            .unwrap(),
    )
    .unwrap();
    OpenAiClient::new(
        OpenAiConfig::new("cassette-only")
            .unwrap()
            .with_base_url(&format!("{}v1/", server.base_url()))
            .unwrap(),
    )
    .scheduled(&scheduler, [pool])
    .unwrap()
    .runtime_with_profile(
        "gpt-test",
        ProviderRuntimeProfile::conservative()
            .response_mode(ResponseMode::Complete)
            .without_circuit_breaker(),
    )
    .unwrap()
}

#[tokio::test]
async fn settled_native_receipts_survive_success_expiry_and_http_400_unknown_outcome() {
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", "/v1/responses", ScriptedResponse::json(200, &response("completed", "complete evidence")).unwrap()),
        HttpExchange::new("POST", "/v1/responses", ScriptedResponse::json(400, &json!({"error":{"type":"invalid_request_error","message":"PRIVATE_UPSTREAM_PAYLOAD"}})).unwrap()),
    ]).unwrap();
    let runtime = runtime(&server);
    let agent = runtime
        .agent("researcher")
        .completion_requirement(
            CompletionRequirement::new()
                .max_repairs(3)
                .retry_truncated_response(true),
        )
        .build()
        .unwrap();
    let journal = InMemoryJournal::new();
    agent.run("first window", &run(&journal)).await.unwrap();
    let first = receipts(&journal);
    assert_eq!(first.len(), 1);
    assert!(first[0].permits_continuation());
    assert_eq!(first[0].attempts[0].outcome, ModelRequestOutcome::Completed);
    assert_eq!(first[0].attempts[0].usage.unwrap().total_tokens(), 18);
    assert!(first[0].attempts[0].attempt_id.is_some());

    let expired = run(&journal).with_deadline(
        Instant::now()
            .checked_sub(Duration::from_millis(1))
            .unwrap(),
    );
    assert!(agent.run("expired window", &expired).await.is_err());
    assert_eq!(server.observed_requests().len(), 1);
    assert_eq!(receipts(&journal), first);

    assert!(matches!(
        agent.run("next window", &run(&journal)).await,
        Err(AgentError::Model(_))
    ));
    let all = receipts(&journal);
    assert_eq!(all.len(), 2);
    assert!(!all[1].permits_continuation());
    assert_eq!(all[1].attempts[0].outcome, ModelRequestOutcome::Unknown);
    assert_eq!(all[1].attempts[0].diagnostics["http.status"], 400);
    assert_eq!(
        all[1].attempts[0].diagnostics["runifold.request.provider_invoked"],
        true
    );
    assert!(all[1].attempts[0].attempt_id.is_some());
    assert!(
        !serde_json::to_string(&all)
            .unwrap()
            .contains("PRIVATE_UPSTREAM_PAYLOAD")
    );
    assert_eq!(server.observed_requests().len(), 2);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn native_http_length_continuation_keeps_usage_and_original_evidence() {
    let original = "original <evidence> & exact bytes 🦀";
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1/responses",
            ScriptedResponse::json(200, &response("incomplete", original)).unwrap(),
        ),
        HttpExchange::new(
            "POST",
            "/v1/responses",
            ScriptedResponse::json(200, &response("completed", "complete independent delivery"))
                .unwrap(),
        ),
    ])
    .unwrap();
    let journal = InMemoryJournal::new();
    let scope = run(&journal);
    let agent = runtime(&server)
        .agent("specialist")
        .completion_requirement(
            CompletionRequirement::new()
                .max_repairs(1)
                .retry_truncated_response(true),
        )
        .build()
        .unwrap();
    let outcome = agent.run("research", &scope).await.unwrap();
    assert_eq!(outcome.text(), "complete independent delivery");
    assert!(
        outcome
            .transcript
            .iter()
            .all(|message| !message.metadata.contains_key("runifold.router.receipt"))
    );
    assert_eq!(scope.budget().usage().tokens, 36);
    let observed = server.observed_requests();
    let second = observed[1].json_body().unwrap();
    assert!(second["input"].as_array().unwrap().iter().any(|item| {
        item["role"] == "assistant"
            && item["content"]
                .as_array()
                .is_some_and(|content| content.iter().any(|part| part["text"] == original))
    }));
    let receipts = receipts(&journal);
    assert_eq!(receipts.len(), 2);
    assert!(receipts.iter().all(ModelRouteReceipt::permits_continuation));
    server.assert_finished().unwrap();
}

#[test]
fn native_failed_attempt_usage_is_preserved_in_receipts_and_run_budget() {
    use runifold::model::{
        ModelCapabilities, ModelError, ModelErrorKind, ModelEventStream, ModelFuture, ModelRef,
        ModelRequest, ModelStreamEvent, ModelUsage,
    };
    use runifold::{Agent, Model, ModelCallContext, ModelRouter};

    struct PartialFailure;
    impl Model for PartialFailure {
        fn capabilities<'a>(
            &'a self,
            _: &'a ModelRef,
        ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
            Box::pin(async { Ok(ModelCapabilities::default()) })
        }
        fn stream(
            &self,
            request: ModelRequest,
            _: ModelCallContext,
        ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
            Box::pin(async move {
                Ok(Box::pin(futures_util::stream::iter([
                    Ok(ModelStreamEvent::ResponseStarted {
                        id: None,
                        model: request.model,
                    }),
                    Ok(ModelStreamEvent::UsageUpdated {
                        usage: ModelUsage {
                            input_tokens: 7,
                            output_tokens: 11,
                            ..Default::default()
                        },
                    }),
                    Err(ModelError::local(
                        ModelErrorKind::Transport,
                        "ambiguous stream loss",
                    )),
                ])) as ModelEventStream)
            })
        }
    }
    for mode in [ResponseMode::Complete, ResponseMode::Streaming] {
        let target = ModelRef::new("native", "partial");
        let router = ModelRouter::builder(target.clone())
            .route("physical", Arc::new(PartialFailure), target.clone())
            .build()
            .unwrap();
        let agent = Agent::builder("researcher", Arc::new(router), target)
            .response_mode(mode)
            .completion_requirement(
                CompletionRequirement::new()
                    .max_repairs(3)
                    .retry_truncated_response(true),
            )
            .build()
            .unwrap();
        let journal = InMemoryJournal::new();
        let scope = run(&journal);
        assert!(matches!(
            futures_executor::block_on(agent.run("research", &scope)),
            Err(AgentError::Model(_))
        ));
        let receipts = receipts(&journal);
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].attempts.len(), 1);
        assert!(!receipts[0].permits_continuation());
        assert_eq!(receipts[0].attempts[0].usage.unwrap().total_tokens(), 18);
        assert_eq!(scope.budget().usage().tokens, 18);
    }
}

#[tokio::test]
async fn cancellation_after_native_retry_keeps_prior_failures_and_pending_attempt() {
    let mut delayed = ScriptedResponse::json(200, &response("completed", "too late")).unwrap();
    delayed.chunks[0].delay = Duration::from_millis(400);
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1/responses",
            ScriptedResponse::json(
                503,
                &json!({"error":{"type":"server_error","message":"temporary"}}),
            )
            .unwrap(),
        ),
        HttpExchange::new("POST", "/v1/responses", delayed),
    ])
    .unwrap();
    // The fixture explicitly authorizes a second physical request despite an
    // unknown first outcome. Production defaults must remain conservative.
    let runtime = OpenAiClient::new(
        OpenAiConfig::new("cassette-only")
            .unwrap()
            .with_base_url(&format!("{}v1/", server.base_url()))
            .unwrap(),
    )
    .runtime_with_profile(
        "gpt-test",
        ProviderRuntimeProfile::conservative()
            .response_mode(ResponseMode::Complete)
            .without_circuit_breaker()
            .retry_policy(
                runifold::ModelRetryPolicy::default()
                    .allow_unknown(runifold::model::ModelErrorKind::Provider),
            ),
    )
    .unwrap();
    let agent = runtime.agent("researcher").build().unwrap();
    let journal = InMemoryJournal::new();
    let scope = run(&journal);
    let cancel = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.observed_requests().len() < 2 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("native retry must dispatch the second physical request");
        scope.cancellation().cancel();
    };
    let (result, ()) = tokio::join!(agent.run("research", &scope), cancel);
    assert!(
        matches!(result, Err(AgentError::Model(error)) if error.kind == runifold::model::ModelErrorKind::Cancelled)
    );
    let settled = receipts(&journal);
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].attempts.len(), 2);
    assert_eq!(settled[0].attempts[0].diagnostics["http.status"], 503);
    assert_eq!(settled[0].attempts[1].outcome, ModelRequestOutcome::Unknown);
    assert!(!settled[0].permits_continuation());
    assert_ne!(
        settled[0].attempts[0].attempt_id,
        settled[0].attempts[1].attempt_id
    );
}

#[tokio::test]
async fn native_model_collector_cancellation_preserves_context_snapshot() {
    use runifold::{Message, Model, ModelCallContext, ModelRequest};
    let mut delayed = ScriptedResponse::json(200, &response("completed", "too late")).unwrap();
    delayed.chunks[0].delay = Duration::from_millis(400);
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/v1/responses", delayed)]).unwrap();
    let runtime = runtime(&server);
    let context = ModelCallContext::new();
    let invocation = context.clone();
    let cancellation = context.cancellation().clone();
    let cancel = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.observed_requests().is_empty() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
    };
    let request = ModelRequest::new(runtime.model_ref().clone(), Message::user("research"));
    let (result, ()) = tokio::join!(runtime.invoke(request, invocation), cancel);
    let error = result.unwrap_err();
    assert_eq!(error.kind, runifold::model::ModelErrorKind::Cancelled);
    let receipt = context
        .route_receipt()
        .expect("pending native attempt is observable after cancellation");
    assert_eq!(receipt.attempts.len(), 1);
    assert_eq!(receipt.attempts[0].outcome, ModelRequestOutcome::Unknown);
    assert!(!receipt.permits_continuation());
    assert_eq!(
        serde_json::to_value(&receipt).unwrap(),
        error.metadata["runifold.router.receipt"]
    );
    assert!(context.child_attempt().route_receipt().is_none());
}
