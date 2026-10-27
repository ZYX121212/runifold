//! Public facade contracts: `ProviderRuntime`, Agents, invoke and stream share admission.
#![cfg(all(feature = "runtime", not(target_arch = "wasm32")))]
use futures_util::{StreamExt, future::try_join_all};
use runifold::{
    ContentPart, Model, ModelCallContext, ModelCapacityLimits, ModelCapacityPoolId,
    ModelEventStream, ModelRequest, ModelScheduler, ModelSchedulerConfig, ModelTokenEstimator,
    ModelTokenSettlement, ProviderModel, ProviderModelExt, SupportLevel,
    model::{
        FeatureSupport, FinishReason, ModelCapabilities, ModelError, ModelFuture, ModelRef,
        ModelStreamEvent, ModelUsage,
    },
};
use runifold_testkit::ScriptedModel;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

struct LocalProvider(ScriptedModel);
impl ProviderModel for LocalProvider {
    fn provider(&self) -> &'static str {
        "local"
    }
}
impl Model for LocalProvider {
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        self.0.capabilities(model)
    }
    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            let stream = self.0.stream(request, context).await?;
            Ok(Box::pin(stream.then(|event| async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                event
            })) as ModelEventStream)
        })
    }
}
struct Reservation(Mutex<Vec<ModelRequest>>);
impl ModelTokenEstimator for Reservation {
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ModelError> {
        self.0.lock().unwrap().push(request.clone());
        Ok(100)
    }
}

#[tokio::test]
async fn public_runtime_agents_and_both_model_modes_share_one_scheduler() {
    let pool = ModelCapacityPoolId::new("account").unwrap();
    let scheduler = ModelScheduler::new(
        ModelSchedulerConfig::new(NonZeroUsize::new(64).unwrap())
            .pool(
                pool.clone(),
                ModelCapacityLimits::new(NonZeroUsize::new(3).unwrap()),
            )
            .unwrap(),
    )
    .unwrap();
    let script = ScriptedModel::new().with_capabilities(ModelCapabilities {
        streaming: FeatureSupport::new(SupportLevel::Native),
        ..ModelCapabilities::default()
    });
    for _ in 0..26 {
        script.enqueue([
            ModelStreamEvent::ResponseStarted {
                id: None,
                model: ModelRef::new("local", "test"),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::text("answer"),
            },
            ModelStreamEvent::UsageUpdated {
                usage: ModelUsage {
                    input_tokens: 4,
                    output_tokens: 2,
                    ..ModelUsage::default()
                },
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: std::collections::BTreeMap::new(),
            },
        ]);
    }
    let reservation = Arc::new(Reservation(Mutex::new(Vec::new())));
    let provider = LocalProvider(script.clone())
        .scheduled(&scheduler, [pool])
        .unwrap()
        .token_estimator(reservation.clone())
        .token_settlement(ModelTokenSettlement::ReportedUsage);
    let runtime = provider.runtime("test").unwrap();
    let agents = (0..24)
        .map(|index| {
            runtime
                .clone()
                .agent(format!("agent-{index}"))
                .build()
                .unwrap()
        })
        .collect::<Vec<_>>();
    let pending = try_join_all(agents.iter().map(|agent| agent.prompt_text("hello")));
    tokio::pin!(pending);
    tokio::select! {
        biased;
        result = &mut pending => panic!("requests unexpectedly finished before overlap check: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(1)) => {}
    }
    let overlap = scheduler.snapshot();
    assert_eq!(overlap.pools[0].active, 3);
    assert!(overlap.queued > 0);
    assert!(pending.await.unwrap().iter().all(|text| text == "answer"));
    let request = || {
        ModelRequest::new(
            ModelRef::new("local", "test"),
            runifold::Message::user("hello"),
        )
    };
    assert_eq!(
        runtime
            .invoke(request(), ModelCallContext::new())
            .await
            .unwrap()
            .text(),
        "answer"
    );
    let mut stream = runtime
        .stream(request(), ModelCallContext::new())
        .await
        .unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    let done = scheduler.snapshot();
    assert_eq!(done.admitted, 26);
    assert_eq!(done.pools[0].active, 0);
    assert_eq!(done.queued, 0);
    assert_eq!(done.unknown_usage, 0);
    assert_eq!(script.recorded_requests().len(), 26);
    assert_eq!(*reservation.0.lock().unwrap(), script.recorded_requests());
}
