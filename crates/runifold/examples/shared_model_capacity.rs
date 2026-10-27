//! Offline example: 100 Agents share a physical-request capacity pool.
//! Run with `cargo run -p runifold --example shared_model_capacity --locked`.
use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};

use futures_util::{StreamExt, future::try_join_all};
use runifold::{
    ContentPart, Model, ModelCallContext, ModelCapacityLimits, ModelCapacityPoolId,
    ModelEventStream, ModelRateLimit, ModelRef, ModelRequest, ModelScheduler, ModelSchedulerConfig,
    ProviderModel, ProviderModelExt, SupportLevel,
    model::{
        FeatureSupport, FinishReason, ModelCapabilities, ModelError, ModelFuture, ModelStreamEvent,
        ModelUsage,
    },
};
use runifold_testkit::ScriptedModel;

// Only the demo transport is scripted. Agent, routing, retry, and scheduling use
// the production path. A real ProviderModel can be bound identically.
struct DemoProvider(ScriptedModel);
impl ProviderModel for DemoProvider {
    fn provider(&self) -> &'static str {
        "demo"
    }
}
impl Model for DemoProvider {
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
                tokio::task::yield_now().await;
                event
            })) as ModelEventStream)
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let account = ModelCapacityPoolId::new("research-account")?;
    let scheduler = ModelScheduler::new(
        ModelSchedulerConfig::new(NonZeroUsize::new(128).expect("positive queue bound")).pool(
            account.clone(),
            ModelCapacityLimits::new(NonZeroUsize::new(4).expect("positive concurrency")).requests(
                ModelRateLimit::new(
                    NonZeroU64::new(600).expect("positive RPM"),
                    Duration::from_secs(60),
                )?,
            ),
        )?,
    )?;
    let scripted = ScriptedModel::new().with_capabilities(ModelCapabilities {
        streaming: FeatureSupport::new(SupportLevel::Native),
        ..ModelCapabilities::default()
    });
    for _ in 0..100 {
        scripted.enqueue([
            ModelStreamEvent::ResponseStarted {
                id: None,
                model: ModelRef::new("demo", "research"),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::text("ready"),
            },
            ModelStreamEvent::UsageUpdated {
                usage: ModelUsage {
                    input_tokens: 5,
                    output_tokens: 1,
                    ..ModelUsage::default()
                },
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: std::collections::BTreeMap::new(),
            },
        ]);
    }
    // Bind the provider BEFORE runtime construction: each physical retry gets
    // its own permit. Reuse this scheduler for other models sharing the account.
    let runtime = DemoProvider(scripted)
        .scheduled(&scheduler, [account])?
        .runtime("research")?;
    let agents = (0..100)
        .map(|index| runtime.agent(format!("research-{index}")).build())
        .collect::<Result<Vec<_>, _>>()?;
    let answers = try_join_all(
        agents
            .iter()
            .map(|agent| agent.prompt_text("Research this item")),
    )
    .await?;
    assert!(answers.iter().all(|answer| answer == "ready"));
    let snapshot = scheduler.snapshot();
    assert_eq!(snapshot.admitted, 100);
    assert_eq!(snapshot.queued, 0);
    assert_eq!(snapshot.pools[0].active, 0);
    println!(
        "Completed {} Agents with shared concurrency 4; {} physical attempts admitted.",
        answers.len(),
        snapshot.admitted
    );
    Ok(())
}
