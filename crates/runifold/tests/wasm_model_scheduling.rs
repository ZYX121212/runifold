//! Node WASM verification of local admission timers and canonical stream release.
#![cfg(target_arch = "wasm32")]

use futures_util::StreamExt;
use runifold::{
    Model, ModelCallContext, ModelCapacityLimits, ModelCapacityPoolId, ModelEventStream,
    ModelRateLimit, ModelRef, ModelRequest, ModelScheduler, ModelSchedulerConfig,
    model::{FinishReason, ModelCapabilities, ModelError, ModelFuture, ModelStreamEvent},
};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    time::Duration,
};
use wasm_bindgen_test::wasm_bindgen_test;

struct Local;
impl Model for Local {
    fn capabilities<'a>(
        &'a self,
        _: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        Box::pin(async { Ok(ModelCapabilities::default()) })
    }
    fn stream(
        &self,
        _: ModelRequest,
        _: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async {
            Ok(Box::pin(futures_util::stream::iter([Ok(
                ModelStreamEvent::ResponseCompleted {
                    finish_reason: FinishReason::Stop,
                    provider_metadata: std::collections::BTreeMap::new(),
                },
            )])) as ModelEventStream)
        })
    }
}

#[wasm_bindgen_test]
async fn scheduler_rate_timer_and_release_run_in_wasm() {
    let account = ModelCapacityPoolId::new("wasm-account").unwrap();
    let scheduler = ModelScheduler::new(
        ModelSchedulerConfig::new(NonZeroUsize::MIN)
            .pool(
                account.clone(),
                ModelCapacityLimits::new(NonZeroUsize::MIN).requests(
                    ModelRateLimit::new(NonZeroU64::MIN, Duration::from_millis(20)).unwrap(),
                ),
            )
            .unwrap(),
    )
    .unwrap();
    let model = scheduler.bind(Local, [account]).unwrap();
    for _ in 0..2 {
        let request = ModelRequest::new(
            ModelRef::new("local", "test"),
            runifold::Message::user("test"),
        );
        let mut stream = model
            .stream(request, ModelCallContext::new())
            .await
            .unwrap();
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(scheduler.snapshot().pools[0].active, 0);
    }
    assert_eq!(scheduler.snapshot().admitted, 2);
}
