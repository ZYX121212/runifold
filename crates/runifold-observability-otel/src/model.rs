use std::{fmt, sync::Arc, time::Instant};

use futures_util::StreamExt;
use opentelemetry::{
    Array, Context, KeyValue, Value, global,
    global::{BoxedSpan, BoxedTracer},
    metrics::{Counter, Histogram, Meter},
    trace::{Span, SpanKind, Status, Tracer},
};
use runifold_model::{
    FinishReason, Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind,
    ModelEventStream, ModelFuture, ModelRef, ModelRequest, ModelResponse, ModelStreamAccumulator,
    ModelStreamEvent, ModelUsage, Role,
};

use crate::{
    ContentCapture, CorrelationRegistry, OtelConfig,
    slo::{MODEL_OPERATION_DURATION_SECONDS, MODEL_TIME_TO_FIRST_CHUNK_SECONDS, metric_names},
};

const SCOPE: &str = "runifold.observability.otel";

/// Provider-neutral [`Model`] decorator emitting OpenTelemetry `GenAI` signals.
pub struct OtelModel {
    inner: Arc<dyn Model>,
    tracer: Arc<BoxedTracer>,
    instruments: Instruments,
    config: OtelConfig,
    correlation: Option<Arc<CorrelationRegistry>>,
}

impl OtelModel {
    /// Wraps a model using the globally configured tracer and meter providers.
    pub fn new<M>(inner: M) -> Self
    where
        M: Model + 'static,
    {
        let meter = global::meter(SCOPE);
        Self::from_parts(
            Arc::new(inner),
            global::tracer(SCOPE),
            &meter,
            OtelConfig::default(),
        )
    }

    /// Wraps an existing object-safe model.
    pub fn from_arc(inner: Arc<dyn Model>) -> Self {
        let meter = global::meter(SCOPE);
        Self::from_parts(inner, global::tracer(SCOPE), &meter, OtelConfig::default())
    }

    /// Injects explicit telemetry providers, primarily for isolated tests and
    /// applications that do not use OpenTelemetry globals.
    pub fn from_parts(
        inner: Arc<dyn Model>,
        tracer: BoxedTracer,
        meter: &Meter,
        config: OtelConfig,
    ) -> Self {
        Self {
            inner,
            tracer: Arc::new(tracer),
            instruments: Instruments::new(meter),
            config,
            correlation: None,
        }
    }

    pub(crate) fn from_shared_parts(
        inner: Arc<dyn Model>,
        tracer: Arc<BoxedTracer>,
        meter: &Meter,
        config: OtelConfig,
        correlation: Arc<CorrelationRegistry>,
    ) -> Self {
        Self {
            inner,
            tracer,
            instruments: Instruments::new(meter),
            config,
            correlation: Some(correlation),
        }
    }

    /// Replaces the safe-default capture policy.
    #[must_use]
    pub fn with_config(mut self, config: OtelConfig) -> Self {
        self.config = config;
        self
    }
}

impl fmt::Debug for OtelModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtelModel")
            .field("inner", &"dyn Model")
            .field("tracer", &self.tracer)
            .field("instruments", &self.instruments)
            .field("config", &self.config)
            .field("causal_correlation", &self.correlation.is_some())
            .finish()
    }
}

impl Model for OtelModel {
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        self.inner.capabilities(model)
    }

    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            let mut call = ActiveCall::start(
                self.tracer.as_ref(),
                self.instruments.clone(),
                &request,
                &context,
                self.config.clone(),
                self.correlation.as_deref(),
            );
            let stream = match self.inner.stream(request, context).await {
                Ok(stream) => stream,
                Err(error) => {
                    call.fail(&error);
                    return Err(error);
                }
            };
            Ok(instrument_stream(stream, call))
        })
    }
}

#[derive(Clone, Debug)]
struct Instruments {
    duration: Histogram<f64>,
    token_usage: Histogram<u64>,
    time_to_first_chunk: Histogram<f64>,
    route_failures: Counter<u64>,
    route_selections: Counter<u64>,
}

impl Instruments {
    fn new(meter: &Meter) -> Self {
        Self {
            duration: meter
                .f64_histogram(metric_names::MODEL_OPERATION_DURATION)
                .with_unit("s")
                .with_description("GenAI operation duration.")
                .with_boundaries(MODEL_OPERATION_DURATION_SECONDS.to_vec())
                .build(),
            token_usage: meter
                .u64_histogram("gen_ai.client.token.usage")
                .with_unit("{token}")
                .with_description("Number of input and output tokens used.")
                .build(),
            time_to_first_chunk: meter
                .f64_histogram(metric_names::MODEL_TIME_TO_FIRST_CHUNK)
                .with_unit("s")
                .with_description("Time until the first model-visible response chunk.")
                .with_boundaries(MODEL_TIME_TO_FIRST_CHUNK_SECONDS.to_vec())
                .build(),
            route_failures: meter
                .u64_counter("runifold.model.route.failures")
                .with_description("Logical model route attempts that failed before commitment.")
                .build(),
            route_selections: meter
                .u64_counter("runifold.model.route.selections")
                .with_description("Logical model route selections.")
                .build(),
        }
    }
}

#[derive(Debug)]
struct ActiveCall {
    span: BoxedSpan,
    instruments: Instruments,
    metric_attributes: Vec<KeyValue>,
    started: Instant,
    ended: bool,
    first_chunk_recorded: bool,
    reported_route_failures: usize,
    config: OtelConfig,
}

impl ActiveCall {
    fn start(
        tracer: &BoxedTracer,
        instruments: Instruments,
        request: &ModelRequest,
        context: &ModelCallContext,
        config: OtelConfig,
        correlation: Option<&CorrelationRegistry>,
    ) -> Self {
        let operation = operation_name(&request.model.provider);
        let provider = provider_name(&request.model.provider);
        let metric_attributes = vec![
            KeyValue::new("gen_ai.operation.name", operation),
            KeyValue::new("gen_ai.provider.name", provider),
            KeyValue::new("gen_ai.request.model", request.model.name.clone()),
        ];
        let mut attributes = metric_attributes.clone();
        attributes.push(KeyValue::new("gen_ai.request.stream", true));
        attributes.push(KeyValue::new(
            "runifold.model.invocation.id",
            context.invocation_id().to_string(),
        ));
        if let Some(run_id) = context.run_id() {
            attributes.push(KeyValue::new("runifold.run.id", run_id.to_string()));
        }
        add_generation_attributes(&mut attributes, request);
        add_captured_request(&mut attributes, request, config.content_capture);
        let parent = context
            .run_id()
            .and_then(|run_id| correlation.and_then(|registry| registry.current(run_id)))
            .unwrap_or_else(Context::current);
        let span = tracer
            .span_builder(format!("{operation} {}", request.model.name))
            .with_kind(SpanKind::Client)
            .with_attributes(attributes)
            .start_with_context(tracer, &parent);
        Self {
            span,
            instruments,
            metric_attributes,
            started: Instant::now(),
            ended: false,
            first_chunk_recorded: false,
            reported_route_failures: 0,
            config,
        }
    }

    fn observe(&mut self, event: &ModelStreamEvent, response: Option<&ModelResponse>) {
        if is_first_chunk(event) {
            self.record_first_chunk();
        }
        match event {
            ModelStreamEvent::ResponseStarted { id, model } => {
                if let Some(id) = id {
                    self.span
                        .set_attribute(KeyValue::new("gen_ai.response.id", id.clone()));
                }
                self.span
                    .set_attribute(KeyValue::new("gen_ai.response.model", model.name.clone()));
            }
            ModelStreamEvent::UsageUpdated { usage } => self.set_usage(*usage),
            ModelStreamEvent::Provider { event }
                if event.provider == "runifold.router" && event.name == "route.selected" =>
            {
                self.observe_route_selection(&event.payload);
            }
            ModelStreamEvent::ResponseCompleted { finish_reason, .. } => {
                self.set_finish_reason(finish_reason);
                if let Some(response) = response {
                    self.capture_response(response);
                }
                self.finish();
            }
            _ => {}
        }
    }

    fn observe_route_selection(&mut self, payload: &serde_json::Value) {
        let route = payload
            .get("route")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let route_attempt = payload
            .get("route_attempt")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1);
        let attempt_id = payload
            .get("attempt_id")
            .and_then(serde_json::Value::as_str);
        let prior_failures = payload
            .get("prior_failures")
            .and_then(serde_json::Value::as_array)
            .map_or(&[][..], Vec::as_slice);
        let fallback = !prior_failures.is_empty();
        self.span.set_attributes([
            KeyValue::new("runifold.model.route.name", route.to_owned()),
            KeyValue::new(
                "runifold.model.route.attempt",
                i64::try_from(route_attempt).unwrap_or(i64::MAX),
            ),
            KeyValue::new(
                "runifold.model.route.prior_failures",
                i64::try_from(prior_failures.len()).unwrap_or(i64::MAX),
            ),
            KeyValue::new("runifold.model.route.fallback", fallback),
        ]);
        self.instruments.route_selections.add(
            1,
            &[KeyValue::new("runifold.model.route.fallback", fallback)],
        );
        if let Some(attempt_id) = attempt_id {
            self.span.set_attribute(KeyValue::new(
                "runifold.model.attempt.id",
                attempt_id.to_owned(),
            ));
        }
        self.observe_route_failures(prior_failures);
        self.span.add_event(
            "runifold.model.route.selected",
            vec![
                KeyValue::new("runifold.model.route.name", route.to_owned()),
                KeyValue::new(
                    "runifold.model.route.attempt",
                    i64::try_from(route_attempt).unwrap_or(i64::MAX),
                ),
                KeyValue::new(
                    "runifold.model.route.circuit_probe",
                    payload
                        .get("circuit_probe")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false),
                ),
            ],
        );
    }

    fn observe_route_failures(&mut self, failures: &[serde_json::Value]) {
        for failure in failures.iter().skip(self.reported_route_failures) {
            let error_type = failure
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("_OTHER");
            self.instruments
                .route_failures
                .add(1, &[KeyValue::new("error.type", error_type.to_owned())]);
            self.span.add_event(
                "runifold.model.route.failure",
                route_failure_attributes(failure),
            );
        }
        self.reported_route_failures = self.reported_route_failures.max(failures.len());
    }

    fn record_first_chunk(&mut self) {
        if self.first_chunk_recorded {
            return;
        }
        let elapsed = self.started.elapsed().as_secs_f64();
        self.span.set_attribute(KeyValue::new(
            "gen_ai.response.time_to_first_chunk",
            elapsed,
        ));
        self.instruments
            .time_to_first_chunk
            .record(elapsed, &self.metric_attributes);
        self.first_chunk_recorded = true;
    }

    fn set_usage(&mut self, usage: ModelUsage) {
        self.span.set_attributes([
            KeyValue::new("gen_ai.usage.input_tokens", as_i64(usage.input_tokens)),
            KeyValue::new("gen_ai.usage.output_tokens", as_i64(usage.output_tokens)),
            KeyValue::new(
                "gen_ai.usage.reasoning.output_tokens",
                as_i64(usage.reasoning_tokens),
            ),
            KeyValue::new(
                "gen_ai.usage.cache_read.input_tokens",
                as_i64(usage.cached_input_tokens),
            ),
            KeyValue::new(
                "gen_ai.usage.cache_creation.input_tokens",
                as_i64(usage.cache_write_tokens),
            ),
        ]);
        self.record_tokens("input", usage.input_tokens);
        self.record_tokens("output", usage.output_tokens);
    }

    fn set_finish_reason(&mut self, reason: &FinishReason) {
        let reasons = vec![finish_reason(reason).into()];
        self.span.set_attribute(KeyValue::new(
            "gen_ai.response.finish_reasons",
            Value::Array(Array::String(reasons)),
        ));
    }

    fn capture_response(&mut self, response: &ModelResponse) {
        if matches!(self.config.content_capture, ContentCapture::Disabled) {
            return;
        }
        if let Ok(encoded) = serde_json::to_string(&response.content) {
            self.span
                .set_attribute(KeyValue::new("gen_ai.output.messages", encoded));
        }
    }

    fn fail(&mut self, error: &ModelError) {
        if self.ended {
            return;
        }
        if let Some(failures) = error
            .metadata
            .get("runifold.router.failures")
            .and_then(serde_json::Value::as_array)
        {
            self.observe_route_failures(failures);
        }
        let error_type = error_type(&error.kind);
        self.span
            .set_attribute(KeyValue::new("error.type", error_type));
        let mut event_attributes = vec![KeyValue::new("error.type", error_type)];
        event_attributes.push(KeyValue::new(
            "runifold.diagnostic.code",
            error.diagnostic_code(),
        ));
        event_attributes.extend(diagnostic_attributes(error));
        if self.config.capture_error_messages {
            event_attributes.push(KeyValue::new("exception.message", error.message.clone()));
        }
        self.span
            .add_event("gen_ai.client.operation.exception", event_attributes);
        self.span.set_status(Status::error(error_type));
        self.finish();
    }

    fn fail_abandoned(&mut self) {
        if self.ended {
            return;
        }
        self.span
            .set_attribute(KeyValue::new("error.type", "cancelled_or_abandoned_stream"));
        self.span
            .set_status(Status::error("cancelled_or_abandoned_stream"));
        self.finish();
    }

    fn record_tokens(&self, token_type: &'static str, count: u64) {
        let mut attributes = self.metric_attributes.clone();
        attributes.push(KeyValue::new("gen_ai.token.type", token_type));
        self.instruments.token_usage.record(count, &attributes);
    }

    fn finish(&mut self) {
        if self.ended {
            return;
        }
        self.instruments.duration.record(
            self.started.elapsed().as_secs_f64(),
            &self.metric_attributes,
        );
        self.span.end();
        self.ended = true;
    }
}

impl Drop for ActiveCall {
    fn drop(&mut self) {
        self.fail_abandoned();
    }
}

fn instrument_stream(mut stream: ModelEventStream, mut call: ActiveCall) -> ModelEventStream {
    Box::pin(async_stream::try_stream! {
        let mut accumulator = Some(ModelStreamAccumulator::new());
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    let response = accumulator.as_mut().and_then(|accumulator| {
                        if let Ok(response) = accumulator.push(event.clone()) {
                            response
                        } else {
                            *accumulator = ModelStreamAccumulator::new();
                            None
                        }
                    });
                    call.observe(&event, response.as_ref());
                    yield event;
                }
                Err(error) => {
                    call.fail(&error);
                    Err(error)?;
                }
            }
        }
    })
}

fn add_generation_attributes(attributes: &mut Vec<KeyValue>, request: &ModelRequest) {
    if let Some(temperature) = request.generation.temperature {
        attributes.push(KeyValue::new("gen_ai.request.temperature", temperature));
    }
    if let Some(top_p) = request.generation.top_p {
        attributes.push(KeyValue::new("gen_ai.request.top_p", top_p));
    }
    if let Some(max_tokens) = request.generation.max_output_tokens {
        attributes.push(KeyValue::new(
            "gen_ai.request.max_tokens",
            as_i64(max_tokens),
        ));
    }
}

fn add_captured_request(
    attributes: &mut Vec<KeyValue>,
    request: &ModelRequest,
    capture: ContentCapture,
) {
    if matches!(capture, ContentCapture::Disabled) {
        return;
    }
    let system = request
        .messages
        .iter()
        .filter(|message| message.role == Role::System)
        .collect::<Vec<_>>();
    let input = request
        .messages
        .iter()
        .filter(|message| message.role != Role::System)
        .collect::<Vec<_>>();
    if let Ok(encoded) = serde_json::to_string(&system) {
        attributes.push(KeyValue::new("gen_ai.system_instructions", encoded));
    }
    if let Ok(encoded) = serde_json::to_string(&input) {
        attributes.push(KeyValue::new("gen_ai.input.messages", encoded));
    }
    if matches!(capture, ContentCapture::MessagesAndTools)
        && let Ok(encoded) = serde_json::to_string(&request.tools)
    {
        attributes.push(KeyValue::new("gen_ai.tool.definitions", encoded));
    }
}

fn operation_name(provider: &str) -> &'static str {
    if provider == "gemini" {
        "generate_content"
    } else {
        "chat"
    }
}

fn provider_name(provider: &str) -> String {
    match provider {
        "gemini" => "gcp.gemini".into(),
        provider => provider.into(),
    }
}

fn finish_reason(reason: &FinishReason) -> String {
    match reason {
        FinishReason::Stop => "stop".into(),
        FinishReason::Length => "length".into(),
        FinishReason::ToolCalls => "tool_calls".into(),
        FinishReason::ContentFilter => "content_filter".into(),
        FinishReason::Cancelled => "cancelled".into(),
        FinishReason::Error => "error".into(),
        FinishReason::Other(reason) => reason.clone(),
        _ => "unknown".into(),
    }
}

fn error_type(kind: &ModelErrorKind) -> &'static str {
    match kind {
        ModelErrorKind::InvalidRequest => "invalid_request",
        ModelErrorKind::UnsupportedFeature => "unsupported_feature",
        ModelErrorKind::Transport => "transport",
        ModelErrorKind::Protocol => "protocol",
        ModelErrorKind::StreamState => "stream_state",
        ModelErrorKind::MalformedToolArguments => "malformed_tool_arguments",
        ModelErrorKind::Provider => "provider_error",
        ModelErrorKind::AdmissionDenied => "admission_denied",
        ModelErrorKind::Cancelled => "cancelled",
        ModelErrorKind::DeadlineExceeded => "timeout",
        _ => "_OTHER",
    }
}

fn as_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn route_failure_attributes(failure: &serde_json::Value) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    // Provider events may come from a custom Model. Revalidate the bounded
    // projection rather than trusting an arbitrary diagnostics object.
    let mut projected = ModelError::local(ModelErrorKind::Provider, "");
    projected.provider = Some("provider".into());
    for key in [
        "http.status",
        "retry.after_ms",
        "retry.delay_ms",
        "retry.remaining_ms",
        "provider.error.type",
        "provider.error.code",
        "runifold.scheduler.phase",
        "runifold.scheduler.reason",
        "runifold.request.phase",
        "runifold.request.remote_outcome",
        "runifold.request.provider_invoked",
        "runifold.request.local_capacity_released",
        "runifold.request.output_observed",
        "runifold.request.queue_wait_ms",
        "runifold.request.execution_ms",
    ] {
        if let Some(value) = failure.get("diagnostics").and_then(|value| value.get(key))
            && (value.is_boolean()
                || value.is_u64()
                || value.as_str().is_some_and(|value| value.len() <= 64))
        {
            projected.metadata.insert(key.into(), value.clone());
        }
    }
    attributes.extend(diagnostic_attributes(&projected));
    for (source, target) in [
        ("route", "runifold.model.route.name"),
        ("kind", "error.type"),
        ("retry_safety", "runifold.retry.safety"),
    ] {
        if let Some(value) = failure.get(source).and_then(serde_json::Value::as_str) {
            attributes.push(KeyValue::new(target, value.to_owned()));
        }
    }
    if let Some(attempt) = failure
        .get("route_attempt")
        .and_then(serde_json::Value::as_u64)
    {
        attributes.push(KeyValue::new(
            "runifold.model.route.attempt",
            i64::try_from(attempt).unwrap_or(i64::MAX),
        ));
    }
    if let Some(target) = failure.get("target") {
        if let Some(provider) = target.get("provider").and_then(serde_json::Value::as_str) {
            attributes.push(KeyValue::new(
                "runifold.model.route.provider",
                provider.to_owned(),
            ));
        }
        if let Some(model) = target.get("name").and_then(serde_json::Value::as_str) {
            attributes.push(KeyValue::new(
                "runifold.model.route.model",
                model.to_owned(),
            ));
        }
    }
    attributes
}

fn diagnostic_attributes(error: &ModelError) -> Vec<KeyValue> {
    error
        .diagnostic_metadata()
        .into_iter()
        .filter_map(|(key, value)| {
            let key = match key.as_str() {
                "http.status" => "http.response.status_code".into(),
                "retry.after_ms" => "runifold.retry.after_ms".into(),
                "retry.delay_ms" => "runifold.retry.delay_ms".into(),
                "retry.remaining_ms" => "runifold.retry.remaining_ms".into(),
                "provider.error.type" => "runifold.provider.error.type".into(),
                "provider.error.code" => "runifold.provider.error.code".into(),
                _ => key,
            };
            if let Some(value) = value.as_u64() {
                Some(KeyValue::new(key, as_i64(value)))
            } else if let Some(value) = value.as_bool() {
                Some(KeyValue::new(key, value))
            } else {
                value
                    .as_str()
                    .map(|value| KeyValue::new(key, value.to_owned()))
            }
        })
        .collect()
}

const fn is_first_chunk(event: &ModelStreamEvent) -> bool {
    matches!(
        event,
        ModelStreamEvent::TextDelta { .. }
            | ModelStreamEvent::ReasoningDelta { .. }
            | ModelStreamEvent::ToolArgumentsDelta { .. }
            | ModelStreamEvent::RefusalDelta { .. }
            | ModelStreamEvent::ContentPartCompleted { .. }
    )
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use opentelemetry::{
        Value, global,
        metrics::MeterProvider as _,
        trace::{SpanKind, Status},
    };
    use opentelemetry_sdk::{
        metrics::{InMemoryMetricExporter, SdkMeterProvider},
        trace::SpanData,
    };
    use runifold_model::{
        ContentBlockKind, FinishReason, Message, Model, ModelCallContext, ModelError,
        ModelErrorKind, ModelRef, ModelRequest, ModelStreamEvent, ModelUsage, ProviderEvent,
        ToolSpec,
    };
    use runifold_testkit::ScriptedModel;

    use super::OtelModel;
    use crate::{ContentCapture, OtelConfig, test_support::TraceFixture};

    const SECRET: &str = "do-not-export-this-secret";

    #[tokio::test]
    async fn safe_defaults_emit_semantics_without_message_content() {
        let scripted = ScriptedModel::new();
        scripted.enqueue(success_events(SECRET));
        let fixture = TraceFixture::new();
        let meter = global::meter("runifold.test");
        let model = OtelModel::from_parts(
            Arc::new(scripted),
            fixture.tracer,
            &meter,
            OtelConfig::default(),
        );

        model
            .invoke(request(SECRET), ModelCallContext::new())
            .await
            .unwrap();

        let spans = fixture.exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, "chat gpt-test");
        assert_eq!(span.span_kind, SpanKind::Client);
        assert_eq!(
            attribute(span, "gen_ai.operation.name"),
            Some("chat".into())
        );
        assert_eq!(
            attribute(span, "gen_ai.provider.name"),
            Some("openai".into())
        );
        assert_eq!(
            attribute(span, "gen_ai.usage.input_tokens"),
            Some("11".into())
        );
        assert!(attribute(span, "gen_ai.input.messages").is_none());
        assert!(attribute(span, "gen_ai.system_instructions").is_none());
        assert!(attribute(span, "gen_ai.output.messages").is_none());
        assert!(attribute(span, "gen_ai.tool.definitions").is_none());
        assert!(!format!("{spans:?}").contains(SECRET));
    }

    #[tokio::test]
    async fn explicit_capture_includes_messages_output_and_tools() {
        let scripted = ScriptedModel::new();
        scripted.enqueue(success_events("visible-output"));
        let fixture = TraceFixture::new();
        let meter = global::meter("runifold.test");
        let model = OtelModel::from_parts(
            Arc::new(scripted),
            fixture.tracer,
            &meter,
            OtelConfig::default().with_content_capture(ContentCapture::MessagesAndTools),
        );

        model
            .invoke(request(SECRET), ModelCallContext::new())
            .await
            .unwrap();

        let spans = fixture.exporter.get_finished_spans().unwrap();
        let span = &spans[0];
        assert!(
            attribute(span, "gen_ai.input.messages").is_some_and(|value| value.contains(SECRET))
        );
        assert!(attribute(span, "gen_ai.system_instructions").is_some());
        assert!(attribute(span, "gen_ai.output.messages").is_some());
        assert!(attribute(span, "gen_ai.tool.definitions").is_some());
    }

    #[tokio::test]
    async fn errors_are_classified_without_exporting_messages_by_default() {
        let scripted = ScriptedModel::new();
        let mut failure = ModelError::local(ModelErrorKind::DeadlineExceeded, SECRET);
        failure.provider = Some("openai".into());
        failure
            .metadata
            .insert("http.status".into(), serde_json::json!(504));
        failure.metadata.insert(
            "openai.error.type".into(),
            serde_json::json!("server_error"),
        );
        failure
            .metadata
            .insert("openai.error.param".into(), serde_json::json!(SECRET));
        failure.metadata.insert(
            "runifold.request.remote_outcome".into(),
            serde_json::json!("unknown"),
        );
        scripted.enqueue_error(failure);
        let fixture = TraceFixture::new();
        let meter = global::meter("runifold.test");
        let model = OtelModel::from_parts(
            Arc::new(scripted),
            fixture.tracer,
            &meter,
            OtelConfig::default(),
        );

        let error = model
            .invoke(request("input"), ModelCallContext::new())
            .await
            .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::DeadlineExceeded);
        let spans = fixture.exporter.get_finished_spans().unwrap();
        let span = &spans[0];
        assert_eq!(attribute(span, "error.type"), Some("timeout".into()));
        let exception = span
            .events
            .iter()
            .find(|event| event.name == "gen_ai.client.operation.exception")
            .unwrap();
        assert!(
            exception
                .attributes
                .iter()
                .any(|entry| entry.key.as_str() == "http.response.status_code"
                    && entry.value.to_string() == "504")
        );
        assert!(exception.attributes.iter().any(|entry| entry.key.as_str()
            == "runifold.request.remote_outcome"
            && entry.value.to_string() == "unknown"));
        assert!(matches!(span.status, Status::Error { .. }));
        assert!(!format!("{spans:?}").contains(SECRET));
    }

    #[tokio::test]
    async fn completed_calls_record_standard_duration_and_token_metrics() {
        let scripted = ScriptedModel::new();
        scripted.enqueue(success_events("output"));
        let fixture = TraceFixture::new();
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let meter = meter_provider.meter("runifold.test");
        let model = OtelModel::from_parts(
            Arc::new(scripted),
            fixture.tracer,
            &meter,
            OtelConfig::default(),
        );

        model
            .invoke(request("input"), ModelCallContext::new())
            .await
            .unwrap();
        meter_provider.force_flush().unwrap();

        let metrics = exporter.get_finished_metrics().unwrap();
        let names = metrics
            .iter()
            .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .map(opentelemetry_sdk::metrics::data::Metric::name)
            .collect::<Vec<_>>();
        assert!(names.contains(&"gen_ai.client.operation.duration"));
        assert!(names.contains(&"gen_ai.client.token.usage"));
        assert!(names.contains(&"runifold.model.time_to_first_chunk"));
    }

    #[tokio::test]
    async fn terminal_router_failure_projects_every_attempt_once() {
        let scripted = ScriptedModel::new();
        for (status, safety) in [
            (503, runifold_core::RetrySafety::Safe),
            (502, runifold_core::RetrySafety::Unknown),
        ] {
            let mut error = ModelError::local(ModelErrorKind::Provider, SECRET);
            error.retry_safety = safety;
            error
                .metadata
                .insert("http.status".into(), serde_json::json!(status));
            scripted.enqueue_error(error);
        }
        let logical = ModelRef::new("openai", "gpt-test");
        let router = runifold_model::ModelRouter::builder(logical.clone())
            .route("primary", Arc::new(scripted.clone()), logical.clone())
            .route("backup", Arc::new(scripted), logical)
            .build()
            .unwrap();
        let fixture = TraceFixture::new();
        let meter = global::meter("runifold.test");
        let model = OtelModel::from_parts(
            Arc::new(router),
            fixture.tracer,
            &meter,
            OtelConfig::default(),
        );
        let error = model
            .invoke(request("input"), ModelCallContext::new())
            .await
            .unwrap_err();
        assert_eq!(error.retry_safety, runifold_core::RetrySafety::Unknown);
        let spans = fixture.exporter.get_finished_spans().unwrap();
        let failures: Vec<_> = spans[0]
            .events
            .iter()
            .filter(|event| event.name == "runifold.model.route.failure")
            .collect();
        assert_eq!(failures.len(), 2);
        for (failure, status) in failures.iter().zip(["503", "502"]) {
            assert!(
                failure
                    .attributes
                    .iter()
                    .any(|entry| entry.key.as_str() == "http.response.status_code"
                        && entry.value.to_string() == status)
            );
        }
        assert!(!format!("{spans:?}").contains(SECRET));
    }

    #[tokio::test]
    async fn already_reported_route_failures_are_not_exported_twice() {
        let fixture = TraceFixture::new();
        let meter = global::meter("runifold.test");
        let mut call = super::ActiveCall::start(
            &fixture.tracer,
            super::Instruments::new(&meter),
            &request("input"),
            &ModelCallContext::new(),
            OtelConfig::default(),
            None,
        );
        let first = serde_json::json!({"route": "primary", "kind": "Transport", "diagnostics": {"http.status":503}});
        call.observe_route_selection(
            &serde_json::json!({"route":"backup", "prior_failures":[first.clone()]}),
        );
        let mut error = ModelError::local(ModelErrorKind::Provider, SECRET);
        error.metadata.insert("runifold.router.failures".into(), serde_json::json!([first, {"route":"backup", "kind":"Provider", "diagnostics":{"http.status":502}}]));
        call.fail(&error);
        let spans = fixture.exporter.get_finished_spans().unwrap();
        assert_eq!(
            spans[0]
                .events
                .iter()
                .filter(|event| event.name == "runifold.model.route.failure")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn router_selection_projects_attempts_without_sensitive_errors() {
        let scripted = ScriptedModel::new();
        let mut events = success_events("output");
        events.insert(
            1,
            ModelStreamEvent::Provider {
                event: ProviderEvent {
                    provider: "runifold.router".into(),
                    name: "route.selected".into(),
                    payload: serde_json::json!({
                        "route": "backup",
                        "route_attempt": 1,
                        "attempt_id": "attempt-2",
                        "circuit_probe": false,
                        "prior_failures": [{
                            "route": "primary",
                            "target": {"provider": "openai", "name": "gpt-a"},
                            "route_attempt": 1,
                            "kind": "transport",
                            "retry_safety": "safe",
                            "diagnostics": {
                                "http.status": 503,
                                "provider.error.type": "server_error",
                                "provider.error.code": "do-not-export-this-secret",
                                "private": "do-not-export-this-secret"
                            }
                        }]
                    }),
                },
            },
        );
        scripted.enqueue(events);
        let fixture = TraceFixture::new();
        let exporter = InMemoryMetricExporter::default();
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let meter = meter_provider.meter("runifold.test");
        let model = OtelModel::from_parts(
            Arc::new(scripted),
            fixture.tracer,
            &meter,
            OtelConfig::default(),
        );

        model
            .invoke(request("input"), ModelCallContext::new())
            .await
            .unwrap();
        meter_provider.force_flush().unwrap();

        let spans = fixture.exporter.get_finished_spans().unwrap();
        let span = &spans[0];
        assert_eq!(
            attribute(span, "runifold.model.route.name"),
            Some("backup".into())
        );
        assert_eq!(
            attribute(span, "runifold.model.route.fallback"),
            Some("true".into())
        );
        assert_eq!(
            attribute(span, "runifold.model.route.prior_failures"),
            Some("1".into())
        );
        let event_names = span
            .events
            .iter()
            .map(|event| event.name.as_ref())
            .collect::<Vec<_>>();
        assert!(event_names.contains(&"runifold.model.route.failure"));
        assert!(event_names.contains(&"runifold.model.route.selected"));
        let failure = span
            .events
            .iter()
            .find(|event| event.name == "runifold.model.route.failure")
            .unwrap();
        assert!(
            failure
                .attributes
                .iter()
                .any(|entry| entry.key.as_str() == "http.response.status_code"
                    && entry.value.to_string() == "503")
        );
        assert!(
            failure
                .attributes
                .iter()
                .any(|entry| entry.key.as_str() == "runifold.provider.error.type"
                    && entry.value.to_string() == "server_error")
        );
        assert!(!format!("{spans:?}").contains(SECRET));
        let metrics = exporter.get_finished_metrics().unwrap();
        let metric_names = metrics
            .iter()
            .flat_map(opentelemetry_sdk::metrics::data::ResourceMetrics::scope_metrics)
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            .map(opentelemetry_sdk::metrics::data::Metric::name)
            .collect::<Vec<_>>();
        assert!(metric_names.contains(&"runifold.model.route.failures"));
        assert!(metric_names.contains(&"runifold.model.route.selections"));
    }

    fn request(secret: &str) -> ModelRequest {
        ModelRequest::new(ModelRef::new("openai", "gpt-test"), Message::user(secret))
            .message(Message::system("system-secret"))
            .tool(ToolSpec {
                name: "lookup".into(),
                description: "secret tool description".into(),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: None,
                metadata: BTreeMap::new(),
            })
    }

    fn success_events(text: &str) -> Vec<ModelStreamEvent> {
        vec![
            ModelStreamEvent::ResponseStarted {
                id: Some("response-1".into()),
                model: ModelRef::new("openai", "gpt-test"),
            },
            ModelStreamEvent::ContentBlockStarted {
                index: 0,
                kind: ContentBlockKind::Text,
            },
            ModelStreamEvent::TextDelta {
                index: 0,
                text: text.into(),
            },
            ModelStreamEvent::ContentBlockCompleted { index: 0 },
            ModelStreamEvent::UsageUpdated {
                usage: ModelUsage {
                    input_tokens: 11,
                    output_tokens: 7,
                    reasoning_tokens: 2,
                    cached_input_tokens: 3,
                    cache_write_tokens: 1,
                    cost_microusd: 0,
                },
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: BTreeMap::new(),
            },
        ]
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
