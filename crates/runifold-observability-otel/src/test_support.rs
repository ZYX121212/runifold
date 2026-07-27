use opentelemetry::{global::BoxedTracer, trace::TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

pub(crate) struct TraceFixture {
    pub(crate) exporter: InMemorySpanExporter,
    _provider: SdkTracerProvider,
    pub(crate) tracer: BoxedTracer,
}

impl TraceFixture {
    pub(crate) fn new() -> Self {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = BoxedTracer::new(Box::new(provider.tracer("runifold.test")));
        Self {
            exporter,
            _provider: provider,
            tracer,
        }
    }
}

impl Default for TraceFixture {
    fn default() -> Self {
        Self::new()
    }
}
