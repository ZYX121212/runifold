//! OpenTelemetry `GenAI` instrumentation for Runifold.

mod config;
mod correlation;
mod journal;
mod journal_metrics;
mod model;
mod runtime;
pub mod slo;
#[cfg(test)]
mod test_support;

pub use config::{ContentCapture, OtelConfig};
pub(crate) use correlation::CorrelationRegistry;
pub use journal::OtelJournal;
pub use model::OtelModel;
pub use runtime::OtelRuntime;
