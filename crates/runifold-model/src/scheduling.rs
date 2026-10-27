//! Shared, process-local admission for physical model requests.
//!
//! Wrap physical providers below routing/retry. A shared scheduler coordinates
//! all bound pools atomically; independent schedulers do not share quotas.

use std::{
    collections::BTreeMap,
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Duration,
};

use thiserror::Error;

use crate::{
    Model, ModelError, ModelErrorKind, ModelRequest, RouterClock, RouterSleeper, SystemRouterClock,
    SystemRouterSleeper,
};

mod model;
mod state;
#[cfg(test)]
mod tests;

pub use model::ScheduledModel;
use state::{Pool, State};

/// Explicit account, deployment, or model quota identity; never a credential.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ModelCapacityPoolId(String);

impl ModelCapacityPoolId {
    /// Parses a bounded identity without storing credentials or arbitrary payloads.
    ///
    /// # Errors
    ///
    /// Returns `InvalidPoolId` for empty, oversized, or unsafe identities.
    pub fn new(value: impl Into<String>) -> Result<Self, ModelSchedulerConfigError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b':'))
        {
            return Err(ModelSchedulerConfigError::InvalidPoolId);
        }
        Ok(Self(value))
    }

    /// Returns the configured identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A local rolling-window limit, not a provider-specific billing algorithm.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelRateLimit {
    limit: NonZeroU64,
    window: Duration,
}

impl ModelRateLimit {
    /// Creates a positive allowance with a positive, representable window.
    ///
    /// # Errors
    ///
    /// Returns `InvalidWindow` for zero or unrepresentable windows.
    pub fn new(limit: NonZeroU64, window: Duration) -> Result<Self, ModelSchedulerConfigError> {
        if window.is_zero() || runifold_core::Instant::now().checked_add(window).is_none() {
            return Err(ModelSchedulerConfigError::InvalidWindow);
        }
        Ok(Self { limit, window })
    }

    /// Returns the allowance per rolling window.
    pub const fn limit(&self) -> NonZeroU64 {
        self.limit
    }
    /// Returns the rolling window duration.
    pub const fn window(&self) -> Duration {
        self.window
    }
}

/// Hard local concurrency and optional request/token window limits for one pool.
#[derive(Clone, Debug)]
pub struct ModelCapacityLimits {
    concurrency: NonZeroUsize,
    requests: Option<ModelRateLimit>,
    tokens: Option<ModelRateLimit>,
}

impl ModelCapacityLimits {
    /// Creates a pool with a positive local in-flight attempt limit.
    pub fn new(concurrency: NonZeroUsize) -> Self {
        Self {
            concurrency,
            requests: None,
            tokens: None,
        }
    }
    /// Sets the request allowance, charged when an attempt is admitted.
    #[must_use]
    pub fn requests(mut self, limit: ModelRateLimit) -> Self {
        self.requests = Some(limit);
        self
    }
    /// Sets the token allowance, requiring an explicit token estimator at invocation.
    ///
    /// Active reservations never expire. Settlements are retained for a full
    /// window from release. Unknown usage retains at least the reservation.
    #[must_use]
    pub fn tokens(mut self, limit: ModelRateLimit) -> Self {
        self.tokens = Some(limit);
        self
    }
}

/// Immutable limits and bounded queue policy for one shared scheduler.
#[derive(Clone, Debug)]
pub struct ModelSchedulerConfig {
    max_queued: NonZeroUsize,
    starvation_after: Duration,
    pools: BTreeMap<ModelCapacityPoolId, ModelCapacityLimits>,
}

impl ModelSchedulerConfig {
    /// Creates a bounded queue with a default 30-second starvation barrier.
    pub fn new(max_queued: NonZeroUsize) -> Self {
        Self {
            max_queued,
            starvation_after: Duration::from_secs(30),
            pools: BTreeMap::new(),
        }
    }
    /// Adds a uniquely named pool; duplicate configuration is rejected.
    ///
    /// # Errors
    ///
    /// Returns `DuplicatePool` when the identity already exists.
    pub fn pool(
        mut self,
        id: ModelCapacityPoolId,
        limits: ModelCapacityLimits,
    ) -> Result<Self, ModelSchedulerConfigError> {
        if self.pools.contains_key(&id) {
            return Err(ModelSchedulerConfigError::DuplicatePool);
        }
        self.pools.insert(id, limits);
        Ok(self)
    }
    /// Bounds how long younger requests can bypass a blocked older request.
    ///
    /// Once aged, an older waiter reserves scheduling precedence over younger
    /// waiters touching any of its pools. Unrelated pools continue. This may
    /// intentionally idle capacity to accumulate allowance for large requests.
    #[must_use]
    pub fn starvation_after(mut self, duration: Duration) -> Self {
        self.starvation_after = duration;
        self
    }
}

/// Configuration failures without provider work or private input data.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum ModelSchedulerConfigError {
    /// Invalid pool identity.
    #[error("pool identity must contain 1..=128 safe ASCII characters")]
    InvalidPoolId,
    /// Invalid rolling window.
    #[error("rate window must be positive and representable")]
    InvalidWindow,
    /// No configured pools.
    #[error("configure at least one capacity pool")]
    NoPools,
    /// Repeated pool definition or binding.
    #[error("each pool must be configured and bound exactly once")]
    DuplicatePool,
    /// Route references an unknown pool.
    #[error("configure every pool before binding a model")]
    UnknownPool,
    /// Route has no applicable limits.
    #[error("bind at least one capacity pool")]
    EmptyBinding,
}

/// Admission failures before physical dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum ModelSchedulingError {
    /// The finite waiting queue is full.
    #[error("model admission queue is full; reduce producer concurrency or increase its bound")]
    QueueFull,
    /// Token-limited invocation has no positive reservation.
    #[error("token-limited model calls require an explicit positive token reservation")]
    MissingTokenReservation,
    /// A request can never fit the configured token pool.
    #[error(
        "token reservation exceeds a pool allowance; reduce request size or increase the allowance"
    )]
    ReservationTooLarge,
    /// Scheduler wrappers were nested on one physical invocation path.
    #[error(
        "bind all applicable pools once per physical model path; nested admission would deadlock"
    )]
    NestedAdmission,
    /// No more unique admission identities can be allocated.
    #[error(
        "scheduler admission identity space exhausted; replace the scheduler after draining it"
    )]
    IdentityExhausted,
}

impl From<ModelSchedulingError> for ModelError {
    fn from(value: ModelSchedulingError) -> Self {
        let mut error = Self::local(ModelErrorKind::AdmissionDenied, value.to_string());
        error.retry_safety = runifold_core::RetrySafety::Safe;
        let reason = match value {
            ModelSchedulingError::QueueFull => "queue_full",
            ModelSchedulingError::MissingTokenReservation => "missing_token_reservation",
            ModelSchedulingError::ReservationTooLarge => "reservation_too_large",
            ModelSchedulingError::IdentityExhausted => "identity_exhausted",
            ModelSchedulingError::NestedAdmission => "nested_admission",
        };
        error
            .metadata
            .insert("runifold.scheduler.reason".into(), reason.into());
        error
    }
}

/// Explicit trust in whether a completed stream reports complete billable usage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelTokenSettlement {
    /// Retain at least the reservation, even if a terminal stream reported less.
    ///
    /// A cumulative usage event may contain only input or intermediate usage.
    /// This safe default never treats a partial snapshot as a refund authority.
    #[default]
    Conservative,
    /// On terminal completion, settle to the largest reported cumulative usage.
    ///
    /// Select only after verifying that the adapter always reports complete
    /// billable input/output usage on successful completion. Missing usage,
    /// failures, cancellation and drop still retain at least the reservation.
    ReportedUsage,
}

/// Trusted application policy for reserving billable input plus bounded output.
///
/// Include multimodal input, Tools, and provider reasoning as appropriate. An
/// estimate is not a hard upper bound: overruns are recorded as debt, not hidden.
/// Implementations must be fast and must not expose request data in errors.
pub trait ModelTokenEstimator: Send + Sync {
    /// Estimates one physical attempt using its final request.
    ///
    /// # Errors
    ///
    /// Return a safe local error when a reliable reservation cannot be computed.
    fn estimate(&self, request: &ModelRequest) -> Result<u64, ModelError>;
}

/// Payload-free accounting for one capacity pool at snapshot time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelCapacitySnapshot {
    /// Configured identity.
    pub pool: ModelCapacityPoolId,
    /// Currently admitted local attempts.
    pub active: usize,
    /// Unexpired requests charged at admission.
    pub requests: u64,
    /// Tokens reserved by active attempts, including long-running streams.
    pub reserved_tokens: u128,
    /// Unexpired token settlements, including unknown usage reservations.
    pub settled_tokens: u128,
}

/// Consistent, payload-free local scheduler accounting.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelSchedulerSnapshot {
    /// Requests waiting for atomic admission.
    pub queued: usize,
    /// Physical attempts admitted across all pools (not summed per pool).
    pub admitted: u64,
    /// Admission failures.
    pub rejected: u64,
    /// Queue removals due to cancellation.
    pub cancelled: u64,
    /// Queue removals due to deadline expiry.
    pub expired: u64,
    /// Total time admitted requests spent waiting, in microseconds.
    pub queue_wait_micros: u128,
    /// Released attempts without an authoritative terminal usage snapshot.
    pub unknown_usage: u64,
    /// Attempts whose observed usage exceeded their reservation.
    pub token_overruns: u64,
    /// Pool accounting.
    pub pools: Vec<ModelCapacitySnapshot>,
}

/// Shared process-local physical-attempt admission; clones share all pool state.
///
/// Does not coordinate other processes or confirm remote shutdown after a
/// disconnect. Bind physical providers beneath routing/retry so every retry
/// reacquires capacity and backoff retains no concurrency permit.
#[derive(Clone)]
pub struct ModelScheduler {
    shared: Arc<Shared>,
}

struct Shared {
    state: Mutex<State>,
    clock: Arc<dyn RouterClock>,
    sleeper: Arc<dyn RouterSleeper>,
    max_queued: usize,
    starvation_after: Duration,
}

impl ModelScheduler {
    /// Constructs shared admission with runtime-neutral production time.
    ///
    /// # Errors
    ///
    /// Returns `NoPools` when no capacity pool was configured.
    pub fn new(config: ModelSchedulerConfig) -> Result<Self, ModelSchedulerConfigError> {
        Self::with_time(
            config,
            Arc::new(SystemRouterClock),
            Arc::new(SystemRouterSleeper),
        )
    }
    /// Constructs admission with a matched monotonic clock/timer pair.
    ///
    /// Inject both together for deterministic tests. Context deadlines must use
    /// the same monotonic time domain. Neither callback runs under the state lock.
    ///
    /// # Errors
    ///
    /// Returns `NoPools` when no capacity pool was configured.
    pub fn with_time(
        config: ModelSchedulerConfig,
        clock: Arc<dyn RouterClock>,
        sleeper: Arc<dyn RouterSleeper>,
    ) -> Result<Self, ModelSchedulerConfigError> {
        if config.pools.is_empty() {
            return Err(ModelSchedulerConfigError::NoPools);
        }
        Ok(Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State::new(
                    config
                        .pools
                        .into_iter()
                        .map(|(id, limits)| (id, Pool::new(limits)))
                        .collect(),
                )),
                clock,
                sleeper,
                max_queued: config.max_queued.get(),
                starvation_after: config.starvation_after,
            }),
        })
    }
    /// Wraps a physical Model with explicit applicable pools.
    ///
    /// The wrapper preserves `ProviderModel` identity/profile when available, so
    /// `scheduler.bind(provider, pools)?.runtime(model)` uses the preferred facade
    /// path. Token-limited bindings require `ScheduledModel::token_estimator`.
    ///
    /// # Errors
    ///
    /// Returns `EmptyBinding`, `DuplicatePool`, or `UnknownPool` for invalid bindings.
    pub fn bind<M: Model>(
        &self,
        model: M,
        pools: impl IntoIterator<Item = ModelCapacityPoolId>,
    ) -> Result<ScheduledModel<M>, ModelSchedulerConfigError> {
        let mut pools: Vec<_> = pools.into_iter().collect();
        if pools.is_empty() {
            return Err(ModelSchedulerConfigError::EmptyBinding);
        }
        pools.sort();
        if pools.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ModelSchedulerConfigError::DuplicatePool);
        }
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pools.iter().any(|id| !state.pools.contains_key(id)) {
            return Err(ModelSchedulerConfigError::UnknownPool);
        }
        Ok(ScheduledModel::new(model, self.clone(), pools))
    }
    fn reject(&self, error: ModelSchedulingError) -> ModelError {
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.metrics.rejected = state.metrics.rejected.saturating_add(1);
        error.into()
    }

    /// Reads one consistent accounting snapshot and expires elapsed rate charges.
    pub fn snapshot(&self) -> ModelSchedulerSnapshot {
        let now = self.shared.clock.now();
        let mut state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.refresh(now);
        let mut snapshot = state.metrics.clone();
        snapshot.queued = state.waiters.len();
        snapshot.pools = state
            .pools
            .iter()
            .map(|(id, pool)| ModelCapacitySnapshot {
                pool: id.clone(),
                active: pool.active,
                requests: pool.requests.len() as u64,
                reserved_tokens: pool.reserved,
                settled_tokens: pool.settled,
            })
            .collect();
        snapshot
    }
}

impl std::fmt::Debug for ModelScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelScheduler")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}
