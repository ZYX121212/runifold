use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ModelError, ModelErrorKind, ModelRef};

/// Clock used by model-routing resilience policy.
///
/// Applications may inject a deterministic implementation for tests.
pub trait RouterClock: Send + Sync {
    /// Returns the current monotonic time.
    fn now(&self) -> Instant;
}

/// Monotonic system clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemRouterClock;

impl RouterClock for SystemRouterClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Invalid circuit-breaker configuration.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum CircuitBreakerConfigError {
    /// A circuit could never open.
    #[error("circuit-breaker failure threshold must be greater than zero")]
    ZeroFailureThreshold,
    /// An open circuit would immediately expire.
    #[error("circuit-breaker cooldown must be greater than zero")]
    ZeroCooldown,
}

/// Per-route circuit-breaker policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CircuitBreakerConfig {
    failure_threshold: u32,
    cooldown: Duration,
    counted_kinds: Vec<ModelErrorKind>,
}

impl CircuitBreakerConfig {
    /// Creates a breaker that counts transport, provider, protocol, and stream
    /// state failures.
    ///
    /// # Errors
    ///
    /// Returns [`CircuitBreakerConfigError`] when the threshold or cooldown is
    /// zero.
    pub fn new(
        failure_threshold: u32,
        cooldown: Duration,
    ) -> Result<Self, CircuitBreakerConfigError> {
        if failure_threshold == 0 {
            return Err(CircuitBreakerConfigError::ZeroFailureThreshold);
        }
        if cooldown.is_zero() {
            return Err(CircuitBreakerConfigError::ZeroCooldown);
        }
        Ok(Self {
            failure_threshold,
            cooldown,
            counted_kinds: vec![
                ModelErrorKind::Transport,
                ModelErrorKind::Provider,
                ModelErrorKind::Protocol,
                ModelErrorKind::StreamState,
            ],
        })
    }

    /// Replaces the failure kinds counted by this breaker.
    #[must_use]
    pub fn counted_kinds(mut self, kinds: impl IntoIterator<Item = ModelErrorKind>) -> Self {
        self.counted_kinds.clear();
        for kind in kinds {
            if !self.counted_kinds.contains(&kind) {
                self.counted_kinds.push(kind);
            }
        }
        self
    }

    /// Returns the consecutive counted-failure threshold.
    pub const fn failure_threshold(&self) -> u32 {
        self.failure_threshold
    }

    /// Returns how long an opened route remains unavailable before probing.
    pub const fn cooldown(&self) -> Duration {
        self.cooldown
    }

    /// Returns error kinds that contribute to opening the circuit.
    pub fn failure_kinds(&self) -> &[ModelErrorKind] {
        &self.counted_kinds
    }

    pub(crate) fn counts(&self, error: &ModelError) -> bool {
        error.kind != ModelErrorKind::Cancelled && self.counted_kinds.contains(&error.kind)
    }
}

/// Public route-health state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CircuitState {
    /// Requests may use the route.
    Closed,
    /// Requests skip the route during its cooldown.
    Open,
    /// Exactly one recovery probe is currently using the route.
    HalfOpen,
}

/// Point-in-time health for one physical model route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelRouteHealth {
    /// Stable route name.
    pub route: String,
    /// Physical model target.
    pub target: ModelRef,
    /// Current circuit state.
    pub state: CircuitState,
    /// Consecutive counted failures in the current generation.
    pub consecutive_failures: u32,
    /// Remaining cooldown for an open route.
    pub retry_after: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct BreakerState {
    generation: u64,
    phase: BreakerPhase,
}

#[derive(Debug)]
enum BreakerPhase {
    Closed { failures: u32 },
    Open { until: Option<Instant> },
    HalfOpen,
}

impl Default for BreakerState {
    fn default() -> Self {
        Self {
            generation: 0,
            phase: BreakerPhase::Closed { failures: 0 },
        }
    }
}

pub(crate) type SharedBreakerState = Arc<Mutex<BreakerState>>;

pub(crate) enum RoutePermit {
    Disabled,
    Acquired(BreakerPermit),
    Rejected,
}

pub(crate) struct BreakerPermit {
    state: SharedBreakerState,
    config: CircuitBreakerConfig,
    clock: Arc<dyn RouterClock>,
    generation: u64,
    probe: bool,
    resolved: bool,
}

impl BreakerPermit {
    pub(crate) const fn is_probe(&self) -> bool {
        self.probe
    }

    pub(crate) fn success(mut self) {
        let mut state = lock(&self.state);
        if state.generation == self.generation {
            state.generation = state.generation.wrapping_add(1);
            state.phase = BreakerPhase::Closed { failures: 0 };
        }
        self.resolved = true;
    }

    pub(crate) fn failure(mut self, error: &ModelError) {
        if self.config.counts(error) {
            record_counted_failure(
                &self.state,
                &self.config,
                self.clock.now(),
                self.generation,
                self.probe,
            );
        } else if self.probe {
            reopen(
                &self.state,
                self.clock.now(),
                self.config.cooldown,
                self.generation,
            );
        }
        self.resolved = true;
    }
}

impl Drop for BreakerPermit {
    fn drop(&mut self) {
        if self.probe && !self.resolved {
            reopen(
                &self.state,
                self.clock.now(),
                self.config.cooldown,
                self.generation,
            );
        }
    }
}

pub(crate) fn acquire(
    state: &SharedBreakerState,
    config: Option<&CircuitBreakerConfig>,
    clock: &Arc<dyn RouterClock>,
) -> RoutePermit {
    let Some(config) = config else {
        return RoutePermit::Disabled;
    };
    let now = clock.now();
    let mut state_guard = lock(state);
    let generation = state_guard.generation;
    let probe = match state_guard.phase {
        BreakerPhase::Closed { .. } => false,
        BreakerPhase::Open { until: Some(until) } if now >= until => {
            state_guard.phase = BreakerPhase::HalfOpen;
            true
        }
        BreakerPhase::Open { .. } | BreakerPhase::HalfOpen => return RoutePermit::Rejected,
    };
    drop(state_guard);
    RoutePermit::Acquired(BreakerPermit {
        state: state.clone(),
        config: config.clone(),
        clock: clock.clone(),
        generation,
        probe,
        resolved: false,
    })
}

pub(crate) fn snapshot(
    state: &SharedBreakerState,
    route: String,
    target: ModelRef,
    config: Option<&CircuitBreakerConfig>,
    now: Instant,
) -> ModelRouteHealth {
    let state = lock(state);
    let (health, failures, retry_after) = match state.phase {
        BreakerPhase::Closed { failures } => (CircuitState::Closed, failures, None),
        BreakerPhase::Open { until } => (
            CircuitState::Open,
            0,
            until.map(|until| until.saturating_duration_since(now)),
        ),
        BreakerPhase::HalfOpen => (CircuitState::HalfOpen, 0, None),
    };
    if config.is_none() {
        return ModelRouteHealth {
            route,
            target,
            state: CircuitState::Closed,
            consecutive_failures: 0,
            retry_after: None,
        };
    }
    ModelRouteHealth {
        route,
        target,
        state: health,
        consecutive_failures: failures,
        retry_after,
    }
}

fn record_counted_failure(
    state: &SharedBreakerState,
    config: &CircuitBreakerConfig,
    now: Instant,
    generation: u64,
    probe: bool,
) {
    let mut state = lock(state);
    if state.generation != generation {
        return;
    }
    if probe {
        state.generation = state.generation.wrapping_add(1);
        state.phase = BreakerPhase::Open {
            until: now.checked_add(config.cooldown),
        };
        return;
    }
    let BreakerPhase::Closed { failures } = &mut state.phase else {
        return;
    };
    *failures = failures.saturating_add(1);
    if *failures >= config.failure_threshold {
        state.generation = state.generation.wrapping_add(1);
        state.phase = BreakerPhase::Open {
            until: now.checked_add(config.cooldown),
        };
    }
}

fn reopen(state: &SharedBreakerState, now: Instant, cooldown: Duration, generation: u64) {
    let mut state = lock(state);
    if state.generation == generation {
        state.generation = state.generation.wrapping_add(1);
        state.phase = BreakerPhase::Open {
            until: now.checked_add(cooldown),
        };
    }
}

fn lock(state: &SharedBreakerState) -> std::sync::MutexGuard<'_, BreakerState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use runifold_core::RetrySafety;

    use crate::{ModelError, ModelErrorKind, ModelRef};

    use super::{
        BreakerState, CircuitBreakerConfig, CircuitState, RoutePermit, RouterClock,
        SharedBreakerState, acquire, snapshot,
    };

    struct ManualClock {
        now: Mutex<Instant>,
    }

    impl ManualClock {
        fn new() -> Self {
            Self {
                now: Mutex::new(Instant::now()),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self
                .now
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *now += duration;
        }
    }

    impl RouterClock for ManualClock {
        fn now(&self) -> Instant {
            *self
                .now
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    fn state() -> SharedBreakerState {
        Arc::new(Mutex::new(BreakerState::default()))
    }

    fn config(threshold: u32) -> CircuitBreakerConfig {
        CircuitBreakerConfig::new(threshold, Duration::from_secs(10)).unwrap()
    }

    fn failure() -> ModelError {
        let mut error = ModelError::local(ModelErrorKind::Transport, "failure");
        error.retry_safety = RetrySafety::Safe;
        error
    }

    fn permit(
        state: &SharedBreakerState,
        config: &CircuitBreakerConfig,
        clock: &Arc<dyn RouterClock>,
    ) -> super::BreakerPermit {
        match acquire(state, Some(config), clock) {
            RoutePermit::Acquired(permit) => permit,
            RoutePermit::Disabled | RoutePermit::Rejected => panic!("expected route permit"),
        }
    }

    fn health(
        state: &SharedBreakerState,
        config: &CircuitBreakerConfig,
        clock: &Arc<dyn RouterClock>,
    ) -> super::ModelRouteHealth {
        snapshot(
            state,
            "route".into(),
            ModelRef::new("test", "model"),
            Some(config),
            clock.now(),
        )
    }

    #[test]
    fn threshold_opens_then_one_successful_probe_closes() {
        let state = state();
        let clock_impl = Arc::new(ManualClock::new());
        let clock: Arc<dyn RouterClock> = clock_impl.clone();
        let config = config(2);

        permit(&state, &config, &clock).failure(&failure());
        assert_eq!(health(&state, &config, &clock).consecutive_failures, 1);
        permit(&state, &config, &clock).failure(&failure());
        assert_eq!(health(&state, &config, &clock).state, CircuitState::Open);
        assert!(matches!(
            acquire(&state, Some(&config), &clock),
            RoutePermit::Rejected
        ));

        clock_impl.advance(config.cooldown());
        let probe = permit(&state, &config, &clock);
        assert_eq!(
            health(&state, &config, &clock).state,
            CircuitState::HalfOpen
        );
        assert!(matches!(
            acquire(&state, Some(&config), &clock),
            RoutePermit::Rejected
        ));
        probe.success();

        let health = health(&state, &config, &clock);
        assert_eq!(health.state, CircuitState::Closed);
        assert_eq!(health.consecutive_failures, 0);
    }

    #[test]
    fn stale_failure_cannot_overwrite_a_newer_success_generation() {
        let state = state();
        let clock_impl = Arc::new(ManualClock::new());
        let clock: Arc<dyn RouterClock> = clock_impl.clone();
        let config = config(1);
        let delayed_permit = permit(&state, &config, &clock);
        let opener = permit(&state, &config, &clock);

        opener.failure(&failure());
        clock_impl.advance(config.cooldown());
        permit(&state, &config, &clock).success();
        delayed_permit.failure(&failure());

        assert_eq!(health(&state, &config, &clock).state, CircuitState::Closed);
    }

    #[test]
    fn abandoned_half_open_probe_reopens_the_route() {
        let state = state();
        let clock_impl = Arc::new(ManualClock::new());
        let clock: Arc<dyn RouterClock> = clock_impl.clone();
        let config = config(1);

        permit(&state, &config, &clock).failure(&failure());
        clock_impl.advance(config.cooldown());
        let probe = permit(&state, &config, &clock);
        drop(probe);

        let health = health(&state, &config, &clock);
        assert_eq!(health.state, CircuitState::Open);
        assert_eq!(health.retry_after, Some(config.cooldown()));
    }

    #[test]
    fn non_counted_failure_does_not_damage_a_closed_route() {
        let state = state();
        let clock: Arc<dyn RouterClock> = Arc::new(ManualClock::new());
        let config = config(1);
        let error = ModelError::local(ModelErrorKind::InvalidRequest, "caller error");

        permit(&state, &config, &clock).failure(&error);

        let health = health(&state, &config, &clock);
        assert_eq!(health.state, CircuitState::Closed);
        assert_eq!(health.consecutive_failures, 0);
    }
}
