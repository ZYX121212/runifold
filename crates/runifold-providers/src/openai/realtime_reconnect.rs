//! Safety-first automatic recovery for `OpenAI` Realtime sessions.

use std::{future::Future, sync::Arc, time::Duration};

use futures_util::future::{Either, select};
use runifold_model::{ModelCallContext, RetryJitter, RouterSleeper, SystemRouterSleeper};
use thiserror::Error;

use super::{OpenAiRealtimeError, RealtimeReconnectDisposition};

/// Invalid automatic Realtime reconnect policy.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeReconnectPolicyError {
    /// At least one replacement attempt is required.
    #[error("Realtime reconnect max_attempts must be greater than zero")]
    ZeroMaxAttempts,
    /// Exponential growth cannot use a zero multiplier.
    #[error("Realtime reconnect backoff multiplier must be greater than zero")]
    ZeroMultiplier,
    /// The delay cap cannot be below the first delay.
    #[error("Realtime reconnect max_backoff cannot be less than initial_backoff")]
    InvalidBackoffRange,
}

/// Bounded exponential-backoff policy for replacement Realtime sessions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiRealtimeReconnectPolicy {
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    multiplier: u32,
    jitter: RetryJitter,
}

impl Default for OpenAiRealtimeReconnectPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(8),
            multiplier: 2,
            jitter: RetryJitter::Full,
        }
    }
}

impl OpenAiRealtimeReconnectPolicy {
    /// Creates a validated exponential replacement policy.
    ///
    /// `max_attempts` counts replacement connections, not the connection that
    /// was already lost.
    ///
    /// # Errors
    ///
    /// Rejects a zero attempt count, zero multiplier, or inverted delay range.
    pub fn exponential(
        max_attempts: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
        multiplier: u32,
    ) -> Result<Self, OpenAiRealtimeReconnectPolicyError> {
        if max_attempts == 0 {
            return Err(OpenAiRealtimeReconnectPolicyError::ZeroMaxAttempts);
        }
        if multiplier == 0 {
            return Err(OpenAiRealtimeReconnectPolicyError::ZeroMultiplier);
        }
        if max_backoff < initial_backoff {
            return Err(OpenAiRealtimeReconnectPolicyError::InvalidBackoffRange);
        }
        Ok(Self {
            max_attempts,
            initial_backoff,
            max_backoff,
            multiplier,
            jitter: RetryJitter::Full,
        })
    }

    /// Selects whether each exponential delay receives full jitter.
    #[must_use]
    pub const fn jitter(mut self, jitter: RetryJitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Returns the maximum number of replacement attempts.
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns the first replacement delay.
    pub const fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    /// Returns the replacement delay cap.
    pub const fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// Returns the exponential multiplier.
    pub const fn multiplier(&self) -> u32 {
        self.multiplier
    }

    /// Returns the configured jitter mode.
    pub const fn jitter_mode(&self) -> RetryJitter {
        self.jitter
    }

    fn delay(&self, attempt: u32, entropy: u64) -> Duration {
        let exponent = attempt.saturating_sub(1);
        let mut delay = self.initial_backoff;
        for _ in 0..exponent {
            if delay >= self.max_backoff {
                break;
            }
            delay = delay
                .checked_mul(self.multiplier)
                .unwrap_or(self.max_backoff)
                .min(self.max_backoff);
        }
        match self.jitter {
            RetryJitter::None => delay,
            _ => full_jitter(delay, entropy),
        }
    }
}

/// One replacement connection request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenAiRealtimeReconnectAttempt {
    ordinal: u32,
}

impl OpenAiRealtimeReconnectAttempt {
    /// Returns the one-based replacement attempt number.
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }

    /// Every replacement WebRTC connection requires fresh negotiation and a
    /// newly acquired ephemeral credential.
    pub const fn requires_fresh_credential(self) -> bool {
        true
    }
}

/// Redacted failure category emitted to reconnect observers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeReconnectFailureKind {
    /// The underlying network transport failed.
    Transport,
    /// The peer closed the connection.
    Closed,
    /// A browser WebRTC operation failed.
    BrowserWebRtc,
    /// The application Gateway rejected SDP exchange.
    SdpExchange,
    /// Local request validation failed.
    InvalidRequest,
    /// The peer violated the typed protocol.
    Protocol,
    /// The owning operation was cancelled.
    Cancelled,
    /// The owning deadline expired.
    DeadlineExceeded,
}

/// Bounded lifecycle signal for metrics, logs, and tracing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeReconnectEvent {
    /// A safe replacement was delayed by backoff.
    Scheduled {
        /// One-based replacement attempt.
        attempt: u32,
        /// Selected backoff, including jitter.
        delay: Duration,
    },
    /// The connection factory is being invoked.
    AttemptStarted {
        /// One-based replacement attempt.
        attempt: u32,
    },
    /// A replacement connection failed.
    AttemptFailed {
        /// One-based replacement attempt.
        attempt: u32,
        /// Redacted failure category.
        kind: OpenAiRealtimeReconnectFailureKind,
        /// Whether policy permits another replacement attempt.
        retryable: bool,
    },
    /// A replacement connection succeeded.
    Connected {
        /// One-based replacement attempt.
        attempt: u32,
    },
    /// Automatic replacement stopped before invoking the connection factory.
    Stopped {
        /// Safety or lifecycle reason.
        reason: OpenAiRealtimeReconnectStopReason,
    },
}

/// Why automatic Realtime recovery stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeReconnectStopReason {
    /// A response may have committed before the connection disappeared.
    AmbiguousResponseInFlight,
    /// All bounded replacement attempts failed.
    AttemptsExhausted,
    /// The owning operation was cancelled.
    Cancelled,
    /// Backoff or connection establishment crossed the owning deadline.
    DeadlineExceeded,
    /// A local or protocol error cannot be repaired by reconnecting.
    PermanentFailure,
}

/// Terminal failure from automatic Realtime recovery.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenAiRealtimeReconnectError {
    /// Replaying while a response is in flight could duplicate committed work.
    #[error("Realtime response outcome is ambiguous; automatic reconnect is forbidden")]
    AmbiguousResponseInFlight,
    /// Every bounded replacement attempt failed.
    #[error("Realtime reconnect exhausted {attempts} replacement attempts")]
    AttemptsExhausted {
        /// Number of attempted replacement connections.
        attempts: u32,
        /// Last typed connection failure.
        #[source]
        source: Box<OpenAiRealtimeError>,
    },
    /// A local or protocol failure is not recoverable by replacement.
    #[error("Realtime reconnect stopped after a permanent failure")]
    Permanent {
        /// Typed permanent failure.
        #[source]
        source: Box<OpenAiRealtimeError>,
    },
    /// The owning operation was cancelled.
    #[error("Realtime reconnect was cancelled")]
    Cancelled,
    /// Backoff or connection establishment exceeded the owning deadline.
    #[error("Realtime reconnect exceeded its deadline")]
    DeadlineExceeded,
}

/// Safety-first coordinator for one lost Realtime connection.
///
/// A mutable borrow prevents two replacement loops from racing through the
/// same controller. The supplied connection factory is invoked once per
/// attempt and must acquire a new ephemeral credential inside that invocation;
/// credentials and SDP answers must not be cached between attempts.
pub struct OpenAiRealtimeReconnectController {
    policy: OpenAiRealtimeReconnectPolicy,
    sleeper: Arc<dyn RouterSleeper>,
    generation: u64,
}

impl std::fmt::Debug for OpenAiRealtimeReconnectController {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimeReconnectController")
            .field("policy", &self.policy)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl OpenAiRealtimeReconnectController {
    /// Creates a controller backed by the runtime-neutral production timer.
    pub fn new(policy: OpenAiRealtimeReconnectPolicy) -> Self {
        Self::with_sleeper(policy, Arc::new(SystemRouterSleeper))
    }

    /// Creates a controller with an injected asynchronous timer.
    ///
    /// This is useful for deterministic simulation and virtual-time tests.
    pub fn with_sleeper(
        policy: OpenAiRealtimeReconnectPolicy,
        sleeper: Arc<dyn RouterSleeper>,
    ) -> Self {
        Self {
            policy,
            sleeper,
            generation: 0,
        }
    }

    /// Returns the active replacement policy.
    pub const fn policy(&self) -> &OpenAiRealtimeReconnectPolicy {
        &self.policy
    }

    /// Opens a fresh connection using bounded, deadline-aware retries.
    ///
    /// The `connect` callback is the credential-rotation boundary. For WebRTC,
    /// each invocation must obtain a new client secret and create a new peer
    /// offer. `observe` receives redacted lifecycle events and is never passed
    /// credentials, SDP, close reasons, or server payloads.
    ///
    /// # Errors
    ///
    /// Fails closed for ambiguous in-flight responses, permanent protocol or
    /// validation failures, cancellation, deadline expiry, and exhaustion.
    pub async fn reconnect<T, Connect, ConnectFuture, Observe>(
        &mut self,
        disposition: RealtimeReconnectDisposition,
        context: ModelCallContext,
        mut connect: Connect,
        mut observe: Observe,
    ) -> Result<T, OpenAiRealtimeReconnectError>
    where
        Connect: FnMut(OpenAiRealtimeReconnectAttempt, ModelCallContext) -> ConnectFuture,
        ConnectFuture: Future<Output = Result<T, OpenAiRealtimeError>>,
        Observe: FnMut(OpenAiRealtimeReconnectEvent),
    {
        if disposition == RealtimeReconnectDisposition::AmbiguousResponseInFlight {
            observe(OpenAiRealtimeReconnectEvent::Stopped {
                reason: OpenAiRealtimeReconnectStopReason::AmbiguousResponseInFlight,
            });
            return Err(OpenAiRealtimeReconnectError::AmbiguousResponseInFlight);
        }

        self.generation = self.generation.wrapping_add(1);
        let generation = self.generation;
        for ordinal in 1..=self.policy.max_attempts {
            let entropy =
                reconnect_entropy(&context.invocation_id().to_string(), generation, ordinal);
            let delay = self.policy.delay(ordinal, entropy);
            observe(OpenAiRealtimeReconnectEvent::Scheduled {
                attempt: ordinal,
                delay,
            });
            wait_for_backoff(delay, &context, self.sleeper.as_ref())
                .await
                .map_err(|error| {
                    observe(OpenAiRealtimeReconnectEvent::Stopped {
                        reason: stop_reason(&error),
                    });
                    reconnect_terminal(&error)
                })?;

            observe(OpenAiRealtimeReconnectEvent::AttemptStarted { attempt: ordinal });
            let attempt = OpenAiRealtimeReconnectAttempt { ordinal };
            match run_connect_attempt(connect(attempt, context.child_attempt()), &context).await {
                Ok(connection) => {
                    observe(OpenAiRealtimeReconnectEvent::Connected { attempt: ordinal });
                    return Ok(connection);
                }
                Err(error) => {
                    let retryable = retryable(&error);
                    observe(OpenAiRealtimeReconnectEvent::AttemptFailed {
                        attempt: ordinal,
                        kind: failure_kind(&error),
                        retryable: retryable && ordinal < self.policy.max_attempts,
                    });
                    if let Some(terminal) = terminal_error(error, ordinal, &self.policy) {
                        observe(OpenAiRealtimeReconnectEvent::Stopped {
                            reason: terminal.stop_reason(),
                        });
                        return Err(terminal);
                    }
                }
            }
        }
        unreachable!("validated reconnect policy always executes at least one attempt")
    }
}

impl OpenAiRealtimeReconnectError {
    const fn stop_reason(&self) -> OpenAiRealtimeReconnectStopReason {
        match self {
            Self::AmbiguousResponseInFlight => {
                OpenAiRealtimeReconnectStopReason::AmbiguousResponseInFlight
            }
            Self::AttemptsExhausted { .. } => OpenAiRealtimeReconnectStopReason::AttemptsExhausted,
            Self::Permanent { .. } => OpenAiRealtimeReconnectStopReason::PermanentFailure,
            Self::Cancelled => OpenAiRealtimeReconnectStopReason::Cancelled,
            Self::DeadlineExceeded => OpenAiRealtimeReconnectStopReason::DeadlineExceeded,
        }
    }
}

async fn wait_for_backoff(
    delay: Duration,
    context: &ModelCallContext,
    sleeper: &dyn RouterSleeper,
) -> Result<(), OpenAiRealtimeError> {
    preflight(context)?;
    if context
        .remaining()
        .is_some_and(|remaining| delay >= remaining)
    {
        return Err(OpenAiRealtimeError::DeadlineExceeded);
    }
    if delay.is_zero() {
        return Ok(());
    }
    let cancellation = context.cancellation().clone();
    let cancellable = async {
        match select(
            Box::pin(cancellation.cancelled()),
            Box::pin(sleeper.sleep(delay)),
        )
        .await
        {
            Either::Left(_) => Err(OpenAiRealtimeError::Cancelled),
            Either::Right(_) => Ok(()),
        }
    };
    if let Some(remaining) = context.remaining() {
        match select(
            Box::pin(futures_timer::Delay::new(remaining)),
            Box::pin(cancellable),
        )
        .await
        {
            Either::Left(_) => Err(OpenAiRealtimeError::DeadlineExceeded),
            Either::Right((result, _)) => result,
        }
    } else {
        cancellable.await
    }
}

async fn run_connect_attempt<T>(
    future: impl Future<Output = Result<T, OpenAiRealtimeError>>,
    context: &ModelCallContext,
) -> Result<T, OpenAiRealtimeError> {
    preflight(context)?;
    let cancellation = context.cancellation().clone();
    let cancellable = async {
        match select(Box::pin(cancellation.cancelled()), Box::pin(future)).await {
            Either::Left(_) => Err(OpenAiRealtimeError::Cancelled),
            Either::Right((result, _)) => result,
        }
    };
    if let Some(remaining) = context.remaining() {
        match select(
            Box::pin(futures_timer::Delay::new(remaining)),
            Box::pin(cancellable),
        )
        .await
        {
            Either::Left(_) => Err(OpenAiRealtimeError::DeadlineExceeded),
            Either::Right((result, _)) => result,
        }
    } else {
        cancellable.await
    }
}

fn preflight(context: &ModelCallContext) -> Result<(), OpenAiRealtimeError> {
    if context.cancellation().is_cancelled() {
        return Err(OpenAiRealtimeError::Cancelled);
    }
    if context
        .remaining()
        .is_some_and(|remaining| remaining.is_zero())
    {
        return Err(OpenAiRealtimeError::DeadlineExceeded);
    }
    Ok(())
}

fn retryable(error: &OpenAiRealtimeError) -> bool {
    matches!(
        error,
        OpenAiRealtimeError::Transport
            | OpenAiRealtimeError::BrowserWebRtc(_)
            | OpenAiRealtimeError::SdpExchange {
                retryable: true,
                ..
            }
            | OpenAiRealtimeError::Closed {
                disposition: RealtimeReconnectDisposition::SafeBeforeSession
                    | RealtimeReconnectDisposition::SafeWhenIdle,
                ..
            }
    )
}

fn terminal_error(
    error: OpenAiRealtimeError,
    ordinal: u32,
    policy: &OpenAiRealtimeReconnectPolicy,
) -> Option<OpenAiRealtimeReconnectError> {
    match error {
        OpenAiRealtimeError::Cancelled => Some(OpenAiRealtimeReconnectError::Cancelled),
        OpenAiRealtimeError::DeadlineExceeded => {
            Some(OpenAiRealtimeReconnectError::DeadlineExceeded)
        }
        OpenAiRealtimeError::Closed {
            disposition: RealtimeReconnectDisposition::AmbiguousResponseInFlight,
            ..
        } => Some(OpenAiRealtimeReconnectError::AmbiguousResponseInFlight),
        error if !retryable(&error) => Some(OpenAiRealtimeReconnectError::Permanent {
            source: Box::new(error),
        }),
        error if ordinal == policy.max_attempts => {
            Some(OpenAiRealtimeReconnectError::AttemptsExhausted {
                attempts: ordinal,
                source: Box::new(error),
            })
        }
        _ => None,
    }
}

const fn stop_reason(error: &OpenAiRealtimeError) -> OpenAiRealtimeReconnectStopReason {
    match error {
        OpenAiRealtimeError::Cancelled => OpenAiRealtimeReconnectStopReason::Cancelled,
        OpenAiRealtimeError::DeadlineExceeded => {
            OpenAiRealtimeReconnectStopReason::DeadlineExceeded
        }
        _ => OpenAiRealtimeReconnectStopReason::PermanentFailure,
    }
}

fn reconnect_terminal(error: &OpenAiRealtimeError) -> OpenAiRealtimeReconnectError {
    match error {
        OpenAiRealtimeError::Cancelled => OpenAiRealtimeReconnectError::Cancelled,
        OpenAiRealtimeError::DeadlineExceeded => OpenAiRealtimeReconnectError::DeadlineExceeded,
        _ => unreachable!("backoff only returns cancellation or deadline errors"),
    }
}

const fn failure_kind(error: &OpenAiRealtimeError) -> OpenAiRealtimeReconnectFailureKind {
    match error {
        OpenAiRealtimeError::InvalidRequest(_) => {
            OpenAiRealtimeReconnectFailureKind::InvalidRequest
        }
        OpenAiRealtimeError::Cancelled => OpenAiRealtimeReconnectFailureKind::Cancelled,
        OpenAiRealtimeError::DeadlineExceeded => {
            OpenAiRealtimeReconnectFailureKind::DeadlineExceeded
        }
        OpenAiRealtimeError::Transport => OpenAiRealtimeReconnectFailureKind::Transport,
        OpenAiRealtimeError::BrowserWebRtc(_) => OpenAiRealtimeReconnectFailureKind::BrowserWebRtc,
        OpenAiRealtimeError::SdpExchange { .. } => OpenAiRealtimeReconnectFailureKind::SdpExchange,
        OpenAiRealtimeError::Closed { .. } => OpenAiRealtimeReconnectFailureKind::Closed,
        OpenAiRealtimeError::Protocol(_) => OpenAiRealtimeReconnectFailureKind::Protocol,
    }
}

fn full_jitter(cap: Duration, entropy: u64) -> Duration {
    let cap_nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
    if cap_nanos == u64::MAX {
        return Duration::from_nanos(entropy);
    }
    Duration::from_nanos(entropy % cap_nanos.saturating_add(1))
}

fn reconnect_entropy(invocation: &str, generation: u64, attempt: u32) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in invocation
        .bytes()
        .chain(generation.to_le_bytes())
        .chain(attempt.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures_util::future;
    use runifold_model::{RouterSleepFuture, RouterSleeper};

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingSleeper {
        delays: Mutex<Vec<Duration>>,
    }

    impl RouterSleeper for RecordingSleeper {
        fn sleep(&self, duration: Duration) -> RouterSleepFuture<'_> {
            self.delays.lock().unwrap().push(duration);
            Box::pin(future::ready(()))
        }
    }

    fn test_controller(
        max_attempts: u32,
    ) -> (OpenAiRealtimeReconnectController, Arc<RecordingSleeper>) {
        let sleeper = Arc::new(RecordingSleeper::default());
        let policy = OpenAiRealtimeReconnectPolicy::exponential(
            max_attempts,
            Duration::from_millis(10),
            Duration::from_millis(40),
            2,
        )
        .unwrap()
        .jitter(RetryJitter::None);
        (
            OpenAiRealtimeReconnectController::with_sleeper(policy, sleeper.clone()),
            sleeper,
        )
    }

    #[tokio::test]
    async fn reconnect_rotates_attempts_and_emits_bounded_backoff() {
        let (mut controller, sleeper) = test_controller(3);
        let attempts = Arc::new(Mutex::new(Vec::new()));
        let observed_attempts = attempts.clone();
        let mut events = Vec::new();

        let result = controller
            .reconnect(
                RealtimeReconnectDisposition::SafeWhenIdle,
                ModelCallContext::new(),
                move |attempt, _| {
                    observed_attempts.lock().unwrap().push(attempt);
                    future::ready(if attempt.ordinal() < 3 {
                        Err(OpenAiRealtimeError::Transport)
                    } else {
                        Ok("connected")
                    })
                },
                |event| events.push(event),
            )
            .await
            .unwrap();

        assert_eq!(result, "connected");
        assert_eq!(
            sleeper.delays.lock().unwrap().as_slice(),
            [
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(40)
            ]
        );
        assert!(
            attempts
                .lock()
                .unwrap()
                .iter()
                .all(|attempt| attempt.requires_fresh_credential())
        );
        assert_eq!(
            events.last(),
            Some(&OpenAiRealtimeReconnectEvent::Connected { attempt: 3 })
        );
    }

    #[tokio::test]
    async fn ambiguous_response_never_invokes_connector() {
        let (mut controller, sleeper) = test_controller(3);
        let mut called = false;
        let mut events = Vec::new();

        let error = controller
            .reconnect(
                RealtimeReconnectDisposition::AmbiguousResponseInFlight,
                ModelCallContext::new(),
                |_, _| {
                    called = true;
                    future::ready(Ok::<_, OpenAiRealtimeError>(()))
                },
                |event| events.push(event),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            OpenAiRealtimeReconnectError::AmbiguousResponseInFlight
        ));
        assert!(!called);
        assert!(sleeper.delays.lock().unwrap().is_empty());
        assert_eq!(
            events,
            [OpenAiRealtimeReconnectEvent::Stopped {
                reason: OpenAiRealtimeReconnectStopReason::AmbiguousResponseInFlight
            }]
        );
    }

    #[tokio::test]
    async fn permanent_failure_stops_without_consuming_retry_budget() {
        let (mut controller, sleeper) = test_controller(3);
        let mut calls = 0;

        let error = controller
            .reconnect(
                RealtimeReconnectDisposition::SafeBeforeSession,
                ModelCallContext::new(),
                |_, _| {
                    calls += 1;
                    future::ready(Err::<(), _>(OpenAiRealtimeError::Protocol(
                        "bad event".into(),
                    )))
                },
                |_| {},
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            OpenAiRealtimeReconnectError::Permanent { .. }
        ));
        assert_eq!(calls, 1);
        assert_eq!(
            sleeper.delays.lock().unwrap().as_slice(),
            [Duration::from_millis(10)]
        );
    }

    #[tokio::test]
    async fn retryable_failures_exhaust_the_exact_attempt_budget() {
        let (mut controller, sleeper) = test_controller(2);
        let mut calls = 0;
        let mut events = Vec::new();

        let error = controller
            .reconnect(
                RealtimeReconnectDisposition::SafeWhenIdle,
                ModelCallContext::new(),
                |_, _| {
                    calls += 1;
                    future::ready(Err::<(), _>(OpenAiRealtimeError::Transport))
                },
                |event| events.push(event),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            OpenAiRealtimeReconnectError::AttemptsExhausted { attempts: 2, .. }
        ));
        assert_eq!(calls, 2);
        assert_eq!(sleeper.delays.lock().unwrap().len(), 2);
        assert_eq!(
            events.last(),
            Some(&OpenAiRealtimeReconnectEvent::Stopped {
                reason: OpenAiRealtimeReconnectStopReason::AttemptsExhausted
            })
        );
    }

    #[tokio::test]
    async fn gateway_status_controls_fresh_negotiation_retry() {
        let (mut controller, _) = test_controller(3);
        let mut calls = 0;
        let mut events = Vec::new();

        let error = controller
            .reconnect(
                RealtimeReconnectDisposition::SafeBeforeSession,
                ModelCallContext::new(),
                |_, _| {
                    calls += 1;
                    future::ready(Err::<(), _>(OpenAiRealtimeError::SdpExchange {
                        status: if calls == 1 { 503 } else { 400 },
                        retryable: calls == 1,
                    }))
                },
                |event| events.push(event),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            OpenAiRealtimeReconnectError::Permanent {
                source,
            } if matches!(
                *source,
                OpenAiRealtimeError::SdpExchange {
                    status: 400,
                    retryable: false
                }
            )
        ));
        assert_eq!(calls, 2);
        assert!(
            events.contains(&OpenAiRealtimeReconnectEvent::AttemptFailed {
                attempt: 1,
                kind: OpenAiRealtimeReconnectFailureKind::SdpExchange,
                retryable: true,
            })
        );
    }

    #[tokio::test]
    async fn cancellation_during_backoff_never_invokes_connector() {
        #[derive(Debug)]
        struct CancellingSleeper(runifold_core::CancellationToken);

        impl RouterSleeper for CancellingSleeper {
            fn sleep(&self, _: Duration) -> RouterSleepFuture<'_> {
                self.0.cancel();
                Box::pin(future::pending())
            }
        }

        let context = ModelCallContext::new();
        let cancellation = context.cancellation().clone();
        let policy = OpenAiRealtimeReconnectPolicy::default().jitter(RetryJitter::None);
        let mut controller = OpenAiRealtimeReconnectController::with_sleeper(
            policy,
            Arc::new(CancellingSleeper(cancellation)),
        );
        let mut called = false;

        let error = controller
            .reconnect(
                RealtimeReconnectDisposition::SafeWhenIdle,
                context,
                |_, _| {
                    called = true;
                    future::ready(Ok::<_, OpenAiRealtimeError>(()))
                },
                |_| {},
            )
            .await
            .unwrap_err();

        assert!(matches!(error, OpenAiRealtimeReconnectError::Cancelled));
        assert!(!called);
    }

    #[tokio::test]
    async fn deadline_interrupts_a_stalled_backoff_timer() {
        #[derive(Debug)]
        struct StalledSleeper;

        impl RouterSleeper for StalledSleeper {
            fn sleep(&self, _: Duration) -> RouterSleepFuture<'_> {
                Box::pin(future::pending())
            }
        }

        let policy = OpenAiRealtimeReconnectPolicy::exponential(
            1,
            Duration::from_millis(1),
            Duration::from_millis(1),
            1,
        )
        .unwrap()
        .jitter(RetryJitter::None);
        let mut controller =
            OpenAiRealtimeReconnectController::with_sleeper(policy, Arc::new(StalledSleeper));
        let context = ModelCallContext::new()
            .with_deadline(runifold_core::Instant::now() + Duration::from_millis(20));
        let mut called = false;

        let error = controller
            .reconnect(
                RealtimeReconnectDisposition::SafeWhenIdle,
                context,
                |_, _| {
                    called = true;
                    future::ready(Ok::<_, OpenAiRealtimeError>(()))
                },
                |_| {},
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            OpenAiRealtimeReconnectError::DeadlineExceeded
        ));
        assert!(!called);
    }

    #[test]
    fn policy_rejects_invalid_bounds_and_caps_growth() {
        assert!(matches!(
            OpenAiRealtimeReconnectPolicy::exponential(0, Duration::ZERO, Duration::ZERO, 1),
            Err(OpenAiRealtimeReconnectPolicyError::ZeroMaxAttempts)
        ));
        let policy = OpenAiRealtimeReconnectPolicy::exponential(
            8,
            Duration::from_millis(10),
            Duration::from_millis(25),
            u32::MAX,
        )
        .unwrap()
        .jitter(RetryJitter::None);
        assert_eq!(policy.delay(8, 0), Duration::from_millis(25));
    }
}
