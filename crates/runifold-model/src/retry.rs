use std::{future::Future, pin::Pin, time::Duration};

use runifold_core::RetrySafety;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ModelError, ModelErrorKind};

/// A boxed sleep future used by routing policy.
pub type RouterSleepFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Asynchronous timer boundary used by retry backoff.
pub trait RouterSleeper: Send + Sync {
    /// Waits for the requested monotonic duration.
    fn sleep(&self, duration: Duration) -> RouterSleepFuture<'_>;
}

/// Runtime-neutral production timer backed by `futures-timer`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemRouterSleeper;

impl RouterSleeper for SystemRouterSleeper {
    fn sleep(&self, duration: Duration) -> RouterSleepFuture<'_> {
        Box::pin(futures_timer::Delay::new(duration))
    }
}

/// Jitter applied to an exponential retry delay.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RetryJitter {
    /// Preserve the exact exponential delay.
    None,
    /// Select a deterministic per-invocation delay from zero through the
    /// exponential cap.
    #[default]
    Full,
}

/// Invalid retry-policy configuration.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelRetryPolicyError {
    /// A policy must include the initial attempt.
    #[error("model retry max_attempts must be greater than zero")]
    ZeroMaxAttempts,
    /// Exponential growth cannot use a zero multiplier.
    #[error("model retry backoff multiplier must be greater than zero")]
    ZeroMultiplier,
    /// Maximum delay cannot be below the initial delay.
    #[error("model retry max_backoff cannot be less than initial_backoff")]
    InvalidBackoffRange,
}

/// Explicit same-route retry and backoff authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelRetryPolicy {
    max_attempts: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    multiplier: u32,
    jitter: RetryJitter,
    unknown_safety_kinds: Vec<ModelErrorKind>,
}

impl ModelRetryPolicy {
    /// Creates an exponential policy. `max_attempts` includes the first call.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRetryPolicyError`] for a zero attempt count, zero
    /// multiplier, or inverted delay range.
    pub fn exponential(
        max_attempts: u32,
        initial_backoff: Duration,
        max_backoff: Duration,
        multiplier: u32,
    ) -> Result<Self, ModelRetryPolicyError> {
        if max_attempts == 0 {
            return Err(ModelRetryPolicyError::ZeroMaxAttempts);
        }
        if multiplier == 0 {
            return Err(ModelRetryPolicyError::ZeroMultiplier);
        }
        if max_backoff < initial_backoff {
            return Err(ModelRetryPolicyError::InvalidBackoffRange);
        }
        Ok(Self {
            max_attempts,
            initial_backoff,
            max_backoff,
            multiplier,
            jitter: RetryJitter::Full,
            unknown_safety_kinds: Vec::new(),
        })
    }

    /// Sets retry jitter.
    #[must_use]
    pub const fn jitter(mut self, jitter: RetryJitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Allows retry for one error kind whose retry safety is unknown.
    ///
    /// This is explicit authority to risk another provider charge. It never
    /// overrides cancellation or an error marked unsafe.
    #[must_use]
    pub fn allow_unknown(mut self, kind: ModelErrorKind) -> Self {
        if !self.unknown_safety_kinds.contains(&kind) {
            self.unknown_safety_kinds.push(kind);
        }
        self
    }

    /// Returns the total attempt bound, including the initial attempt.
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns the initial exponential delay.
    pub const fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    /// Returns the delay cap.
    pub const fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// Returns the integer exponential multiplier.
    pub const fn multiplier(&self) -> u32 {
        self.multiplier
    }

    /// Returns the configured jitter mode.
    pub const fn jitter_mode(&self) -> RetryJitter {
        self.jitter
    }

    pub(crate) fn permits(&self, error: &ModelError) -> bool {
        if error.kind == ModelErrorKind::Cancelled {
            return false;
        }
        match error.retry_safety {
            RetrySafety::Safe => true,
            RetrySafety::Unknown => self.unknown_safety_kinds.contains(&error.kind),
            _ => false,
        }
    }

    pub(crate) fn delay(&self, retry: u32, entropy: u64) -> Duration {
        let exponent = retry.saturating_sub(1);
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
            RetryJitter::Full => full_jitter(delay, entropy),
        }
    }
}

fn full_jitter(cap: Duration, entropy: u64) -> Duration {
    let cap_nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
    if cap_nanos == u64::MAX {
        return Duration::from_nanos(entropy);
    }
    Duration::from_nanos(entropy % cap_nanos.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{ModelError, ModelErrorKind};
    use runifold_core::RetrySafety;

    use super::{ModelRetryPolicy, ModelRetryPolicyError, RetryJitter};

    #[test]
    fn exponential_delay_is_capped_without_overflow() {
        let policy = ModelRetryPolicy::exponential(
            10,
            Duration::from_millis(100),
            Duration::from_secs(1),
            3,
        )
        .unwrap()
        .jitter(RetryJitter::None);

        assert_eq!(policy.delay(1, 0), Duration::from_millis(100));
        assert_eq!(policy.delay(2, 0), Duration::from_millis(300));
        assert_eq!(policy.delay(3, 0), Duration::from_millis(900));
        assert_eq!(policy.delay(4, 0), Duration::from_secs(1));
        assert_eq!(policy.delay(u32::MAX, 0), Duration::from_secs(1));
    }

    #[test]
    fn full_jitter_is_deterministic_and_within_cap() {
        let policy = ModelRetryPolicy::exponential(
            2,
            Duration::from_millis(100),
            Duration::from_millis(100),
            2,
        )
        .unwrap();

        let first = policy.delay(1, 42);
        let second = policy.delay(1, 42);
        assert_eq!(first, second);
        assert!(first <= Duration::from_millis(100));
    }

    #[test]
    fn invalid_policy_is_rejected() {
        assert_eq!(
            ModelRetryPolicy::exponential(
                0,
                Duration::from_millis(1),
                Duration::from_millis(1),
                2,
            )
            .unwrap_err(),
            ModelRetryPolicyError::ZeroMaxAttempts
        );
    }

    #[test]
    fn unknown_error_requires_explicit_retry_authority() {
        let policy = ModelRetryPolicy::exponential(2, Duration::ZERO, Duration::ZERO, 1).unwrap();
        let mut error = ModelError::local(ModelErrorKind::Transport, "failure");
        error.retry_safety = RetrySafety::Unknown;
        assert!(!policy.permits(&error));
        assert!(
            policy
                .allow_unknown(ModelErrorKind::Transport)
                .permits(&error)
        );
    }
}
