use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use futures_util::future::poll_fn;
use runifold_core::{CancellationToken, Instant, RetrySafety};

use super::{
    ModelCapacityLimits, ModelCapacityPoolId, ModelScheduler, ModelSchedulerSnapshot,
    ModelSchedulingError,
};
use crate::{ModelCallContext, ModelError, ModelErrorKind, ModelFuture};

pub(super) struct Pool {
    pub(super) limits: ModelCapacityLimits,
    pub(super) active: usize,
    pub(super) reserved: u128,
    pub(super) settled: u128,
    pub(super) requests: VecDeque<Instant>,
    settlements: VecDeque<(Instant, u128)>,
}

impl Pool {
    pub(super) fn new(limits: ModelCapacityLimits) -> Self {
        Self {
            limits,
            active: 0,
            reserved: 0,
            settled: 0,
            requests: VecDeque::new(),
            settlements: VecDeque::new(),
        }
    }
    fn refresh(&mut self, now: Instant) {
        if let Some(limit) = &self.limits.requests {
            while self
                .requests
                .front()
                .is_some_and(|at| now.saturating_duration_since(*at) >= limit.window)
            {
                self.requests.pop_front();
            }
        }
        if let Some(limit) = &self.limits.tokens {
            while self
                .settlements
                .front()
                .is_some_and(|(at, _)| now.saturating_duration_since(*at) >= limit.window)
            {
                if let Some((_, amount)) = self.settlements.pop_front() {
                    self.settled -= amount;
                }
            }
        }
    }
    fn charge_request(&mut self, at: Instant) {
        let index = self.requests.partition_point(|previous| *previous <= at);
        self.requests.insert(index, at);
    }
    fn settle(&mut self, at: Instant, amount: u128) {
        let index = self
            .settlements
            .partition_point(|(previous, _)| *previous <= at);
        self.settled += amount;
        self.settlements.insert(index, (at, amount));
    }
    fn fits(&self, tokens: u64) -> bool {
        self.active < self.limits.concurrency.get()
            && self
                .limits
                .requests
                .as_ref()
                .is_none_or(|limit| (self.requests.len() as u128) < u128::from(limit.limit.get()))
            && self.limits.tokens.as_ref().is_none_or(|limit| {
                self.reserved + self.settled + u128::from(tokens) <= u128::from(limit.limit.get())
            })
    }
}

pub(super) struct Waiter {
    pools: Vec<ModelCapacityPoolId>,
    tokens: u64,
    since: Instant,
    context: ModelCallContext,
    pub(super) waker: Option<Arc<Waker>>,
}

pub(super) struct State {
    pub(super) pools: BTreeMap<ModelCapacityPoolId, Pool>,
    pub(super) waiters: BTreeMap<u64, Waiter>,
    pub(super) metrics: ModelSchedulerSnapshot,
    next_ticket: u64,
}

impl State {
    pub(super) fn new(pools: BTreeMap<ModelCapacityPoolId, Pool>) -> Self {
        Self {
            pools,
            waiters: BTreeMap::new(),
            metrics: ModelSchedulerSnapshot::default(),
            next_ticket: 0,
        }
    }
    pub(super) fn refresh(&mut self, now: Instant) {
        for pool in self.pools.values_mut() {
            pool.refresh(now);
        }
    }
    fn prune_waiters(&mut self, now: Instant, retired: &mut Vec<Arc<Waker>>) {
        self.waiters.retain(|_, waiter| {
            let cancelled = waiter.context.cancellation().is_cancelled();
            let expired = waiter
                .context
                .deadline()
                .is_some_and(|deadline| deadline <= now);
            if cancelled || expired {
                if cancelled {
                    self.metrics.cancelled = self.metrics.cancelled.saturating_add(1);
                } else {
                    self.metrics.expired = self.metrics.expired.saturating_add(1);
                }
                if let Some(waker) = waiter.waker.take() {
                    retired.push(waker);
                }
                false
            } else {
                true
            }
        });
    }
    fn selected(&self, now: Instant, starvation_after: std::time::Duration) -> Option<u64> {
        let mut blocked = BTreeSet::new();
        for (ticket, waiter) in &self.waiters {
            let fits = waiter
                .pools
                .iter()
                .all(|id| self.pools[id].fits(waiter.tokens));
            if fits && waiter.pools.iter().all(|id| !blocked.contains(id)) {
                return Some(*ticket);
            }
            if now.saturating_duration_since(waiter.since) >= starvation_after {
                blocked.extend(waiter.pools.iter().cloned());
            }
        }
        None
    }
    fn next_change(&self, now: Instant, starvation_after: std::time::Duration) -> Option<Instant> {
        let mut next = None;
        let mut consider = |at: Option<Instant>| {
            if let Some(at) = at.filter(|at| *at > now) {
                next = Some(next.map_or(at, |old: Instant| old.min(at)));
            }
        };
        for pool in self.pools.values() {
            if let (Some(at), Some(limit)) = (pool.requests.front(), &pool.limits.requests) {
                consider(at.checked_add(limit.window));
            }
            if let (Some((at, _)), Some(limit)) = (pool.settlements.front(), &pool.limits.tokens) {
                consider(at.checked_add(limit.window));
            }
        }
        for waiter in self.waiters.values() {
            consider(waiter.since.checked_add(starvation_after));
        }
        next
    }
    fn wakeups(&self) -> Vec<Arc<Waker>> {
        self.waiters
            .values()
            .filter_map(|w| w.waker.clone())
            .collect()
    }
}

pub(super) fn wake(wakers: Vec<Arc<Waker>>) {
    for waker in wakers {
        waker.wake_by_ref();
    }
}

pub(super) fn lifecycle_error(
    context: &ModelCallContext,
    now: Instant,
    dispatched: bool,
    visible: bool,
) -> Option<ModelError> {
    let kind = if context.cancellation().is_cancelled() {
        ModelErrorKind::Cancelled
    } else if context.deadline().is_some_and(|deadline| deadline <= now) {
        ModelErrorKind::DeadlineExceeded
    } else {
        return None;
    };
    let mut error = ModelError::local(
        kind,
        "scheduled model call was cancelled or exceeded its deadline",
    );
    error.retry_safety = if visible {
        RetrySafety::UnsafeAfterVisibleOutput
    } else if dispatched {
        RetrySafety::Unknown
    } else {
        RetrySafety::Safe
    };
    error.metadata.insert(
        "runifold.scheduler.phase".into(),
        if dispatched { "attempt" } else { "queue" }.into(),
    );
    Some(error)
}

/// Owns a queue entry even when its acquisition future is abandoned.
struct Waiting {
    scheduler: ModelScheduler,
    ticket: u64,
    context: ModelCallContext,
    cancelled: ModelFuture<'static, ()>,
    timer: Option<(Instant, ModelFuture<'static, ()>)>,
}

impl ModelScheduler {
    pub(super) async fn acquire(
        &self,
        pools: &[ModelCapacityPoolId],
        tokens: u64,
        context: ModelCallContext,
    ) -> Result<Permit, ModelError> {
        let now = self.shared.clock.now();
        if let Some(error) = lifecycle_error(&context, now, false, false) {
            return Err(error);
        }
        // RawWaker clone/drop callbacks must never run under the state lock.
        let mut retired = Vec::new();
        let ticket = {
            let mut state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.refresh(now);
            state.prune_waiters(now, &mut retired);
            let invalid = pools.iter().find_map(|id| {
                state.pools[id].limits.tokens.as_ref().and_then(|limit| {
                    if tokens == 0 {
                        Some(ModelSchedulingError::MissingTokenReservation)
                    } else if tokens > limit.limit.get() {
                        Some(ModelSchedulingError::ReservationTooLarge)
                    } else {
                        None
                    }
                })
            });
            let invalid = invalid
                .or_else(|| {
                    (state.waiters.len() >= self.shared.max_queued)
                        .then_some(ModelSchedulingError::QueueFull)
                })
                .or_else(|| {
                    (state.next_ticket == u64::MAX)
                        .then_some(ModelSchedulingError::IdentityExhausted)
                });
            if let Some(error) = invalid {
                state.metrics.rejected = state.metrics.rejected.saturating_add(1);
                return Err(error.into());
            }
            let ticket = state.next_ticket;
            state.next_ticket += 1;
            state.waiters.insert(
                ticket,
                Waiter {
                    pools: pools.to_vec(),
                    tokens,
                    since: now,
                    context: context.clone(),
                    waker: None,
                },
            );
            ticket
        };
        let token = context.cancellation().clone();
        let mut waiting = Waiting {
            scheduler: self.clone(),
            ticket,
            context,
            cancelled: Box::pin(async move { token.cancelled().await }),
            timer: None,
        };
        poll_fn(|cx| waiting.poll(cx)).await
    }

    pub(super) fn timer(&self, at: Instant) -> ModelFuture<'static, ()> {
        let sleeper = self.shared.sleeper.clone();
        let clock = self.shared.clock.clone();
        Box::pin(async move {
            sleeper
                .sleep(at.saturating_duration_since(clock.now()))
                .await;
        })
    }
}

impl Waiting {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Permit, ModelError>> {
        let waker = Arc::new(cx.waker().clone());
        let mut retired = Vec::new();
        let _ = self.cancelled.as_mut().poll(cx);
        let now = self.scheduler.shared.clock.now();
        if let Some(error) = lifecycle_error(&self.context, now, false, false) {
            return Poll::Ready(Err(error));
        }
        let (permit, next, wakers) = {
            let mut state = self
                .scheduler
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.refresh(now);
            state.prune_waiters(now, &mut retired);
            if let Some(waiter) = state.waiters.get_mut(&self.ticket)
                && let Some(previous) = waiter.waker.replace(waker)
            {
                retired.push(previous);
            }
            let selected = state.selected(now, self.scheduler.shared.starvation_after);
            if selected == Some(self.ticket) {
                let mut waiter = state
                    .waiters
                    .remove(&self.ticket)
                    .expect("selected waiter exists");
                if let Some(waker) = waiter.waker.take() {
                    retired.push(waker);
                }
                for id in &waiter.pools {
                    let pool = state
                        .pools
                        .get_mut(id)
                        .expect("bindings validated at construction");
                    pool.active += 1;
                    if pool.limits.tokens.is_some() {
                        pool.reserved += u128::from(waiter.tokens);
                    }
                    if pool.limits.requests.is_some() {
                        pool.charge_request(now);
                    }
                }
                state.metrics.admitted = state.metrics.admitted.saturating_add(1);
                state.metrics.queue_wait_micros = state
                    .metrics
                    .queue_wait_micros
                    .saturating_add(now.saturating_duration_since(waiter.since).as_micros());
                let permit = Permit {
                    scheduler: self.scheduler.clone(),
                    pools: waiter.pools,
                    tokens: waiter.tokens,
                    observed: None,
                    charged: u128::from(waiter.tokens),
                    cancellation: self.context.cancellation().clone(),
                    released: false,
                };
                (Some(permit), None, state.wakeups())
            } else {
                let next = state.next_change(now, self.scheduler.shared.starvation_after);
                let wakers = selected
                    .and_then(|id| state.waiters[&id].waker.clone())
                    .into_iter()
                    .collect();
                (None, next, wakers)
            }
        };
        wake(wakers);
        if let Some(permit) = permit {
            return Poll::Ready(Ok(permit));
        }
        let next = match (next, self.context.deadline()) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if self.timer.as_ref().map(|(at, _)| *at) != next {
            self.timer = next.map(|at| (at, self.scheduler.timer(at)));
        }
        if let Some((_, timer)) = &mut self.timer
            && Pin::new(timer).poll(cx).is_ready()
        {
            self.timer = None;
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        let now = self.scheduler.shared.clock.now();
        let mut retired = Vec::new();
        let wakers = {
            let mut state = self
                .scheduler
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(mut waiter) = state.waiters.remove(&self.ticket) {
                if let Some(waker) = waiter.waker.take() {
                    retired.push(waker);
                }
                if self.context.cancellation().is_cancelled() {
                    state.metrics.cancelled = state.metrics.cancelled.saturating_add(1);
                } else if self.context.deadline().is_some_and(|at| at <= now) {
                    state.metrics.expired = state.metrics.expired.saturating_add(1);
                }
            }
            state.wakeups()
        };
        wake(wakers);
    }
}

pub(super) struct Permit {
    scheduler: ModelScheduler,
    pools: Vec<ModelCapacityPoolId>,
    tokens: u64,
    pub(super) observed: Option<u128>,
    charged: u128,
    cancellation: CancellationToken,
    released: bool,
}

impl Permit {
    pub(super) fn observe(&mut self, actual: u128) {
        self.observed = Some(self.observed.unwrap_or(0).max(actual));
        if actual <= self.charged {
            return;
        }
        let mut state = self
            .scheduler
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for id in &self.pools {
            let pool = state
                .pools
                .get_mut(id)
                .expect("permit pools remain configured");
            if pool.limits.tokens.is_some() {
                pool.reserved += actual - self.charged;
            }
        }
        self.charged = actual;
    }
    pub(super) fn finish(&mut self, authoritative: bool) {
        if self.released {
            return;
        }
        self.released = true;
        self.cancellation.cancel();
        let now = self.scheduler.shared.clock.now();
        let known = authoritative && self.observed.is_some();
        let amount = if known {
            self.observed.unwrap_or(u128::from(self.tokens))
        } else {
            u128::from(self.tokens).max(self.observed.unwrap_or(0))
        };
        let wakers = {
            let mut state = self
                .scheduler
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.refresh(now);
            if !known {
                state.metrics.unknown_usage = state.metrics.unknown_usage.saturating_add(1);
            }
            if self
                .observed
                .is_some_and(|actual| actual > u128::from(self.tokens))
                && self
                    .pools
                    .iter()
                    .any(|id| state.pools[id].limits.tokens.is_some())
            {
                state.metrics.token_overruns = state.metrics.token_overruns.saturating_add(1);
            }
            for id in &self.pools {
                let pool = state
                    .pools
                    .get_mut(id)
                    .expect("permit pools remain configured");
                pool.active -= 1;
                if pool.limits.tokens.is_some() {
                    pool.reserved -= self.charged;
                    if amount > 0 {
                        pool.settle(now, amount);
                    }
                }
            }
            state.wakeups()
        };
        wake(wakers);
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.finish(false);
    }
}

#[cfg(test)]
mod ordering_tests {
    use super::*;
    use crate::ModelRateLimit;
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        time::Duration,
    };

    #[test]
    fn expiry_remains_ordered_when_outside_lock_clock_reads_commit_out_of_order() {
        let window = Duration::from_secs(60);
        let limit = ModelRateLimit::new(NonZeroU64::new(100).unwrap(), window).unwrap();
        let mut pool = Pool::new(
            ModelCapacityLimits::new(NonZeroUsize::MIN)
                .requests(limit.clone())
                .tokens(limit),
        );
        let earlier = Instant::now();
        let later = earlier + Duration::from_secs(10);
        pool.charge_request(later);
        pool.charge_request(earlier);
        pool.settle(later, 20);
        pool.settle(earlier, 30);
        pool.refresh(earlier + window);
        assert_eq!(pool.requests.len(), 1);
        assert_eq!(pool.settled, 20);
        pool.refresh(later + window);
        assert!(pool.requests.is_empty());
        assert_eq!(pool.settled, 0);
    }
}
