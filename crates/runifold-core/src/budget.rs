use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Hard limits applied to a run tree.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Budget {
    /// Maximum model tokens.
    pub tokens: Option<u64>,
    /// Maximum cost in micro-US-dollars.
    pub cost_microusd: Option<u64>,
    /// Maximum wall-clock duration attributed to work.
    pub duration: Option<Duration>,
    /// Maximum agent turns.
    pub turns: Option<u64>,
    /// Maximum tool calls.
    pub tool_calls: Option<u64>,
    /// Maximum agent delegations.
    pub delegations: Option<u64>,
}

impl Budget {
    /// Returns the stricter intersection of two budgets.
    #[must_use]
    pub fn tighten(self, other: Self) -> Self {
        Self {
            tokens: minimum(self.tokens, other.tokens),
            cost_microusd: minimum(self.cost_microusd, other.cost_microusd),
            duration: minimum(self.duration, other.duration),
            turns: minimum(self.turns, other.turns),
            tool_calls: minimum(self.tool_calls, other.tool_calls),
            delegations: minimum(self.delegations, other.delegations),
        }
    }
}

fn minimum<T: Ord>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// Resources consumed by a run tree.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Usage {
    /// Model tokens consumed.
    pub tokens: u64,
    /// Cost consumed in micro-US-dollars.
    pub cost_microusd: u64,
    /// Attributed duration in microseconds.
    pub duration_micros: u64,
    /// Agent turns consumed.
    pub turns: u64,
    /// Tool calls consumed.
    pub tool_calls: u64,
    /// Agent delegations consumed.
    pub delegations: u64,
}

impl Usage {
    fn checked_add(self, delta: Self) -> Option<Self> {
        Some(Self {
            tokens: self.tokens.checked_add(delta.tokens)?,
            cost_microusd: self.cost_microusd.checked_add(delta.cost_microusd)?,
            duration_micros: self.duration_micros.checked_add(delta.duration_micros)?,
            turns: self.turns.checked_add(delta.turns)?,
            tool_calls: self.tool_calls.checked_add(delta.tool_calls)?,
            delegations: self.delegations.checked_add(delta.delegations)?,
        })
    }

    fn checked_sub(self, delta: Self) -> Option<Self> {
        Some(Self {
            tokens: self.tokens.checked_sub(delta.tokens)?,
            cost_microusd: self.cost_microusd.checked_sub(delta.cost_microusd)?,
            duration_micros: self.duration_micros.checked_sub(delta.duration_micros)?,
            turns: self.turns.checked_sub(delta.turns)?,
            tool_calls: self.tool_calls.checked_sub(delta.tool_calls)?,
            delegations: self.delegations.checked_sub(delta.delegations)?,
        })
    }
}

/// A resource dimension that can exceed its budget.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum BudgetResource {
    /// Model tokens.
    Tokens,
    /// Monetary cost.
    Cost,
    /// Attributed duration.
    Duration,
    /// Agent turns.
    Turns,
    /// Tool calls.
    ToolCalls,
    /// Agent delegations.
    Delegations,
    /// An integer counter overflowed.
    CounterOverflow,
}

/// An atomic budget rejection.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{resource:?} budget exceeded: limit={limit}, attempted={attempted}")]
pub struct BudgetExceeded {
    /// Resource that exceeded its limit.
    pub resource: BudgetResource,
    /// Configured limit in the resource's base unit.
    pub limit: u128,
    /// Attempted cumulative usage in the same unit.
    pub attempted: u128,
}

/// Thread-safe accounting shared by a run and its descendants.
#[derive(Clone, Debug)]
pub struct BudgetTracker {
    inner: Arc<BudgetState>,
    reservation: Option<Arc<ReservationLease>>,
}

#[derive(Debug)]
struct BudgetState {
    limit: Budget,
    ledger: Mutex<BudgetLedger>,
}

#[derive(Debug, Default)]
struct BudgetLedger {
    usage: Usage,
    reserved: Usage,
    reservations: BTreeMap<u64, Usage>,
    next_reservation_id: u64,
}

#[derive(Debug)]
struct ReservationLease {
    state: Arc<BudgetState>,
    id: u64,
}

impl Drop for ReservationLease {
    fn drop(&mut self) {
        let mut ledger = lock_ledger(&self.state);
        if let Some(remaining) = ledger.reservations.remove(&self.id) {
            ledger.reserved = ledger
                .reserved
                .checked_sub(remaining)
                .expect("reservation aggregate contains every live reservation");
        }
    }
}

/// A scoped share of a run tree's hard budget.
///
/// Cloned trackers returned by [`Self::tracker`] consume only this
/// reservation. Unused resources are released when the reservation and all of
/// its scoped tracker clones are dropped.
#[derive(Clone, Debug)]
pub struct BudgetReservation {
    tracker: BudgetTracker,
    reserved: Usage,
}

impl BudgetReservation {
    /// Returns a tracker constrained to this reservation.
    pub fn tracker(&self) -> BudgetTracker {
        self.tracker.clone()
    }

    /// Returns the original reserved upper bound.
    pub const fn reserved(&self) -> Usage {
        self.reserved
    }

    /// Returns resources not yet committed by this reservation.
    pub fn remaining(&self) -> Usage {
        self.tracker.reservation_remaining().unwrap_or_default()
    }

    /// Conservatively commits every unconsumed resource in this reservation.
    ///
    /// This is useful when work may continue remotely after local
    /// cancellation and its terminal usage can no longer be observed.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetExceeded`] if an accounting counter overflows.
    pub fn forfeit_remaining(&self) -> Result<Usage, BudgetExceeded> {
        self.tracker.forfeit_reservation()
    }

    pub(crate) fn belongs_to(&self, tracker: &BudgetTracker) -> bool {
        Arc::ptr_eq(&self.tracker.inner, &tracker.inner)
    }
}

impl BudgetTracker {
    /// Creates a tracker with the given hard limits.
    pub fn new(limit: Budget) -> Self {
        Self {
            inner: Arc::new(BudgetState {
                limit,
                ledger: Mutex::new(BudgetLedger::default()),
            }),
            reservation: None,
        }
    }

    /// Restores a tracker from a previously persisted usage snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetExceeded`] when `usage` already exceeds `limit`.
    pub fn restore(limit: Budget, usage: Usage) -> Result<Self, BudgetExceeded> {
        validate(limit, usage)?;
        Ok(Self {
            inner: Arc::new(BudgetState {
                limit,
                ledger: Mutex::new(BudgetLedger {
                    usage,
                    ..BudgetLedger::default()
                }),
            }),
            reservation: None,
        })
    }

    /// Returns the configured limits.
    pub fn limit(&self) -> Budget {
        self.inner.limit
    }

    /// Returns a snapshot of cumulative usage.
    pub fn usage(&self) -> Usage {
        self.lock_ledger().usage
    }

    /// Atomically consumes resources or leaves all counters unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetExceeded`] when any cumulative counter would exceed its
    /// configured limit or overflow.
    pub fn try_consume(&self, delta: Usage) -> Result<Usage, BudgetExceeded> {
        let mut ledger = self.lock_ledger();
        if let Some(lease) = &self.reservation {
            let remaining = ledger
                .reservations
                .get(&lease.id)
                .copied()
                .unwrap_or_default();
            validate_reservation(remaining, delta)?;
            let attempted = ledger.usage.checked_add(delta).ok_or(BudgetExceeded {
                resource: BudgetResource::CounterOverflow,
                limit: u128::from(u64::MAX),
                attempted: u128::from(u64::MAX) + 1,
            })?;
            let remaining = remaining.checked_sub(delta).ok_or_else(counter_overflow)?;
            ledger.reservations.insert(lease.id, remaining);
            ledger.reserved = ledger
                .reserved
                .checked_sub(delta)
                .ok_or_else(counter_overflow)?;
            ledger.usage = attempted;
            return Ok(attempted);
        }

        let committed_and_reserved =
            ledger
                .usage
                .checked_add(ledger.reserved)
                .ok_or(BudgetExceeded {
                    resource: BudgetResource::CounterOverflow,
                    limit: u128::from(u64::MAX),
                    attempted: u128::from(u64::MAX) + 1,
                })?;
        let attempted = committed_and_reserved
            .checked_add(delta)
            .ok_or(BudgetExceeded {
                resource: BudgetResource::CounterOverflow,
                limit: u128::from(u64::MAX),
                attempted: u128::from(u64::MAX) + 1,
            })?;

        validate(self.inner.limit, attempted)?;
        ledger.usage = ledger
            .usage
            .checked_add(delta)
            .ok_or_else(counter_overflow)?;
        Ok(ledger.usage)
    }

    /// Atomically reserves one scoped budget share.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetExceeded`] without creating a reservation when the
    /// requested share and all existing commitments exceed a hard limit.
    pub fn try_reserve(&self, amount: Usage) -> Result<BudgetReservation, BudgetExceeded> {
        self.try_reserve_batch([amount])?
            .pop()
            .ok_or_else(counter_overflow)
    }

    /// Atomically reserves multiple independent budget shares.
    ///
    /// Either every requested share is created in input order or none are.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetExceeded`] without changing reservation state when the
    /// complete batch would exceed a hard limit or overflow.
    pub fn try_reserve_batch(
        &self,
        amounts: impl IntoIterator<Item = Usage>,
    ) -> Result<Vec<BudgetReservation>, BudgetExceeded> {
        let amounts = amounts.into_iter().collect::<Vec<_>>();
        let mut requested = Usage::default();
        for amount in &amounts {
            requested = requested.checked_add(*amount).ok_or(BudgetExceeded {
                resource: BudgetResource::CounterOverflow,
                limit: u128::from(u64::MAX),
                attempted: u128::from(u64::MAX) + 1,
            })?;
        }
        let count = u64::try_from(amounts.len()).map_err(|_| counter_overflow())?;

        let mut ledger = self.lock_ledger();
        let start = ledger.next_reservation_id;
        let next_reservation_id = start.checked_add(count).ok_or_else(counter_overflow)?;
        if let Some(lease) = &self.reservation {
            let remaining = ledger
                .reservations
                .get(&lease.id)
                .copied()
                .unwrap_or_default();
            validate_reservation(remaining, requested)?;
            let remaining = remaining
                .checked_sub(requested)
                .ok_or_else(counter_overflow)?;
            ledger.reservations.insert(lease.id, remaining);
        } else {
            let attempted = ledger
                .usage
                .checked_add(ledger.reserved)
                .and_then(|current| current.checked_add(requested))
                .ok_or(BudgetExceeded {
                    resource: BudgetResource::CounterOverflow,
                    limit: u128::from(u64::MAX),
                    attempted: u128::from(u64::MAX) + 1,
                })?;
            validate(self.inner.limit, attempted)?;
            ledger.reserved = ledger
                .reserved
                .checked_add(requested)
                .ok_or_else(counter_overflow)?;
        }

        ledger.next_reservation_id = next_reservation_id;
        let mut reservations = Vec::with_capacity(amounts.len());
        for (id, amount) in (start..next_reservation_id).zip(amounts) {
            ledger.reservations.insert(id, amount);
            reservations.push(BudgetReservation {
                tracker: Self {
                    inner: self.inner.clone(),
                    reservation: Some(Arc::new(ReservationLease {
                        state: self.inner.clone(),
                        id,
                    })),
                },
                reserved: amount,
            });
        }
        Ok(reservations)
    }

    fn reservation_remaining(&self) -> Option<Usage> {
        let lease = self.reservation.as_ref()?;
        self.lock_ledger().reservations.get(&lease.id).copied()
    }

    fn forfeit_reservation(&self) -> Result<Usage, BudgetExceeded> {
        let Some(lease) = &self.reservation else {
            return Err(counter_overflow());
        };
        let mut ledger = self.lock_ledger();
        let remaining = ledger
            .reservations
            .get(&lease.id)
            .copied()
            .unwrap_or_default();
        let attempted = ledger
            .usage
            .checked_add(remaining)
            .ok_or_else(counter_overflow)?;
        ledger.reserved = ledger
            .reserved
            .checked_sub(remaining)
            .ok_or_else(counter_overflow)?;
        ledger.reservations.insert(lease.id, Usage::default());
        ledger.usage = attempted;
        Ok(attempted)
    }

    fn lock_ledger(&self) -> MutexGuard<'_, BudgetLedger> {
        lock_ledger(&self.inner)
    }
}

fn lock_ledger(state: &BudgetState) -> MutexGuard<'_, BudgetLedger> {
    state
        .ledger
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn validate_reservation(remaining: Usage, delta: Usage) -> Result<(), BudgetExceeded> {
    check(BudgetResource::Tokens, Some(remaining.tokens), delta.tokens)?;
    check(
        BudgetResource::Cost,
        Some(remaining.cost_microusd),
        delta.cost_microusd,
    )?;
    check(
        BudgetResource::Duration,
        Some(remaining.duration_micros),
        delta.duration_micros,
    )?;
    check(BudgetResource::Turns, Some(remaining.turns), delta.turns)?;
    check(
        BudgetResource::ToolCalls,
        Some(remaining.tool_calls),
        delta.tool_calls,
    )?;
    check(
        BudgetResource::Delegations,
        Some(remaining.delegations),
        delta.delegations,
    )
}

fn counter_overflow() -> BudgetExceeded {
    BudgetExceeded {
        resource: BudgetResource::CounterOverflow,
        limit: u128::from(u64::MAX),
        attempted: u128::from(u64::MAX) + 1,
    }
}

fn validate(limit: Budget, attempted: Usage) -> Result<(), BudgetExceeded> {
    check(BudgetResource::Tokens, limit.tokens, attempted.tokens)?;
    check(
        BudgetResource::Cost,
        limit.cost_microusd,
        attempted.cost_microusd,
    )?;
    check(
        BudgetResource::Duration,
        limit.duration.map(|duration| duration.as_micros()),
        u128::from(attempted.duration_micros),
    )?;
    check(BudgetResource::Turns, limit.turns, attempted.turns)?;
    check(
        BudgetResource::ToolCalls,
        limit.tool_calls,
        attempted.tool_calls,
    )?;
    check(
        BudgetResource::Delegations,
        limit.delegations,
        attempted.delegations,
    )
}

fn check<T>(resource: BudgetResource, limit: Option<T>, attempted: T) -> Result<(), BudgetExceeded>
where
    T: Copy + Into<u128> + Ord,
{
    if let Some(limit) = limit
        && attempted > limit
    {
        return Err(BudgetExceeded {
            resource,
            limit: limit.into(),
            attempted: attempted.into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Budget, BudgetResource, BudgetTracker, Usage};

    #[test]
    fn rejected_consumption_is_atomic() {
        let tracker = BudgetTracker::new(Budget {
            tokens: Some(10),
            tool_calls: Some(1),
            ..Budget::default()
        });

        tracker
            .try_consume(Usage {
                tokens: 6,
                ..Usage::default()
            })
            .unwrap();

        let error = tracker
            .try_consume(Usage {
                tokens: 5,
                tool_calls: 1,
                ..Usage::default()
            })
            .unwrap_err();

        assert_eq!(error.resource, BudgetResource::Tokens);
        assert_eq!(
            tracker.usage(),
            Usage {
                tokens: 6,
                ..Usage::default()
            }
        );
    }

    #[test]
    fn tightening_keeps_stricter_limits() {
        let first = Budget {
            tokens: Some(100),
            turns: None,
            ..Budget::default()
        };
        let second = Budget {
            tokens: Some(50),
            turns: Some(3),
            ..Budget::default()
        };

        let tightened = first.tighten(second);

        assert_eq!(tightened.tokens, Some(50));
        assert_eq!(tightened.turns, Some(3));
    }

    #[test]
    fn restored_usage_is_validated_against_limits() {
        let tracker = BudgetTracker::restore(
            Budget {
                turns: Some(3),
                ..Budget::default()
            },
            Usage {
                turns: 2,
                ..Usage::default()
            },
        )
        .unwrap();

        assert_eq!(tracker.usage().turns, 2);

        let error = BudgetTracker::restore(
            Budget {
                turns: Some(1),
                ..Budget::default()
            },
            Usage {
                turns: 2,
                ..Usage::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.resource, BudgetResource::Turns);
    }

    #[test]
    fn reservations_isolate_parallel_budget_shares() {
        let tracker = BudgetTracker::new(Budget {
            tokens: Some(10),
            ..Budget::default()
        });
        let reservations = tracker
            .try_reserve_batch([
                Usage {
                    tokens: 4,
                    ..Usage::default()
                },
                Usage {
                    tokens: 6,
                    ..Usage::default()
                },
            ])
            .unwrap();

        let unreserved = tracker
            .try_consume(Usage {
                tokens: 1,
                ..Usage::default()
            })
            .unwrap_err();
        assert_eq!(unreserved.resource, BudgetResource::Tokens);

        reservations[0]
            .tracker()
            .try_consume(Usage {
                tokens: 4,
                ..Usage::default()
            })
            .unwrap();
        let branch_error = reservations[1]
            .tracker()
            .try_consume(Usage {
                tokens: 7,
                ..Usage::default()
            })
            .unwrap_err();

        assert_eq!(branch_error.limit, 6);
        assert_eq!(tracker.usage().tokens, 4);
    }

    #[test]
    fn unused_reservation_is_released_on_last_scoped_drop() {
        let tracker = BudgetTracker::new(Budget {
            turns: Some(2),
            ..Budget::default()
        });
        let reservation = tracker
            .try_reserve(Usage {
                turns: 2,
                ..Usage::default()
            })
            .unwrap();
        let scoped = reservation.tracker();
        drop(reservation);

        assert!(
            tracker
                .try_consume(Usage {
                    turns: 1,
                    ..Usage::default()
                })
                .is_err()
        );
        drop(scoped);

        tracker
            .try_consume(Usage {
                turns: 2,
                ..Usage::default()
            })
            .unwrap();
    }

    #[test]
    fn forfeiting_a_reservation_commits_its_entire_remaining_share() {
        let tracker = BudgetTracker::new(Budget {
            turns: Some(3),
            ..Budget::default()
        });
        let reservation = tracker
            .try_reserve(Usage {
                turns: 2,
                ..Usage::default()
            })
            .unwrap();
        reservation
            .tracker()
            .try_consume(Usage {
                turns: 1,
                ..Usage::default()
            })
            .unwrap();

        let usage = reservation.forfeit_remaining().unwrap();

        assert_eq!(usage.turns, 2);
        assert_eq!(reservation.remaining().turns, 0);
        tracker
            .try_consume(Usage {
                turns: 1,
                ..Usage::default()
            })
            .unwrap();
        assert_eq!(tracker.usage().turns, 3);
    }

    #[test]
    fn failed_batch_reservation_changes_nothing() {
        let tracker = BudgetTracker::new(Budget {
            tool_calls: Some(2),
            ..Budget::default()
        });

        let error = tracker
            .try_reserve_batch([
                Usage {
                    tool_calls: 1,
                    ..Usage::default()
                },
                Usage {
                    tool_calls: 2,
                    ..Usage::default()
                },
            ])
            .unwrap_err();

        assert_eq!(error.resource, BudgetResource::ToolCalls);
        tracker
            .try_consume(Usage {
                tool_calls: 2,
                ..Usage::default()
            })
            .unwrap();
    }

    #[test]
    fn nested_reservation_cannot_exceed_parent_share() {
        let tracker = BudgetTracker::new(Budget {
            delegations: Some(5),
            ..Budget::default()
        });
        let parent = tracker
            .try_reserve(Usage {
                delegations: 3,
                ..Usage::default()
            })
            .unwrap();
        let child = parent
            .tracker()
            .try_reserve(Usage {
                delegations: 2,
                ..Usage::default()
            })
            .unwrap();

        let error = parent
            .tracker()
            .try_consume(Usage {
                delegations: 2,
                ..Usage::default()
            })
            .unwrap_err();
        assert_eq!(error.limit, 1);

        child
            .tracker()
            .try_consume(Usage {
                delegations: 2,
                ..Usage::default()
            })
            .unwrap();
        assert_eq!(tracker.usage().delegations, 2);
    }
}
