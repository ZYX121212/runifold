use super::super::model::duration_micros;
use super::{
    BTreeMap, Budget, CheckpointId, InMemoryWorkflowStore, LeaseDuration, StoredBudgetAuditEvent,
    StoredBudgetAuditProjection, StoredBudgetReservation, StoredTenantBudget, Usage, WorkerId,
    WorkflowBudgetAuditCursor, WorkflowBudgetAuditEvent, WorkflowBudgetAuditKind,
    WorkflowBudgetAuditLimit, WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease,
    WorkflowBudgetForfeitReason, WorkflowBudgetReservationOutcome, WorkflowLease,
    WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowTenantBudgetPolicy,
    WorkflowTenantBudgetSnapshot, WorkflowTenantId, WorkflowTenantListLimit, require_current_lease,
    workflow_not_found,
};

pub(super) fn budget_request(
    tenant_limit: Budget,
    workflow_limit: Budget,
    baseline: Usage,
) -> Result<Usage, WorkflowStoreError> {
    Ok(Usage {
        tokens: remaining_budget(
            "tokens",
            tenant_limit.tokens,
            workflow_limit.tokens,
            baseline.tokens,
        )?,
        cost_microusd: remaining_budget(
            "cost",
            tenant_limit.cost_microusd,
            workflow_limit.cost_microusd,
            baseline.cost_microusd,
        )?,
        duration_micros: remaining_budget(
            "duration",
            tenant_limit.duration.map(duration_micros).transpose()?,
            workflow_limit.duration.map(duration_micros).transpose()?,
            baseline.duration_micros,
        )?,
        turns: remaining_budget(
            "turns",
            tenant_limit.turns,
            workflow_limit.turns,
            baseline.turns,
        )?,
        tool_calls: remaining_budget(
            "tool calls",
            tenant_limit.tool_calls,
            workflow_limit.tool_calls,
            baseline.tool_calls,
        )?,
        delegations: remaining_budget(
            "delegations",
            tenant_limit.delegations,
            workflow_limit.delegations,
            baseline.delegations,
        )?,
    })
}

pub(super) fn remaining_budget(
    resource: &str,
    tenant_limit: Option<u64>,
    workflow_limit: Option<u64>,
    used: u64,
) -> Result<u64, WorkflowStoreError> {
    if tenant_limit.is_none() {
        return Ok(0);
    }
    let workflow_limit = workflow_limit.ok_or_else(|| {
        WorkflowStoreError::invalid_input(format!(
            "tenant-controlled {resource} requires a finite workflow definition limit"
        ))
    })?;
    workflow_limit.checked_sub(used).ok_or_else(|| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::Conflict,
            format!("persisted workflow {resource} exceeds its definition limit"),
        )
    })
}

pub(super) fn controlled_usage(limit: Budget, usage: Usage) -> Usage {
    Usage {
        tokens: limit.tokens.map_or(0, |_| usage.tokens),
        cost_microusd: limit.cost_microusd.map_or(0, |_| usage.cost_microusd),
        duration_micros: limit.duration.map_or(0, |_| usage.duration_micros),
        turns: limit.turns.map_or(0, |_| usage.turns),
        tool_calls: limit.tool_calls.map_or(0, |_| usage.tool_calls),
        delegations: limit.delegations.map_or(0, |_| usage.delegations),
    }
}

pub(super) fn usage_checked_add(left: Usage, right: Usage) -> Result<Usage, WorkflowStoreError> {
    Ok(Usage {
        tokens: left
            .tokens
            .checked_add(right.tokens)
            .ok_or_else(budget_overflow)?,
        cost_microusd: left
            .cost_microusd
            .checked_add(right.cost_microusd)
            .ok_or_else(budget_overflow)?,
        duration_micros: left
            .duration_micros
            .checked_add(right.duration_micros)
            .ok_or_else(budget_overflow)?,
        turns: left
            .turns
            .checked_add(right.turns)
            .ok_or_else(budget_overflow)?,
        tool_calls: left
            .tool_calls
            .checked_add(right.tool_calls)
            .ok_or_else(budget_overflow)?,
        delegations: left
            .delegations
            .checked_add(right.delegations)
            .ok_or_else(budget_overflow)?,
    })
}

pub(super) fn usage_checked_sub(left: Usage, right: Usage) -> Result<Usage, WorkflowStoreError> {
    Ok(Usage {
        tokens: left
            .tokens
            .checked_sub(right.tokens)
            .ok_or_else(budget_overflow)?,
        cost_microusd: left
            .cost_microusd
            .checked_sub(right.cost_microusd)
            .ok_or_else(budget_overflow)?,
        duration_micros: left
            .duration_micros
            .checked_sub(right.duration_micros)
            .ok_or_else(budget_overflow)?,
        turns: left
            .turns
            .checked_sub(right.turns)
            .ok_or_else(budget_overflow)?,
        tool_calls: left
            .tool_calls
            .checked_sub(right.tool_calls)
            .ok_or_else(budget_overflow)?,
        delegations: left
            .delegations
            .checked_sub(right.delegations)
            .ok_or_else(budget_overflow)?,
    })
}

pub(super) fn usage_fits(usage: Usage, limit: Usage) -> bool {
    usage.tokens <= limit.tokens
        && usage.cost_microusd <= limit.cost_microusd
        && usage.duration_micros <= limit.duration_micros
        && usage.turns <= limit.turns
        && usage.tool_calls <= limit.tool_calls
        && usage.delegations <= limit.delegations
}

pub(super) fn validate_usage_budget(
    limit: Budget,
    attempted: Usage,
) -> Result<(), WorkflowStoreError> {
    let duration_limit = limit.duration.map(duration_micros).transpose()?;
    let exceeded = [
        ("tokens", limit.tokens, attempted.tokens),
        ("cost", limit.cost_microusd, attempted.cost_microusd),
        ("duration", duration_limit, attempted.duration_micros),
        ("turns", limit.turns, attempted.turns),
        ("tool calls", limit.tool_calls, attempted.tool_calls),
        ("delegations", limit.delegations, attempted.delegations),
    ]
    .into_iter()
    .find(|(_, configured, used)| configured.is_some_and(|limit| *used > limit));
    if let Some((resource, _, _)) = exceeded {
        return Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::AdmissionDenied,
            format!("workflow tenant {resource} budget is exhausted"),
        ));
    }
    Ok(())
}

pub(super) fn validate_projection_advance(
    expected: WorkflowBudgetAuditCursor,
    next: WorkflowBudgetAuditCursor,
) -> Result<(), WorkflowStoreError> {
    if next <= expected {
        return Err(WorkflowStoreError::invalid_input(
            "workflow budget audit projection cursor must advance monotonically",
        ));
    }
    Ok(())
}

pub(super) fn require_current_projection_lease(
    projection: &StoredBudgetAuditProjection,
    lease: &WorkflowBudgetAuditProjectionLease,
    now: u64,
) -> Result<(), WorkflowStoreError> {
    if projection.owner.as_ref() != Some(&lease.owner)
        || projection.fencing_token != lease.fencing_token
        || projection
            .expires_at_ms
            .is_none_or(|expires_at_ms| expires_at_ms <= now)
    {
        return Err(projection_lease_lost());
    }
    Ok(())
}

pub(super) fn budget_audit_projection_lease(
    tenant_id: WorkflowTenantId,
    projection_id: WorkflowBudgetAuditProjectionId,
    owner: WorkerId,
    projection: &StoredBudgetAuditProjection,
) -> WorkflowBudgetAuditProjectionLease {
    WorkflowBudgetAuditProjectionLease {
        tenant_id,
        projection_id,
        owner,
        cursor: projection.cursor,
        fencing_token: projection.fencing_token,
        expires_at_ms: projection
            .expires_at_ms
            .expect("owned budget audit projection has an expiration"),
    }
}

pub(super) fn projection_lease_lost() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::LeaseLost,
        "workflow budget audit projection lease is expired, superseded, or owned by another worker",
    )
}

pub(super) fn tenant_budget_not_found(tenant_id: &WorkflowTenantId) -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::NotFound,
        format!(
            "workflow tenant `{}` has no budget policy",
            tenant_id.as_str()
        ),
    )
}

pub(super) fn maintain_budget_ledger(
    ledger: &mut StoredTenantBudget,
    now: u64,
) -> Result<(), WorkflowStoreError> {
    let expired = ledger
        .reservations
        .iter()
        .filter_map(|(checkpoint_id, reservation)| {
            (reservation.expires_at_ms <= now).then_some(*checkpoint_id)
        })
        .collect::<Vec<_>>();
    for checkpoint_id in expired {
        forfeit_budget_reservation(
            ledger,
            checkpoint_id,
            now,
            WorkflowBudgetForfeitReason::RecoveryExpired,
        )?;
    }
    if ledger.reservations.is_empty()
        && now
            >= ledger
                .window_started_at_ms
                .saturating_add(ledger.policy.window_millis())
    {
        let drained = ledger.committed;
        ledger.window_started_at_ms = now;
        ledger.committed = Usage::default();
        ledger.reserved = Usage::default();
        record_budget_audit(
            ledger,
            None,
            now,
            WorkflowBudgetAuditKind::WindowReset,
            drained,
            None,
        )?;
    }
    Ok(())
}

pub(super) fn forfeit_budget_reservation(
    ledger: &mut StoredTenantBudget,
    checkpoint_id: CheckpointId,
    now: u64,
    reason: WorkflowBudgetForfeitReason,
) -> Result<(), WorkflowStoreError> {
    let Some(reservation) = ledger.reservations.remove(&checkpoint_id) else {
        return Ok(());
    };
    ledger.reserved = usage_checked_sub(ledger.reserved, reservation.amount)?;
    ledger.committed = usage_checked_add(ledger.committed, reservation.amount)?;
    record_budget_audit(
        ledger,
        Some(checkpoint_id),
        now,
        WorkflowBudgetAuditKind::Forfeited(reason),
        reservation.amount,
        Some(now.saturating_sub(reservation.reserved_at_ms)),
    )?;
    Ok(())
}

pub(super) fn adopt_budget_reservation(
    ledger: &mut StoredTenantBudget,
    checkpoint_id: CheckpointId,
    existing: StoredBudgetReservation,
    baseline: Usage,
    request: Usage,
    now: u64,
) -> Result<(), WorkflowStoreError> {
    let observed = usage_checked_sub(baseline, existing.baseline).map_err(|_| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::Conflict,
            "workflow budget takeover baseline moved backwards",
        )
    })?;
    let charged = controlled_usage(ledger.policy.limit(), observed);
    if !usage_fits(charged, existing.amount) {
        record_budget_audit(
            ledger,
            Some(checkpoint_id),
            now,
            WorkflowBudgetAuditKind::UsageExceeded,
            charged,
            Some(now.saturating_sub(existing.reserved_at_ms)),
        )?;
        return Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::AdmissionDenied,
            "workflow checkpoint usage exceeded its reserved tenant budget",
        ));
    }
    let remaining = usage_checked_sub(existing.amount, charged)?;
    if remaining != request {
        return Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::Conflict,
            "workflow budget envelope changed while its reservation was recoverable",
        ));
    }
    ledger.committed = usage_checked_add(ledger.committed, charged)?;
    ledger.reserved = usage_checked_sub(ledger.reserved, charged)?;
    ledger.reservations.insert(
        checkpoint_id,
        StoredBudgetReservation {
            baseline,
            amount: remaining,
            reserved_at_ms: existing.reserved_at_ms,
            expires_at_ms: existing.expires_at_ms,
        },
    );
    record_budget_audit(
        ledger,
        Some(checkpoint_id),
        now,
        WorkflowBudgetAuditKind::Adopted,
        charged,
        Some(now.saturating_sub(existing.reserved_at_ms)),
    )?;
    Ok(())
}

pub(super) fn record_budget_audit(
    ledger: &mut StoredTenantBudget,
    checkpoint_id: Option<CheckpointId>,
    occurred_at_ms: u64,
    kind: WorkflowBudgetAuditKind,
    usage: Usage,
    reservation_age_ms: Option<u64>,
) -> Result<(), WorkflowStoreError> {
    ledger.next_audit_sequence = ledger.next_audit_sequence.checked_add(1).ok_or_else(|| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "workflow tenant budget audit sequence overflow",
        )
    })?;
    ledger.audit_events.push(StoredBudgetAuditEvent {
        cursor: WorkflowBudgetAuditCursor::new(ledger.next_audit_sequence),
        checkpoint_id,
        occurred_at_ms,
        kind,
        usage,
        reservation_age_ms,
        limit: ledger.policy.limit(),
        committed: ledger.committed,
        reserved: ledger.reserved,
    });
    Ok(())
}

pub(super) fn budget_audit_event(
    tenant_id: WorkflowTenantId,
    stored: &StoredBudgetAuditEvent,
) -> WorkflowBudgetAuditEvent {
    WorkflowBudgetAuditEvent {
        cursor: stored.cursor,
        tenant_id,
        checkpoint_id: stored.checkpoint_id,
        occurred_at_ms: stored.occurred_at_ms,
        kind: stored.kind,
        usage: stored.usage,
        reservation_age_ms: stored.reservation_age_ms,
        limit: stored.limit,
        committed: stored.committed,
        reserved: stored.reserved,
    }
}

pub(super) fn budget_snapshot(
    tenant_id: WorkflowTenantId,
    ledger: &StoredTenantBudget,
) -> WorkflowTenantBudgetSnapshot {
    WorkflowTenantBudgetSnapshot {
        tenant_id,
        limit: ledger.policy.limit(),
        window_started_at_ms: ledger.window_started_at_ms,
        committed: ledger.committed,
        reserved: ledger.reserved,
        active_reservations: u64::try_from(ledger.reservations.len()).unwrap_or(u64::MAX),
    }
}

pub(super) fn budget_overflow() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::Storage,
        "workflow tenant budget counter overflow",
    )
}

impl InMemoryWorkflowStore {
    pub(super) fn set_tenant_budget_policy_impl(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantBudgetPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = admission.budgets.get_mut(&tenant_id) {
                maintain_budget_ledger(existing, now)?;
                if !existing.reservations.is_empty() {
                    return Err(WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Conflict,
                        "tenant budget policy cannot change while reservations are active",
                    ));
                }
                existing.policy = policy;
                existing.window_started_at_ms = now;
                existing.committed = Usage::default();
                existing.reserved = Usage::default();
                record_budget_audit(
                    existing,
                    None,
                    now,
                    WorkflowBudgetAuditKind::PolicyConfigured,
                    Usage::default(),
                    None,
                )?;
                return Ok(());
            }
            let mut ledger = StoredTenantBudget {
                policy,
                window_started_at_ms: now,
                committed: Usage::default(),
                reserved: Usage::default(),
                reservations: BTreeMap::new(),
                next_audit_sequence: 0,
                audit_events: Vec::new(),
            };
            record_budget_audit(
                &mut ledger,
                None,
                now,
                WorkflowBudgetAuditKind::PolicyConfigured,
                Usage::default(),
                None,
            )?;
            admission.budgets.insert(tenant_id, ledger);
            Ok(())
        })
    }

    pub(super) fn list_tenant_budgets_impl(
        &self,
        after: Option<WorkflowTenantId>,
        limit: WorkflowTenantListLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>> {
        Box::pin(async move {
            let admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let limit = usize::try_from(limit.get()).unwrap_or(usize::MAX);
            Ok(admission
                .budgets
                .keys()
                .filter(|tenant_id| after.as_ref().is_none_or(|after| *tenant_id > after))
                .take(limit)
                .cloned()
                .collect())
        })
    }

    pub(super) fn inspect_tenant_budget_impl(
        &self,
        tenant_id: WorkflowTenantId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTenantBudgetSnapshot, WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let ledger = admission.budgets.get_mut(&tenant_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!(
                        "workflow tenant `{}` has no budget policy",
                        tenant_id.as_str()
                    ),
                )
            })?;
            maintain_budget_ledger(ledger, now)?;
            Ok(budget_snapshot(tenant_id, ledger))
        })
    }

    pub(super) fn list_tenant_budget_audit_impl(
        &self,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowBudgetAuditCursor>,
        limit: WorkflowBudgetAuditLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowBudgetAuditEvent>, WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let ledger = admission.budgets.get_mut(&tenant_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!(
                        "workflow tenant `{}` has no budget policy",
                        tenant_id.as_str()
                    ),
                )
            })?;
            maintain_budget_ledger(ledger, now)?;
            let after = after.map_or(0, WorkflowBudgetAuditCursor::sequence);
            let limit = usize::try_from(limit.get()).unwrap_or(usize::MAX);
            Ok(ledger
                .audit_events
                .iter()
                .filter(|event| event.cursor.sequence() > after)
                .take(limit)
                .map(|event| budget_audit_event(tenant_id.clone(), event))
                .collect())
        })
    }

    pub(super) fn compact_tenant_budget_audit_impl(
        &self,
        tenant_id: WorkflowTenantId,
        through: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        Box::pin(async move {
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if admission
                .budget_audit_projections
                .iter()
                .filter_map(|((projection_tenant, _), projection)| {
                    (projection_tenant == &tenant_id).then_some(projection.cursor)
                })
                .min()
                .is_some_and(|oldest_projection| through > oldest_projection)
            {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Conflict,
                    "workflow budget audit compaction would overrun a durable projection",
                ));
            }
            let ledger = admission.budgets.get_mut(&tenant_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!(
                        "workflow tenant `{}` has no budget policy",
                        tenant_id.as_str()
                    ),
                )
            })?;
            let before = ledger.audit_events.len();
            ledger.audit_events.retain(|event| event.cursor > through);
            Ok(u64::try_from(before.saturating_sub(ledger.audit_events.len())).unwrap_or(u64::MAX))
        })
    }

    pub(super) fn load_or_create_tenant_budget_audit_projection_impl(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditCursor, WorkflowStoreError>> {
        Box::pin(async move {
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !admission.budgets.contains_key(&tenant_id) {
                return Err(tenant_budget_not_found(&tenant_id));
            }
            Ok(admission
                .budget_audit_projections
                .entry((tenant_id, projection_id))
                .or_default()
                .cursor)
        })
    }

    pub(super) fn advance_tenant_budget_audit_projection_impl(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        expected: WorkflowBudgetAuditCursor,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<bool, WorkflowStoreError>> {
        Box::pin(async move {
            validate_projection_advance(expected, next)?;
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !admission.budgets.contains_key(&tenant_id) {
                return Err(tenant_budget_not_found(&tenant_id));
            }
            let now = self.clock.now_ms();
            let key = (tenant_id, projection_id);
            let projection = admission
                .budget_audit_projections
                .get_mut(&key)
                .ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::NotFound,
                        "workflow budget audit projection is not registered",
                    )
                })?;
            if projection
                .expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms > now)
            {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Conflict,
                    "workflow budget audit projection has an active lease",
                ));
            }
            if projection.cursor != expected {
                return Ok(false);
            }
            projection.cursor = next;
            Ok(true)
        })
    }

    pub(super) fn claim_tenant_budget_audit_projection_impl(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowBudgetAuditProjectionLease>, WorkflowStoreError>,
    > {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !admission.budgets.contains_key(&tenant_id) {
                return Err(tenant_budget_not_found(&tenant_id));
            }
            let projection = admission
                .budget_audit_projections
                .entry((tenant_id.clone(), projection_id.clone()))
                .or_default();
            if projection
                .expires_at_ms
                .is_some_and(|expires_at_ms| expires_at_ms > now)
            {
                return Ok(None);
            }
            projection.fencing_token =
                projection.fencing_token.checked_add(1).ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Storage,
                        "workflow budget audit projection fencing token overflow",
                    )
                })?;
            projection.owner = Some(owner.clone());
            projection.expires_at_ms = Some(now.saturating_add(lease.as_millis()));
            Ok(Some(budget_audit_projection_lease(
                tenant_id,
                projection_id,
                owner,
                projection,
            )))
        })
    }

    pub(super) fn heartbeat_tenant_budget_audit_projection_impl(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let projection = admission
                .budget_audit_projections
                .get_mut(&(lease.tenant_id.clone(), lease.projection_id.clone()))
                .ok_or_else(projection_lease_lost)?;
            require_current_projection_lease(projection, &lease, now)?;
            projection.expires_at_ms = Some(now.saturating_add(extension.as_millis()));
            Ok(budget_audit_projection_lease(
                lease.tenant_id,
                lease.projection_id,
                lease.owner,
                projection,
            ))
        })
    }

    pub(super) fn advance_tenant_budget_audit_projection_lease_impl(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        Box::pin(async move {
            validate_projection_advance(lease.cursor, next)?;
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let projection = admission
                .budget_audit_projections
                .get_mut(&(lease.tenant_id.clone(), lease.projection_id.clone()))
                .ok_or_else(projection_lease_lost)?;
            require_current_projection_lease(projection, &lease, now)?;
            if projection.cursor != lease.cursor {
                return Err(projection_lease_lost());
            }
            projection.cursor = next;
            Ok(budget_audit_projection_lease(
                lease.tenant_id,
                lease.projection_id,
                lease.owner,
                projection,
            ))
        })
    }

    pub(super) fn release_tenant_budget_audit_projection_impl(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let projection = admission
                .budget_audit_projections
                .get_mut(&(lease.tenant_id, lease.projection_id))
                .ok_or_else(projection_lease_lost)?;
            if projection.owner.as_ref() != Some(&lease.owner)
                || projection.fencing_token != lease.fencing_token
            {
                return Err(projection_lease_lost());
            }
            projection.owner = None;
            projection.expires_at_ms = None;
            Ok(())
        })
    }

    pub(super) fn reserve_budget_impl(
        &self,
        lease: WorkflowLease,
        workflow_limit: Budget,
        baseline: Usage,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetReservationOutcome, WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let tasks = self.tasks();
            let stored = tasks
                .get(&lease.checkpoint_id)
                .ok_or_else(|| workflow_not_found(lease.checkpoint_id))?;
            require_current_lease(stored, &lease, now)?;
            let Some(ledger) = admission.budgets.get_mut(&lease.tenant_id) else {
                return Ok(WorkflowBudgetReservationOutcome::NotConfigured);
            };
            maintain_budget_ledger(ledger, now)?;
            let request = budget_request(ledger.policy.limit(), workflow_limit, baseline)?;
            let expires_at_ms = lease
                .expires_at_ms
                .saturating_add(ledger.policy.recovery_grace_millis());
            if let Some(existing) = ledger.reservations.get(&lease.checkpoint_id).copied() {
                adopt_budget_reservation(
                    ledger,
                    lease.checkpoint_id,
                    existing,
                    baseline,
                    request,
                    now,
                )?;
                ledger
                    .reservations
                    .get_mut(&lease.checkpoint_id)
                    .expect("adopted tenant budget reservation remains present")
                    .expires_at_ms = expires_at_ms;
                return Ok(WorkflowBudgetReservationOutcome::Reserved);
            }
            let attempted = usage_checked_add(
                usage_checked_add(ledger.committed, ledger.reserved)?,
                request,
            )?;
            if let Err(error) = validate_usage_budget(ledger.policy.limit(), attempted) {
                if error.kind == WorkflowStoreErrorKind::AdmissionDenied {
                    record_budget_audit(
                        ledger,
                        Some(lease.checkpoint_id),
                        now,
                        WorkflowBudgetAuditKind::AdmissionDenied,
                        request,
                        None,
                    )?;
                }
                return Err(error);
            }
            ledger.reserved = usage_checked_add(ledger.reserved, request)?;
            ledger.reservations.insert(
                lease.checkpoint_id,
                StoredBudgetReservation {
                    baseline,
                    amount: request,
                    reserved_at_ms: now,
                    expires_at_ms,
                },
            );
            record_budget_audit(
                ledger,
                Some(lease.checkpoint_id),
                now,
                WorkflowBudgetAuditKind::Reserved,
                request,
                Some(0),
            )?;
            Ok(WorkflowBudgetReservationOutcome::Reserved)
        })
    }

    pub(super) fn settle_budget_impl(
        &self,
        lease: WorkflowLease,
        cumulative: Usage,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let tasks = self.tasks();
            let stored = tasks
                .get(&lease.checkpoint_id)
                .ok_or_else(|| workflow_not_found(lease.checkpoint_id))?;
            require_current_lease(stored, &lease, now)?;
            let Some(ledger) = admission.budgets.get_mut(&lease.tenant_id) else {
                return Ok(());
            };
            maintain_budget_ledger(ledger, now)?;
            let reservation = ledger
                .reservations
                .get(&lease.checkpoint_id)
                .copied()
                .ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Conflict,
                        "workflow tenant budget reservation does not exist",
                    )
                })?;
            let observed = usage_checked_sub(cumulative, reservation.baseline).map_err(|_| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Conflict,
                    "workflow cumulative usage is older than its budget reservation baseline",
                )
            })?;
            let charged = controlled_usage(ledger.policy.limit(), observed);
            if !usage_fits(charged, reservation.amount) {
                record_budget_audit(
                    ledger,
                    Some(lease.checkpoint_id),
                    now,
                    WorkflowBudgetAuditKind::UsageExceeded,
                    charged,
                    Some(now.saturating_sub(reservation.reserved_at_ms)),
                )?;
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::AdmissionDenied,
                    "workflow usage exceeded its reserved tenant budget envelope",
                ));
            }
            ledger.committed = usage_checked_add(ledger.committed, charged)?;
            ledger.reserved = usage_checked_sub(ledger.reserved, reservation.amount)?;
            ledger.reservations.remove(&lease.checkpoint_id);
            record_budget_audit(
                ledger,
                Some(lease.checkpoint_id),
                now,
                WorkflowBudgetAuditKind::Settled,
                charged,
                Some(now.saturating_sub(reservation.reserved_at_ms)),
            )?;
            Ok(())
        })
    }
}
