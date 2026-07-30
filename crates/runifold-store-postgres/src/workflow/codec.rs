//! `PostgreSQL` row codecs and durable fork representation.

use runifold_core::{Checkpoint, CheckpointId};
use runifold_workflow::{
    ClaimedWorkflow, WorkerId, WorkflowBudgetAuditCursor, WorkflowBudgetAuditEvent,
    WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease, WorkflowCheckpointPhase,
    WorkflowCheckpointRevision, WorkflowLease, WorkflowLineage, WorkflowSignalId,
    WorkflowSignalSnapshot, WorkflowSignalState, WorkflowStoreError, WorkflowStoreErrorKind,
    WorkflowTask, WorkflowTaskSnapshot, WorkflowTaskStatus, WorkflowTenantBudgetSnapshot,
    WorkflowTenantId, WorkflowWait,
};
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

use super::{
    budget::{StoredBudgetLimit, budget_decoding, decode_budget_audit_kind},
    support::{database_i64, decode_u64, storage},
};

#[derive(Debug)]
pub(super) struct ForkStorageFields {
    pub(super) state: &'static str,
    pub(super) wait_kind: Option<&'static str>,
    pub(super) wait_name: Option<String>,
    pub(super) wait: Option<Value>,
    pub(super) wake_delay_ms: Option<i64>,
}

#[derive(Debug)]
pub(super) struct ForkSource {
    pub(super) workflow: String,
    pub(super) workflow_version: i32,
    pub(super) input: Value,
    pub(super) priority: i32,
    pub(super) checkpoint: Checkpoint,
}

#[derive(Debug)]
pub(super) struct PreparedFork {
    pub(super) source: ForkSource,
    pub(super) checkpoint: Value,
    pub(super) lineage: Value,
    pub(super) fields: ForkStorageFields,
}

pub(super) fn decode_budget_snapshot(
    tenant_id: WorkflowTenantId,
    row: &Row,
) -> Result<WorkflowTenantBudgetSnapshot, WorkflowStoreError> {
    let limit: Value = row.try_get(0).map_err(storage)?;
    let limit = serde_json::from_value::<StoredBudgetLimit>(limit)
        .map_err(budget_decoding)?
        .into_budget();
    let window_started_at_ms = decode_u64(
        row.try_get::<_, i64>(1).map_err(storage)?,
        "tenant budget window start",
    )?;
    let committed =
        serde_json::from_value(row.try_get(2).map_err(storage)?).map_err(budget_decoding)?;
    let reserved =
        serde_json::from_value(row.try_get(3).map_err(storage)?).map_err(budget_decoding)?;
    let active_reservations = decode_u64(
        row.try_get::<_, i64>(4).map_err(storage)?,
        "active budget reservation count",
    )?;
    Ok(WorkflowTenantBudgetSnapshot {
        tenant_id,
        limit,
        window_started_at_ms,
        committed,
        reserved,
        active_reservations,
    })
}

pub(super) fn decode_budget_audit_event(
    tenant_id: WorkflowTenantId,
    row: &Row,
) -> Result<WorkflowBudgetAuditEvent, WorkflowStoreError> {
    let sequence = decode_u64(
        row.try_get::<_, i64>(0).map_err(storage)?,
        "workflow budget audit sequence",
    )?;
    let checkpoint_id = row
        .try_get::<_, Option<Uuid>>(1)
        .map_err(storage)?
        .map(CheckpointId::from_uuid);
    let occurred_at_ms = decode_u64(
        row.try_get::<_, i64>(2).map_err(storage)?,
        "workflow budget audit time",
    )?;
    let kind: String = row.try_get(3).map_err(storage)?;
    let reason: Option<String> = row.try_get(4).map_err(storage)?;
    let usage =
        serde_json::from_value(row.try_get(5).map_err(storage)?).map_err(budget_decoding)?;
    let reservation_age_ms = row
        .try_get::<_, Option<i64>>(6)
        .map_err(storage)?
        .map(|value| decode_u64(value, "workflow budget reservation age"))
        .transpose()?;
    let limit = serde_json::from_value::<StoredBudgetLimit>(row.try_get(7).map_err(storage)?)
        .map_err(budget_decoding)?
        .into_budget();
    let committed =
        serde_json::from_value(row.try_get(8).map_err(storage)?).map_err(budget_decoding)?;
    let reserved =
        serde_json::from_value(row.try_get(9).map_err(storage)?).map_err(budget_decoding)?;
    Ok(WorkflowBudgetAuditEvent {
        cursor: WorkflowBudgetAuditCursor::new(sequence),
        tenant_id,
        checkpoint_id,
        occurred_at_ms,
        kind: decode_budget_audit_kind(&kind, reason.as_deref())?,
        usage,
        reservation_age_ms,
        limit,
        committed,
        reserved,
    })
}

pub(super) fn decode_budget_audit_projection_lease(
    tenant_id: WorkflowTenantId,
    projection_id: WorkflowBudgetAuditProjectionId,
    owner: WorkerId,
    row: &Row,
) -> Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError> {
    Ok(WorkflowBudgetAuditProjectionLease {
        tenant_id,
        projection_id,
        owner,
        cursor: WorkflowBudgetAuditCursor::new(decode_u64(
            row.get("sequence"),
            "workflow budget audit projection cursor",
        )?),
        fencing_token: decode_u64(
            row.get("fencing_token"),
            "workflow budget audit projection fencing token",
        )?,
        expires_at_ms: decode_u64(
            row.get("expires_at_ms"),
            "workflow budget audit projection expiration",
        )?,
    })
}

pub(super) fn decode_claim(
    row: &Row,
    worker: WorkerId,
) -> Result<ClaimedWorkflow, WorkflowStoreError> {
    let id: Uuid = row.try_get(0).map_err(storage)?;
    let tenant_id = WorkflowTenantId::parse(row.try_get::<_, String>(1).map_err(storage)?)
        .map_err(|_| {
            WorkflowStoreError::new(
                WorkflowStoreErrorKind::Storage,
                "stored workflow tenant violates domain invariants",
            )
        })?;
    let workflow: String = row.try_get(2).map_err(storage)?;
    let version: i32 = row.try_get(3).map_err(storage)?;
    let input: Value = row.try_get(4).map_err(storage)?;
    let priority: i32 = row.try_get(5).map_err(storage)?;
    let fencing_token = decode_u64(row.try_get::<_, i64>(6).map_err(storage)?, "fencing token")?;
    let attempt = decode_u64(row.try_get::<_, i64>(7).map_err(storage)?, "attempt")?;
    let expires_at_ms = decode_u64(
        row.try_get::<_, i64>(8).map_err(storage)?,
        "lease expiration",
    )?;
    let wake = row
        .try_get::<_, Option<Value>>(9)
        .map_err(storage)?
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| {
            WorkflowStoreError::new(
                WorkflowStoreErrorKind::Storage,
                "stored workflow wake violates the durable schema",
            )
        })?;
    let checkpoint_id = CheckpointId::from_uuid(id);
    let workflow_version = u32::try_from(version).map_err(|_| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "stored workflow version is outside u32",
        )
    })?;
    let task = WorkflowTask {
        checkpoint_id,
        tenant_id: tenant_id.clone(),
        workflow,
        workflow_version,
        input,
        priority,
    };
    task.validate().map_err(|_| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "stored workflow task violates domain invariants",
        )
    })?;
    Ok(ClaimedWorkflow {
        task,
        wake,
        lease: WorkflowLease {
            checkpoint_id,
            tenant_id,
            worker,
            fencing_token,
            attempt,
            expires_at_ms,
        },
    })
}

pub(super) fn decode_snapshot(
    checkpoint_id: CheckpointId,
    tenant_id: WorkflowTenantId,
    row: &Row,
) -> Result<WorkflowTaskSnapshot, WorkflowStoreError> {
    let state: String = row.try_get(0).map_err(storage)?;
    let attempts = decode_u64(row.try_get::<_, i64>(1).map_err(storage)?, "attempt")?;
    let fencing_token = decode_u64(row.try_get::<_, i64>(2).map_err(storage)?, "fencing token")?;
    let owner = row
        .try_get::<_, Option<String>>(3)
        .map_err(storage)?
        .map(WorkerId::parse)
        .transpose()?;
    let lease_expires_at_ms = row
        .try_get::<_, Option<i64>>(4)
        .map_err(storage)?
        .map(|value| decode_u64(value, "lease expiration"))
        .transpose()?;
    let interrupt = row
        .try_get::<_, Option<Value>>(5)
        .map_err(storage)?
        .map(|value| {
            serde_json::from_value::<WorkflowWait>(value)
                .map_err(|_| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Storage,
                        "stored workflow wait is invalid",
                    )
                })
                .and_then(|wait| match wait {
                    WorkflowWait::Interrupt { request } => Ok(request),
                    _ => Err(WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Storage,
                        "stored workflow inspection wait is unsupported",
                    )),
                })
        })
        .transpose()?;
    let lineage = row
        .try_get::<_, Option<Value>>(6)
        .map_err(storage)?
        .map(|value| {
            serde_json::from_value::<WorkflowLineage>(value).map_err(|_| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Storage,
                    "stored workflow lineage is invalid",
                )
            })
        })
        .transpose()?;
    let failure_message = row.try_get::<_, Option<String>>(7).map_err(storage)?;
    let created_at_ms = decode_u64(
        row.try_get::<_, i64>(8).map_err(storage)?,
        "workflow task creation time",
    )?;
    let updated_at_ms = decode_u64(
        row.try_get::<_, i64>(9).map_err(storage)?,
        "workflow task update time",
    )?;
    let workflow = row.try_get::<_, String>(10).map_err(storage)?;
    let workflow_version =
        u32::try_from(row.try_get::<_, i32>(11).map_err(storage)?).map_err(|_| {
            WorkflowStoreError::new(
                WorkflowStoreErrorKind::Storage,
                "stored workflow version is invalid",
            )
        })?;
    let status = match state.as_str() {
        "queued" => WorkflowTaskStatus::Queued,
        "leased" => WorkflowTaskStatus::Leased,
        "waiting_timer" | "waiting_signal" => WorkflowTaskStatus::Waiting,
        "completed" => WorkflowTaskStatus::Completed,
        "failed" => WorkflowTaskStatus::Failed,
        "cancelled" => WorkflowTaskStatus::Cancelled,
        _ => {
            return Err(WorkflowStoreError::new(
                WorkflowStoreErrorKind::Storage,
                "stored workflow state is unsupported",
            ));
        }
    };
    Ok(WorkflowTaskSnapshot {
        checkpoint_id,
        tenant_id,
        workflow,
        workflow_version,
        status,
        created_at_ms,
        updated_at_ms,
        attempts,
        fencing_token,
        owner,
        lease_expires_at_ms,
        interrupt,
        failure_message,
        lineage,
    })
}

pub(super) fn fork_storage_fields(
    revision: &WorkflowCheckpointRevision,
) -> Result<ForkStorageFields, WorkflowStoreError> {
    let WorkflowCheckpointPhase::Waiting { wait, .. } = &revision.state.phase else {
        return Ok(ForkStorageFields {
            state: "queued",
            wait_kind: None,
            wait_name: None,
            wait: None,
            wake_delay_ms: None,
        });
    };
    match wait {
        WorkflowWait::Timer { delay_ms } => Ok(ForkStorageFields {
            state: "waiting_timer",
            wait_kind: Some("timer"),
            wait_name: None,
            wait: None,
            wake_delay_ms: Some(database_i64(*delay_ms, "forked timer delay")?),
        }),
        WorkflowWait::Signal { name } => Ok(ForkStorageFields {
            state: "waiting_signal",
            wait_kind: Some("signal"),
            wait_name: Some(name.as_str().into()),
            wait: None,
            wake_delay_ms: None,
        }),
        WorkflowWait::SignalOrTimeout { name, timeout_ms } => Ok(ForkStorageFields {
            state: "waiting_signal",
            wait_kind: Some("signal"),
            wait_name: Some(name.as_str().into()),
            wait: None,
            wake_delay_ms: Some(database_i64(*timeout_ms, "forked signal timeout")?),
        }),
        WorkflowWait::Interrupt { request } => Ok(ForkStorageFields {
            state: "waiting_signal",
            wait_kind: Some("signal"),
            wait_name: Some(request.signal_name().as_str().into()),
            wait: Some(serde_json::to_value(wait).map_err(|_| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::InvalidInput,
                    "forked workflow wait cannot be encoded",
                )
            })?),
            wake_delay_ms: None,
        }),
        _ => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::InvalidInput,
            "forked workflow wait is not supported by this adapter",
        )),
    }
}

pub(super) fn decode_signal_snapshot(
    signal_id: WorkflowSignalId,
    row: &Row,
) -> Result<WorkflowSignalSnapshot, WorkflowStoreError> {
    let tenant_id = WorkflowTenantId::parse(row.try_get::<_, String>(0).map_err(storage)?)
        .map_err(|_| {
            WorkflowStoreError::new(
                WorkflowStoreErrorKind::Storage,
                "stored workflow signal tenant violates domain invariants",
            )
        })?;
    let checkpoint_id = CheckpointId::from_uuid(row.try_get::<_, Uuid>(1).map_err(storage)?);
    let name =
        runifold_workflow::WorkflowSignalName::parse(row.try_get::<_, String>(2).map_err(storage)?)
            .map_err(|_| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Storage,
                    "stored workflow signal name violates domain invariants",
                )
            })?;
    let consumed: bool = row.try_get(3).map_err(storage)?;
    let dead_lettered: bool = row.try_get(4).map_err(storage)?;
    let accepted_at_ms = decode_u64(
        row.try_get::<_, i64>(5).map_err(storage)?,
        "signal acceptance time",
    )?;
    let state = if consumed {
        WorkflowSignalState::Consumed
    } else if dead_lettered {
        WorkflowSignalState::DeadLettered
    } else {
        WorkflowSignalState::Pending
    };
    Ok(WorkflowSignalSnapshot {
        signal_id,
        tenant_id,
        checkpoint_id,
        name,
        state,
        accepted_at_ms,
    })
}
