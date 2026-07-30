//! Workflow-store validation, numeric conversion, and error normalization.

use std::time::Duration;

use runifold_core::{CheckpointError, CheckpointErrorKind};
use runifold_workflow::{WorkflowStoreError, WorkflowStoreErrorKind};

use super::PostgresWorkflowStoreError;

pub(super) fn validate_identifier(identifier: &str) -> Result<(), PostgresWorkflowStoreError> {
    let mut characters = identifier.chars();
    let Some(first) = characters.next() else {
        return Err(PostgresWorkflowStoreError::InvalidTable);
    };
    if identifier.len() > 48
        || !(first == '_' || first.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(PostgresWorkflowStoreError::InvalidTable);
    }
    Ok(())
}

pub(super) fn validate_failure_reason(reason: &str) -> Result<(), WorkflowStoreError> {
    if reason.trim().is_empty() || reason.len() > 1_024 {
        return Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::InvalidInput,
            "workflow failure reason must contain 1..=1024 bytes",
        ));
    }
    Ok(())
}

pub(super) fn duration_millis(duration: Duration) -> Result<i64, WorkflowStoreError> {
    let value = u64::try_from(duration.as_millis()).map_err(|_| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::InvalidInput,
            "workflow delay exceeds the supported millisecond range",
        )
    })?;
    database_i64(value, "workflow delay")
}

pub(super) fn database_i64(value: u64, field: &str) -> Result<i64, WorkflowStoreError> {
    i64::try_from(value).map_err(|_| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::InvalidInput,
            format!("{field} exceeds PostgreSQL BIGINT"),
        )
    })
}

pub(super) fn decode_u64(value: i64, field: &str) -> Result<u64, WorkflowStoreError> {
    u64::try_from(value).map_err(|_| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            format!("stored {field} is negative"),
        )
    })
}

pub(super) fn lease_lost() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::LeaseLost,
        "workflow lease is expired, superseded, or owned by another worker",
    )
}

pub(super) fn projection_lease_lost() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::LeaseLost,
        "workflow budget audit projection lease is expired, superseded, or owned by another worker",
    )
}

pub(super) fn storage(error: tokio_postgres::Error) -> WorkflowStoreError {
    drop(error);
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::Storage,
        "PostgreSQL workflow store operation failed",
    )
}

pub(super) fn checkpoint_decoding(_error: serde_json::Error) -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::Storage,
        "stored workflow checkpoint is invalid",
    )
}

pub(super) fn checkpoint_encoding(_error: serde_json::Error) -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::InvalidInput,
        "workflow checkpoint cannot be encoded",
    )
}

pub(super) fn checkpoint_domain_error(error: CheckpointError) -> WorkflowStoreError {
    let kind = match error.kind {
        CheckpointErrorKind::NotFound => WorkflowStoreErrorKind::NotFound,
        CheckpointErrorKind::Conflict => WorkflowStoreErrorKind::Conflict,
        CheckpointErrorKind::InvalidPayload => WorkflowStoreErrorKind::InvalidInput,
        _ => WorkflowStoreErrorKind::Storage,
    };
    WorkflowStoreError::new(kind, error.message)
}

pub(super) fn checkpoint_i64(value: u64, field: &str) -> Result<i64, CheckpointError> {
    i64::try_from(value).map_err(|_| {
        CheckpointError::new(
            CheckpointErrorKind::InvalidPayload,
            format!("{field} exceeds PostgreSQL BIGINT"),
        )
    })
}

pub(super) fn tenant_mismatch() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::TenantMismatch,
        "workflow resource does not belong to the supplied tenant",
    )
}

pub(super) fn checkpoint_lease_lost() -> CheckpointError {
    CheckpointError::new(
        CheckpointErrorKind::Conflict,
        "workflow checkpoint lease is expired, superseded, or owned by another worker",
    )
}

pub(super) fn checkpoint_storage(error: tokio_postgres::Error) -> CheckpointError {
    drop(error);
    CheckpointError::new(
        CheckpointErrorKind::Storage,
        "PostgreSQL workflow checkpoint operation failed",
    )
}
