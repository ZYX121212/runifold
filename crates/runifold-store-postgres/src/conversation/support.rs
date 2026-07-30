//! `PostgreSQL` conversation codecs, validation, and usage normalization.

use std::{collections::BTreeMap, time::Instant};

use pgvector::Vector;
use runifold_agent::{
    ConversationAppend, ConversationId, ConversationSequence, ConversationStoreError,
    ConversationStoreErrorKind, ConversationSummary, ConversationSummaryCommit,
    ConversationTranscriptEntry, ConversationVersion, MemoryNamespace, SemanticMemory,
    SemanticMemoryId, SemanticMemorySource, SemanticMemoryUpsert,
};
use runifold_core::{CheckpointId, Usage};
use runifold_model::Role;
use runifold_retrieval::{Embedding, RetrievalError};
use serde_json::Value;
use tokio_postgres::Row;
use uuid::Uuid;

use super::{MAX_CONTENT_BYTES, PostgresConversationStoreError};

pub(super) fn decode_transcript_entry(
    row: &Row,
) -> Result<ConversationTranscriptEntry, ConversationStoreError> {
    let sequence = ConversationSequence::new(to_u64(row.get("sequence"))?)?;
    let message = serde_json::from_value(row.get("message")).map_err(decode_error)?;
    Ok(ConversationTranscriptEntry { sequence, message })
}

pub(super) fn decode_summary(
    row: &Row,
) -> Result<Option<ConversationSummary>, ConversationStoreError> {
    let summary_id: Option<Uuid> = row.get("summary_id");
    summary_id.map(|_| decode_required_summary(row)).transpose()
}

pub(super) fn decode_required_summary(
    row: &Row,
) -> Result<ConversationSummary, ConversationStoreError> {
    let summary_id: Uuid = row.get("summary_id");
    Ok(ConversationSummary {
        summary_id: CheckpointId::from_uuid(summary_id),
        content: row.get("summary_content"),
        through_sequence: ConversationSequence::new(to_u64(row.get("summary_through"))?)?,
        transcript_version: decode_version(row.get("summary_transcript_version"))?,
        created_at_ms: to_u64(row.get("summary_created_at_ms"))?,
    })
}

pub(super) fn decode_memory(row: &Row) -> Result<SemanticMemory, ConversationStoreError> {
    let namespace = MemoryNamespace::parse(row.get::<_, String>("namespace"))?;
    let sources: Vec<SemanticMemorySource> =
        serde_json::from_value(row.get("sources")).map_err(decode_error)?;
    let metadata: BTreeMap<String, Value> =
        serde_json::from_value(row.get("metadata")).map_err(decode_error)?;
    Ok(SemanticMemory {
        memory_id: SemanticMemoryId::from_checkpoint_id(CheckpointId::from_uuid(
            row.get("memory_id"),
        )),
        namespace,
        content: row.get("content"),
        sources,
        metadata,
        revision: to_u64(row.get("revision"))?,
        created_at_ms: to_u64(row.get("created_at_ms"))?,
        updated_at_ms: to_u64(row.get("updated_at_ms"))?,
    })
}

pub(super) fn decode_version(value: i64) -> Result<ConversationVersion, ConversationStoreError> {
    Ok(ConversationVersion::new(to_u64(value)?))
}

pub(super) fn validate_append(command: &ConversationAppend) -> Result<(), ConversationStoreError> {
    if command.messages.is_empty() {
        return Err(invalid_input(
            "conversation append requires at least one message",
        ));
    }
    if command
        .messages
        .iter()
        .any(|message| matches!(message.role, Role::System))
    {
        return Err(invalid_input(
            "system instructions cannot be persisted as conversation transcript",
        ));
    }
    Ok(())
}

pub(super) fn validate_summary(
    command: &ConversationSummaryCommit,
) -> Result<(), ConversationStoreError> {
    if command.content.trim().is_empty() || command.content.len() > MAX_CONTENT_BYTES {
        return Err(invalid_input(
            "conversation summary must contain 1..=262144 bytes",
        ));
    }
    Ok(())
}

pub(super) fn validate_memory(
    command: &SemanticMemoryUpsert,
) -> Result<(), ConversationStoreError> {
    if command.content.trim().is_empty() || command.content.len() > MAX_CONTENT_BYTES {
        return Err(invalid_input(
            "semantic memory must contain 1..=262144 bytes",
        ));
    }
    if command.sources.is_empty()
        || command
            .sources
            .iter()
            .any(|source| source.from_sequence > source.through_sequence)
    {
        return Err(invalid_input(
            "semantic memory requires valid transcript provenance",
        ));
    }
    Ok(())
}

pub(super) fn validate_identifier(identifier: &str) -> Result<(), PostgresConversationStoreError> {
    if identifier.is_empty()
        || identifier.len() > 48
        || !identifier
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || identifier.as_bytes().first().is_none_or(u8::is_ascii_digit)
    {
        return Err(PostgresConversationStoreError::InvalidTable);
    }
    Ok(())
}

pub(super) fn conversation_uuid(id: ConversationId) -> Uuid {
    id.as_checkpoint_id().as_uuid()
}

pub(super) fn memory_uuid(id: SemanticMemoryId) -> Uuid {
    id.as_checkpoint_id().as_uuid()
}

pub(super) fn to_i64(value: u64) -> Result<i64, ConversationStoreError> {
    i64::try_from(value).map_err(|_| invalid_input("numeric value exceeds PostgreSQL BIGINT"))
}

pub(super) fn to_u64(value: i64) -> Result<u64, ConversationStoreError> {
    u64::try_from(value).map_err(|_| {
        ConversationStoreError::new(
            ConversationStoreErrorKind::Storage,
            "PostgreSQL returned an invalid negative numeric value",
        )
    })
}

#[allow(clippy::cast_possible_truncation)]
pub(super) fn to_pgvector(embedding: &Embedding) -> Result<Vector, ConversationStoreError> {
    let values = embedding
        .values()
        .iter()
        .enumerate()
        .map(|(index, value)| {
            if value.abs() <= f64::from(f32::MAX) {
                Ok(*value as f32)
            } else {
                Err(ConversationStoreError::new(
                    ConversationStoreErrorKind::InvalidInput,
                    format!("semantic memory embedding coordinate {index} exceeds f32"),
                ))
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Vector::from(values))
}

pub(super) fn database_usage(started: Instant) -> Usage {
    Usage {
        duration_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        ..Usage::default()
    }
}

pub(super) fn combine_usage(left: Usage, right: Usage) -> Result<Usage, ConversationStoreError> {
    Ok(Usage {
        tokens: left
            .tokens
            .checked_add(right.tokens)
            .ok_or_else(usage_overflow)?,
        cost_microusd: left
            .cost_microusd
            .checked_add(right.cost_microusd)
            .ok_or_else(usage_overflow)?,
        duration_micros: left
            .duration_micros
            .checked_add(right.duration_micros)
            .ok_or_else(usage_overflow)?,
        turns: left
            .turns
            .checked_add(right.turns)
            .ok_or_else(usage_overflow)?,
        tool_calls: left
            .tool_calls
            .checked_add(right.tool_calls)
            .ok_or_else(usage_overflow)?,
        delegations: left
            .delegations
            .checked_add(right.delegations)
            .ok_or_else(usage_overflow)?,
    })
}

fn usage_overflow() -> ConversationStoreError {
    ConversationStoreError::new(
        ConversationStoreErrorKind::Storage,
        "semantic memory usage overflow",
    )
}

pub(super) fn retrieval_error(error: &RetrievalError) -> ConversationStoreError {
    let message = match error {
        RetrievalError::Cancelled => "semantic memory embedding was cancelled",
        RetrievalError::DeadlineExceeded => "semantic memory embedding exceeded its deadline",
        _ => "semantic memory embedding failed",
    };
    ConversationStoreError::new(ConversationStoreErrorKind::Storage, message)
}

pub(super) fn storage_error(_error: tokio_postgres::Error) -> ConversationStoreError {
    ConversationStoreError::new(
        ConversationStoreErrorKind::Storage,
        "PostgreSQL conversation operation failed",
    )
}

pub(super) fn encode_error(_error: serde_json::Error) -> ConversationStoreError {
    invalid_input("conversation value could not be encoded as JSON")
}

fn decode_error(_error: serde_json::Error) -> ConversationStoreError {
    ConversationStoreError::new(
        ConversationStoreErrorKind::Storage,
        "PostgreSQL conversation data could not be decoded",
    )
}

pub(super) fn invalid_input(message: &'static str) -> ConversationStoreError {
    ConversationStoreError::new(ConversationStoreErrorKind::InvalidInput, message)
}

pub(super) fn not_found() -> ConversationStoreError {
    ConversationStoreError::new(
        ConversationStoreErrorKind::NotFound,
        "conversation does not exist",
    )
}

pub(super) fn namespace_mismatch() -> ConversationStoreError {
    ConversationStoreError::new(
        ConversationStoreErrorKind::NamespaceMismatch,
        "conversation resource does not belong to the supplied memory namespace",
    )
}

pub(super) fn conflict_error(message: &'static str) -> ConversationStoreError {
    ConversationStoreError::new(ConversationStoreErrorKind::Conflict, message)
}
