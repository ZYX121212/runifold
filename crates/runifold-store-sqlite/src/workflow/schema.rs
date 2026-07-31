pub(super) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS runifold_workflow_state (
    singleton_id INTEGER PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
    format_version INTEGER NOT NULL CHECK (format_version > 0),
    state_blob BLOB NOT NULL,
    updated_at_ms INTEGER NOT NULL CHECK (updated_at_ms >= 0)
);
";

pub(super) const SNAPSHOT_FORMAT_VERSION: i64 = 1;
