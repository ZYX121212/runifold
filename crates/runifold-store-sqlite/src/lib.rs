//! Durable `SQLite` persistence for Runifold.
//!
//! [`SqliteStore`] implements effect, checkpoint, and journal persistence.
//! [`SqliteWorkflowStore`] implements the complete asynchronous workflow
//! control plane, including fenced leases, budgets, signals, interrupts,
//! checkpoint history, and forks. Both adapters may use the same database file.

mod store;
mod workflow;

pub use store::{SqliteStore, SqliteStoreError};
pub use workflow::{SqliteWorkflowStore, SqliteWorkflowStoreError};
