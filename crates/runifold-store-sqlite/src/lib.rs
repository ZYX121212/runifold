//! Durable `SQLite` persistence for Runifold.
//!
//! One [`SqliteStore`] implements effect, checkpoint, and journal persistence
//! so applications can share a single local database across runtime layers.

mod store;

pub use store::{SqliteStore, SqliteStoreError};
