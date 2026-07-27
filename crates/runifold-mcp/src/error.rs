use serde_json::Value;
use thiserror::Error;

/// Stable MCP failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum McpErrorKind {
    /// Underlying transport failed.
    Transport,
    /// JSON encoding or protocol framing failed.
    Protocol,
    /// Peer returned a JSON-RPC error.
    Remote,
    /// Request exceeded its deadline.
    DeadlineExceeded,
    /// Request was cancelled.
    Cancelled,
    /// MCP lifecycle order was invalid.
    Lifecycle,
    /// No mutually supported protocol version exists.
    UnsupportedVersion,
    /// HTTP authentication was rejected.
    Authentication,
    /// The server-side HTTP session no longer exists.
    SessionExpired,
    /// Durable observability rejected an event.
    Observability,
}

/// Typed MCP client, server, and transport failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum McpError {
    /// An I/O transport operation failed.
    #[error("MCP transport failed: {0}")]
    Transport(#[from] std::io::Error),
    /// An HTTP transport operation failed before a protocol response arrived.
    #[error("MCP HTTP transport failed: {0}")]
    Http(#[from] reqwest::Error),
    /// A JSON message could not be encoded or decoded.
    #[error("MCP protocol JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// A protocol invariant was violated.
    #[error("MCP protocol violation: {message}")]
    Protocol {
        /// Safe protocol explanation.
        message: String,
    },
    /// The peer returned a JSON-RPC error.
    #[error("MCP peer returned JSON-RPC error {code}: {message}")]
    Remote {
        /// JSON-RPC error code.
        code: i64,
        /// Peer-provided error message.
        message: String,
        /// Optional structured error data.
        data: Option<Value>,
    },
    /// A request exceeded its effective deadline.
    #[error("MCP request exceeded its deadline")]
    DeadlineExceeded,
    /// A request was cancelled.
    #[error("MCP request was cancelled")]
    Cancelled,
    /// An operation was invalid for the current lifecycle phase.
    #[error("MCP lifecycle violation: {message}")]
    Lifecycle {
        /// Safe lifecycle explanation.
        message: String,
    },
    /// The server selected an unsupported protocol version.
    #[error("unsupported MCP protocol version `{selected}`")]
    UnsupportedVersion {
        /// Version selected by the server.
        selected: String,
    },
    /// The HTTP peer rejected authentication.
    #[error("MCP HTTP authentication was rejected")]
    Authentication,
    /// The HTTP session expired or was deleted.
    #[error("MCP HTTP session expired")]
    SessionExpired,
    /// The HTTP peer returned an unexpected status.
    #[error("MCP HTTP peer returned status {status}: {message}")]
    HttpStatus {
        /// HTTP status code.
        status: u16,
        /// Safe response explanation.
        message: String,
    },
    /// A durable Runifold event could not be recorded.
    #[error("MCP observability failed: {0}")]
    Observability(#[from] runifold_core::JournalError),
}

impl McpError {
    /// Returns the stable failure category.
    pub const fn kind(&self) -> McpErrorKind {
        match self {
            Self::Transport(_) | Self::Http(_) | Self::HttpStatus { .. } => McpErrorKind::Transport,
            Self::Json(_) | Self::Protocol { .. } => McpErrorKind::Protocol,
            Self::Remote { .. } => McpErrorKind::Remote,
            Self::DeadlineExceeded => McpErrorKind::DeadlineExceeded,
            Self::Cancelled => McpErrorKind::Cancelled,
            Self::Lifecycle { .. } => McpErrorKind::Lifecycle,
            Self::UnsupportedVersion { .. } => McpErrorKind::UnsupportedVersion,
            Self::Authentication => McpErrorKind::Authentication,
            Self::SessionExpired => McpErrorKind::SessionExpired,
            Self::Observability(_) => McpErrorKind::Observability,
        }
    }

    pub(crate) fn protocol(message: impl Into<String>) -> Self {
        Self::Protocol {
            message: message.into(),
        }
    }

    pub(crate) fn lifecycle(message: impl Into<String>) -> Self {
        Self::Lifecycle {
            message: message.into(),
        }
    }
}
