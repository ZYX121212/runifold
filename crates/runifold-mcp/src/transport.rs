use std::{future::Future, pin::Pin, sync::Arc};

use futures_util::Stream;

use crate::{JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, McpError, McpTool};

/// Boxed future returned by MCP transport operations.
pub type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, McpError>> + Send + 'a>>;

/// Stream of server-originated MCP notifications.
pub type ServerNotificationStream =
    Pin<Box<dyn Stream<Item = Result<JsonRpcNotification, McpError>> + Send>>;

/// Cancellation binding used by stateless requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatelessCancellation {
    /// Send `notifications/cancelled`, as required by stdio.
    Notification,
    /// Drop or close the request-scoped response stream, as required by HTTP.
    DropRequest,
}

/// Object-safe handler for server-to-client JSON-RPC requests.
pub trait PeerRequestHandler: Send + Sync {
    /// Handles one request initiated by the MCP server.
    fn handle(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse>;

    /// Observes one server notification relevant to an active peer request.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the notification is malformed or cannot be applied.
    fn notify(&self, _notification: JsonRpcNotification) -> Result<(), McpError> {
        Ok(())
    }
}

pub(crate) trait ClientPeerTransport: Send + Sync {
    fn request_client(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse>;

    fn notify_client(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()>;
}

/// Object-safe bidirectional MCP request transport.
pub trait McpTransport: Send + Sync {
    /// Sends one request and waits for the matching response.
    fn request(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse>;

    /// Sends one notification.
    fn notify(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()>;

    /// Returns the transport-specific stateless cancellation binding.
    fn stateless_cancellation(&self) -> StatelessCancellation {
        StatelessCancellation::Notification
    }

    /// Validates and caches transport-specific Tool metadata.
    ///
    /// Non-HTTP transports preserve Tool definitions unchanged.
    /// Applies transport-specific validation and prepares discovered Tools.
    ///
    /// # Errors
    ///
    /// Returns an error when transport metadata cannot be prepared.
    fn prepare_tools(&self, tools: Vec<McpTool>) -> Result<Vec<McpTool>, McpError> {
        Ok(tools)
    }

    /// Opens the server-to-client notification channel.
    fn subscribe(&self) -> TransportFuture<'_, ServerNotificationStream> {
        Box::pin(async {
            Err(McpError::protocol(
                "this MCP transport does not support server notifications",
            ))
        })
    }

    /// Opens a modern `subscriptions/listen` request stream.
    fn listen(&self, _request: JsonRpcRequest) -> TransportFuture<'_, ServerNotificationStream> {
        Box::pin(async {
            Err(McpError::protocol(
                "this MCP transport does not support subscriptions/listen",
            ))
        })
    }

    /// Installs the single server-to-client request handler for this connection.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when peer requests are unsupported or a handler is already installed.
    fn install_peer_handler(&self, _handler: Arc<dyn PeerRequestHandler>) -> Result<(), McpError> {
        Err(McpError::protocol(
            "this MCP transport does not support server-to-client requests",
        ))
    }

    /// Starts transport-specific peer request delivery after initialization.
    fn start_peer(&self) -> TransportFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}
