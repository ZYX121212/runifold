use std::{
    collections::HashMap,
    process::Stdio,
    sync::{Arc, Mutex, MutexGuard},
};

use serde::Serialize;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{Mutex as AsyncMutex, broadcast, oneshot},
    task::JoinSet,
};

use crate::{
    JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, McpError, McpSession, McpTransport,
    PeerRequestHandler, RequestId, ServerNotificationStream, TransportFuture,
    transport::ClientPeerTransport,
};

const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;

/// Serves one MCP session over process stdin and stdout.
///
/// # Errors
///
/// Returns [`McpError`] for framing, JSON, I/O, or request-task failures.
pub async fn serve_stdio(session: McpSession) -> Result<(), McpError> {
    serve_io(
        session,
        BufReader::new(tokio::io::stdin()),
        tokio::io::stdout(),
    )
    .await
}

/// Serves one MCP session over newline-delimited asynchronous I/O.
///
/// This lower-level entry point enables embedded transports and deterministic
/// duplex tests while preserving stdio framing.
///
/// # Errors
///
/// Returns [`McpError`] for framing, JSON, I/O, or request-task failures.
pub async fn serve_io<R, W>(session: McpSession, reader: R, writer: W) -> Result<(), McpError>
where
    R: AsyncBufRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    let mut lines = reader.lines();
    let writer: SharedWriter = Arc::new(AsyncMutex::new(Box::new(writer)));
    let server_pending = PendingMap::default();
    session.install_client_peer(Arc::new(StdioServerPeer {
        writer: Arc::clone(&writer),
        pending: server_pending.clone(),
    }));
    let mut notifications = session.subscribe_notifications();
    let notification_writer = Arc::clone(&writer);
    let notification_task = tokio::spawn(async move {
        use futures_util::StreamExt;
        while let Some(notification) = notifications.next().await {
            write_message(&notification_writer, &notification?).await?;
        }
        Ok::<_, McpError>(())
    });
    let mut requests = JoinSet::new();
    while let Some(line) = lines.next_line().await? {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) else {
            write_response(
                &writer,
                &JsonRpcResponse::error(RequestId::Null, PARSE_ERROR, "parse error", None),
            )
            .await?;
            continue;
        };
        if value.get("method").is_none() {
            let Ok(response) = serde_json::from_value::<JsonRpcResponse>(value) else {
                write_response(
                    &writer,
                    &JsonRpcResponse::error(
                        RequestId::Null,
                        INVALID_REQUEST,
                        "invalid response",
                        None,
                    ),
                )
                .await?;
                continue;
            };
            if let Some(sender) = server_pending.lock().remove(response.id()) {
                let _ = sender.send(Ok(response));
            }
            continue;
        }
        if value.get("id").is_some() {
            let Ok(request) = serde_json::from_value::<JsonRpcRequest>(value) else {
                write_response(
                    &writer,
                    &JsonRpcResponse::error(
                        RequestId::Null,
                        INVALID_REQUEST,
                        "invalid request",
                        None,
                    ),
                )
                .await?;
                continue;
            };
            let session = session.clone();
            let writer = Arc::clone(&writer);
            requests.spawn(async move {
                let response = session.handle_request(request).await;
                write_response(&writer, &response).await
            });
        } else {
            let notification = serde_json::from_value::<JsonRpcNotification>(value)?;
            session.handle_notification(notification)?;
        }
    }
    while let Some(result) = requests.join_next().await {
        result.map_err(|error| McpError::protocol(error.to_string()))??;
    }
    notification_task.abort();
    Ok(())
}

/// Multiplexed stdio client transport.
pub struct StdioTransport {
    writer: SharedWriter,
    pending: PendingMap,
    child: Option<Arc<AsyncMutex<Child>>>,
    notifications: broadcast::Sender<JsonRpcNotification>,
    peer_handler: PeerHandlerSlot,
}

impl StdioTransport {
    /// Spawns one MCP server subprocess with piped stdin and stdout.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the process cannot start or does not expose
    /// piped standard streams.
    pub fn spawn(mut command: Command) -> Result<Self, McpError> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| McpError::protocol("MCP child stdin is unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| McpError::protocol("MCP child stdout is unavailable"))?;
        Ok(Self::from_parts(
            BufReader::new(stdout),
            stdin,
            Some(Arc::new(AsyncMutex::new(child))),
        ))
    }

    /// Creates a stdio-framed transport over arbitrary asynchronous streams.
    pub fn from_io<R, W>(reader: R, writer: W) -> Self
    where
        R: AsyncBufRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        Self::from_parts(reader, writer, None)
    }

    /// Terminates and waits for the child process.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when process termination or waiting fails.
    pub async fn shutdown(&self) -> Result<(), McpError> {
        self.shutdown_with_timeout(std::time::Duration::from_secs(2))
            .await
    }

    /// Gracefully closes stdin, then terminates a child that does not exit
    /// within `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when stream shutdown, process waiting, or forced
    /// termination fails.
    pub async fn shutdown_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), McpError> {
        self.writer.lock().await.shutdown().await?;
        if let Some(child) = &self.child {
            let mut child = child.lock().await;
            if let Ok(status) = tokio::time::timeout(timeout, child.wait()).await {
                status?;
            } else {
                child.start_kill()?;
                child.wait().await?;
            }
        }
        Ok(())
    }

    fn from_parts<R, W>(reader: R, writer: W, child: Option<Arc<AsyncMutex<Child>>>) -> Self
    where
        R: AsyncBufRead + Send + Unpin + 'static,
        W: AsyncWrite + Send + Unpin + 'static,
    {
        let pending = PendingMap::default();
        let (notifications, _) = broadcast::channel(256);
        let peer_handler = PeerHandlerSlot::default();
        let writer = Arc::new(AsyncMutex::new(Box::new(writer) as DynWriter));
        spawn_response_reader(
            reader,
            pending.clone(),
            notifications.clone(),
            Arc::clone(&writer),
            peer_handler.clone(),
        );
        Self {
            writer,
            pending,
            child,
            notifications,
            peer_handler,
        }
    }
}

impl std::fmt::Debug for StdioTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StdioTransport")
            .field("pending_requests", &self.pending.lock().len())
            .finish_non_exhaustive()
    }
}

impl McpTransport for StdioTransport {
    fn request(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse> {
        Box::pin(async move {
            let id = request.id.clone();
            let (sender, receiver) = oneshot::channel();
            if self.pending.lock().insert(id.clone(), sender).is_some() {
                return Err(McpError::protocol("duplicate in-flight request id"));
            }
            let _guard = PendingGuard::new(id, self.pending.clone());
            write_message(&self.writer, &request).await?;
            receiver
                .await
                .map_err(|_| McpError::protocol("MCP response channel closed"))?
        })
    }

    fn notify(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()> {
        Box::pin(async move { write_message(&self.writer, &notification).await })
    }

    fn subscribe(&self) -> TransportFuture<'_, ServerNotificationStream> {
        let mut receiver = self.notifications.subscribe();
        Box::pin(async move {
            Ok(Box::pin(async_stream::stream! {
                loop {
                    match receiver.recv().await {
                        Ok(notification) => yield Ok(notification),
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            yield Err(McpError::protocol(format!(
                                "MCP notification receiver lagged by {skipped} messages"
                            )));
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            }) as ServerNotificationStream)
        })
    }

    fn install_peer_handler(&self, handler: Arc<dyn PeerRequestHandler>) -> Result<(), McpError> {
        let mut installed = self
            .peer_handler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if installed.is_some() {
            return Err(McpError::lifecycle(
                "stdio peer request handler is already installed",
            ));
        }
        *installed = Some(handler);
        Ok(())
    }
}

type PendingSender = oneshot::Sender<Result<JsonRpcResponse, McpError>>;
type DynWriter = Box<dyn AsyncWrite + Send + Unpin>;
type SharedWriter = Arc<AsyncMutex<DynWriter>>;
type PeerHandlerSlot = Arc<Mutex<Option<Arc<dyn PeerRequestHandler>>>>;

struct StdioServerPeer {
    writer: SharedWriter,
    pending: PendingMap,
}

impl ClientPeerTransport for StdioServerPeer {
    fn request_client(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse> {
        Box::pin(async move {
            let id = request.id.clone();
            let (sender, receiver) = oneshot::channel();
            if self.pending.lock().insert(id.clone(), sender).is_some() {
                return Err(McpError::protocol("duplicate in-flight server request id"));
            }
            let _guard = PendingGuard::new(id, self.pending.clone());
            write_message(&self.writer, &request).await?;
            receiver
                .await
                .map_err(|_| McpError::protocol("MCP client response channel closed"))?
        })
    }

    fn notify_client(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()> {
        Box::pin(async move { write_message(&self.writer, &notification).await })
    }
}

#[derive(Clone, Default)]
struct PendingMap {
    inner: Arc<Mutex<HashMap<RequestId, PendingSender>>>,
}

impl PendingMap {
    fn lock(&self) -> MutexGuard<'_, HashMap<RequestId, PendingSender>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn fail_all(&self, message: &str) {
        for (_, sender) in self.lock().drain() {
            let _ = sender.send(Err(McpError::protocol(message)));
        }
    }
}

struct PendingGuard {
    id: RequestId,
    pending: PendingMap,
}

impl PendingGuard {
    fn new(id: RequestId, pending: PendingMap) -> Self {
        Self { id, pending }
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock().remove(&self.id);
    }
}

fn spawn_response_reader<R>(
    reader: R,
    pending: PendingMap,
    notifications: broadcast::Sender<JsonRpcNotification>,
    writer: SharedWriter,
    peer_handler: PeerHandlerSlot,
) where
    R: AsyncBufRead + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        let mut lines = reader.lines();
        loop {
            let line = match lines.next_line().await {
                Ok(Some(line)) => line,
                Ok(None) => {
                    pending.fail_all("MCP server closed stdout");
                    return;
                }
                Err(error) => {
                    pending.fail_all(&format!("MCP stdout failed: {error}"));
                    return;
                }
            };
            let value = match serde_json::from_str::<serde_json::Value>(&line) {
                Ok(value) => value,
                Err(error) => {
                    pending.fail_all(&format!("invalid MCP message: {error}"));
                    return;
                }
            };
            if value.get("method").is_some() && value.get("id").is_none() {
                match serde_json::from_value::<JsonRpcNotification>(value) {
                    Ok(notification) => {
                        let handler = peer_handler
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clone();
                        if let Some(handler) = handler {
                            if handler.notify(notification.clone()).is_err() {
                                continue;
                            }
                        }
                        let _ = notifications.send(notification);
                        continue;
                    }
                    Err(error) => {
                        pending.fail_all(&format!("invalid MCP notification: {error}"));
                        return;
                    }
                }
            }
            if value.get("method").is_some() && value.get("id").is_some() {
                let request = match serde_json::from_value::<JsonRpcRequest>(value) {
                    Ok(request) => request,
                    Err(error) => {
                        pending.fail_all(&format!("invalid MCP peer request: {error}"));
                        return;
                    }
                };
                let Some(handler) = peer_handler
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
                else {
                    let response = JsonRpcResponse::error(
                        request.id,
                        INVALID_REQUEST,
                        "client peer request handler is unavailable",
                        None,
                    );
                    if write_message(&writer, &response).await.is_err() {
                        pending.fail_all("failed to write MCP peer response");
                        return;
                    }
                    continue;
                };
                let writer = Arc::clone(&writer);
                tokio::spawn(async move {
                    let response = handler.handle(request).await.unwrap_or_else(|error| {
                        JsonRpcResponse::error(
                            RequestId::Null,
                            INVALID_REQUEST,
                            error.to_string(),
                            None,
                        )
                    });
                    let _ = write_message(&writer, &response).await;
                });
                continue;
            }
            let response = match serde_json::from_value::<JsonRpcResponse>(value) {
                Ok(response) => response,
                Err(error) => {
                    pending.fail_all(&format!("invalid MCP response: {error}"));
                    return;
                }
            };
            if let Some(sender) = pending.lock().remove(response.id()) {
                let _ = sender.send(Ok(response));
            }
        }
    });
}

async fn write_response<W>(
    writer: &Arc<AsyncMutex<W>>,
    response: &JsonRpcResponse,
) -> Result<(), McpError>
where
    W: AsyncWrite + Send + Unpin + 'static,
{
    write_message(writer, response).await
}

async fn write_message<W, T>(writer: &Arc<AsyncMutex<W>>, message: &T) -> Result<(), McpError>
where
    W: AsyncWrite + Send + Unpin,
    T: Serialize + ?Sized,
{
    let mut encoded = serde_json::to_vec(message)?;
    encoded.push(b'\n');
    let mut writer = writer.lock().await;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}
