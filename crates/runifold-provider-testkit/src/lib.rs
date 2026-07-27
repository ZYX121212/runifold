//! Offline HTTP cassette and fault-injection support for provider adapters.
//!
//! The server intentionally speaks HTTP/1.1 over a loopback socket. This keeps
//! provider tests independent from a production HTTP stack while still
//! exercising real request serialization, response streaming, delays, and
//! mid-stream disconnects.

use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// One expected request and its scripted response.
#[derive(Clone, Debug)]
pub struct HttpExchange {
    /// Expected uppercase HTTP method.
    pub method: String,
    /// Expected path, including a query string when present.
    pub path: String,
    /// Expected JSON body. `None` accepts any request body.
    pub json_body: Option<Value>,
    /// Response returned after the request matches.
    pub response: ScriptedResponse,
}

impl HttpExchange {
    /// Creates an exchange that accepts any request body.
    pub fn new(
        method: impl Into<String>,
        path: impl Into<String>,
        response: ScriptedResponse,
    ) -> Self {
        Self {
            method: method.into().to_ascii_uppercase(),
            path: path.into(),
            json_body: None,
            response,
        }
    }

    /// Requires an exact JSON request body.
    #[must_use]
    pub fn with_json_body(mut self, body: Value) -> Self {
        self.json_body = Some(body);
        self
    }
}

/// One response body fragment and the delay before it is sent.
#[derive(Clone, Debug, Default)]
pub struct ResponseChunk {
    /// Bytes written as one HTTP chunk.
    pub body: Vec<u8>,
    /// Artificial delay before writing the chunk.
    pub delay: Duration,
}

impl ResponseChunk {
    /// Creates an immediate UTF-8 response chunk.
    pub fn text(body: impl Into<String>) -> Self {
        Self {
            body: body.into().into_bytes(),
            delay: Duration::ZERO,
        }
    }

    /// Delays this chunk.
    #[must_use]
    pub const fn after(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

/// A streamed HTTP response, optionally disconnected before its terminator.
#[derive(Clone, Debug)]
pub struct ScriptedResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers.
    pub headers: BTreeMap<String, String>,
    /// Ordered chunked-transfer body fragments.
    pub chunks: Vec<ResponseChunk>,
    /// Close the socket without a final zero-sized chunk.
    pub disconnect: bool,
}

impl ScriptedResponse {
    /// Creates a successful response.
    pub fn ok(chunks: Vec<ResponseChunk>) -> Self {
        Self {
            status: 200,
            headers: BTreeMap::new(),
            chunks,
            disconnect: false,
        }
    }

    /// Creates a JSON response.
    ///
    /// # Errors
    ///
    /// Returns an error when the JSON body cannot be serialized.
    pub fn json(status: u16, body: &Value) -> Result<Self, CassetteError> {
        let encoded = serde_json::to_vec(body)?;
        let mut headers = BTreeMap::new();
        headers.insert("content-type".into(), "application/json".into());
        Ok(Self {
            status,
            headers,
            chunks: vec![ResponseChunk {
                body: encoded,
                delay: Duration::ZERO,
            }],
            disconnect: false,
        })
    }

    /// Adds or replaces a response header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Disconnects after the configured chunks, simulating a broken stream.
    #[must_use]
    pub const fn disconnect_after_chunks(mut self) -> Self {
        self.disconnect = true;
        self
    }
}

/// A request captured by the cassette server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObservedRequest {
    /// HTTP method.
    pub method: String,
    /// Request target.
    pub path: String,
    /// Lowercase headers with credentials replaced by `[REDACTED]`.
    pub headers: BTreeMap<String, String>,
    /// Raw request body.
    pub body: Vec<u8>,
}

impl ObservedRequest {
    /// Parses the request body as JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if the captured body is not valid JSON.
    pub fn json_body(&self) -> Result<Value, CassetteError> {
        Ok(serde_json::from_slice(&self.body)?)
    }
}

/// Errors produced by cassette construction, serving, or verification.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CassetteError {
    /// A loopback socket operation failed.
    #[error("cassette I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization or validation failed.
    #[error("cassette JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    /// A request did not match its scripted exchange.
    #[error("cassette request mismatch: {0}")]
    RequestMismatch(String),
    /// The server thread panicked.
    #[error("cassette server thread panicked")]
    ServerPanicked,
    /// Not every scripted exchange was consumed.
    #[error("cassette consumed {observed} of {expected} exchanges")]
    Incomplete {
        /// Number of captured requests.
        observed: usize,
        /// Number of scripted exchanges.
        expected: usize,
    },
}

/// A loopback server executing a fixed sequence of HTTP exchanges.
#[derive(Debug)]
pub struct CassetteServer {
    address: SocketAddr,
    expected: usize,
    observed: Arc<Mutex<Vec<ObservedRequest>>>,
    failure: Arc<Mutex<Option<String>>>,
    stats: Arc<ServerCounters>,
    shutdown: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

/// Snapshot of cassette-server concurrency and completion counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ServerStats {
    /// Connections accepted for scripted exchanges.
    pub accepted: usize,
    /// Responses that completed without a server-side write error.
    pub completed: usize,
    /// Largest number of simultaneously active handlers.
    pub max_in_flight: usize,
}

#[derive(Debug, Default)]
struct ServerCounters {
    accepted: AtomicUsize,
    completed: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
}

impl CassetteServer {
    /// Starts an offline loopback server.
    ///
    /// # Errors
    ///
    /// Returns an error when the loopback listener cannot be created.
    pub fn start(exchanges: Vec<HttpExchange>) -> Result<Self, CassetteError> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let expected = exchanges.len();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let failure = Arc::new(Mutex::new(None));
        let stats = Arc::new(ServerCounters::default());
        let shutdown = Arc::new(AtomicBool::new(false));

        let thread_observed = Arc::clone(&observed);
        let thread_failure = Arc::clone(&failure);
        let thread_stats = Arc::clone(&stats);
        let thread_shutdown = Arc::clone(&shutdown);
        let thread = thread::spawn(move || {
            serve(
                &listener,
                exchanges,
                &thread_observed,
                &thread_failure,
                &thread_stats,
                &thread_shutdown,
            );
        });

        Ok(Self {
            address,
            expected,
            observed,
            failure,
            stats,
            shutdown,
            thread: Some(thread),
        })
    }

    /// Starts a server which accepts `request_count` concurrent copies of one
    /// exchange.
    ///
    /// This mode is intended for connection-pool and request-isolation stress
    /// tests where arrival order is deliberately nondeterministic.
    ///
    /// # Errors
    ///
    /// Returns an error when the loopback listener cannot be created.
    pub fn start_repeating(
        exchange: HttpExchange,
        request_count: usize,
    ) -> Result<Self, CassetteError> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let observed = Arc::new(Mutex::new(Vec::new()));
        let failure = Arc::new(Mutex::new(None));
        let stats = Arc::new(ServerCounters::default());
        let shutdown = Arc::new(AtomicBool::new(false));

        let thread_observed = Arc::clone(&observed);
        let thread_failure = Arc::clone(&failure);
        let thread_stats = Arc::clone(&stats);
        let thread_shutdown = Arc::clone(&shutdown);
        let thread = thread::spawn(move || {
            serve_repeating(
                &listener,
                &exchange,
                request_count,
                &thread_observed,
                &thread_failure,
                &thread_stats,
                &thread_shutdown,
            );
        });

        Ok(Self {
            address,
            expected: request_count,
            observed,
            failure,
            stats,
            shutdown,
            thread: Some(thread),
        })
    }

    /// Returns a base URL suitable for provider configuration.
    pub fn base_url(&self) -> String {
        format!("http://{}/", self.address)
    }

    /// Returns a snapshot of all captured requests.
    pub fn observed_requests(&self) -> Vec<ObservedRequest> {
        self.observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Returns current server counters.
    pub fn stats(&self) -> ServerStats {
        ServerStats {
            accepted: self.stats.accepted.load(Ordering::Acquire),
            completed: self.stats.completed.load(Ordering::Acquire),
            max_in_flight: self.stats.max_in_flight.load(Ordering::Acquire),
        }
    }

    /// Verifies that all exchanges matched and were consumed.
    ///
    /// # Errors
    ///
    /// Returns a mismatch, server failure, or incomplete-cassette error.
    pub fn assert_finished(&self) -> Result<(), CassetteError> {
        if let Some(message) = self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
        {
            return Err(CassetteError::RequestMismatch(message));
        }
        let observed = self
            .observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        if observed != self.expected {
            return Err(CassetteError::Incomplete {
                observed,
                expected: self.expected,
            });
        }
        Ok(())
    }
}

impl Drop for CassetteServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn serve(
    listener: &TcpListener,
    exchanges: Vec<HttpExchange>,
    observed: &Mutex<Vec<ObservedRequest>>,
    failure: &Mutex<Option<String>>,
    stats: &ServerCounters,
    shutdown: &AtomicBool,
) {
    for exchange in exchanges {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let Ok((mut stream, _)) = listener.accept() else {
            set_failure(failure, "failed to accept a connection");
            return;
        };
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        request_started(stats);
        let request = match read_request(&mut stream) {
            Ok(request) => request,
            Err(error) => {
                set_failure(failure, error.to_string());
                request_finished(stats, false);
                return;
            }
        };
        let mismatch = validate_request(&request, &exchange);
        observed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(redact(request));
        if let Err(error) = mismatch {
            set_failure(failure, error.to_string());
            let _ = write_simple_error(&mut stream);
            request_finished(stats, false);
            return;
        }
        if let Err(error) = write_response(&mut stream, &exchange.response) {
            set_failure(failure, error.to_string());
            request_finished(stats, false);
            return;
        }
        request_finished(stats, true);
    }
}

fn serve_repeating(
    listener: &TcpListener,
    exchange: &HttpExchange,
    request_count: usize,
    observed: &Arc<Mutex<Vec<ObservedRequest>>>,
    failure: &Arc<Mutex<Option<String>>>,
    stats: &Arc<ServerCounters>,
    shutdown: &AtomicBool,
) {
    let mut workers = Vec::with_capacity(request_count);
    for _ in 0..request_count {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let Ok((stream, _)) = listener.accept() else {
            set_failure(failure, "failed to accept a connection");
            break;
        };
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let exchange = exchange.clone();
        let observed = Arc::clone(observed);
        let failure = Arc::clone(failure);
        let stats = Arc::clone(stats);
        workers.push(thread::spawn(move || {
            handle_repeated(stream, &exchange, &observed, &failure, &stats);
        }));
    }
    for worker in workers {
        if worker.join().is_err() {
            set_failure(failure, "cassette connection handler panicked");
        }
    }
}

fn handle_repeated(
    mut stream: TcpStream,
    exchange: &HttpExchange,
    observed: &Mutex<Vec<ObservedRequest>>,
    failure: &Mutex<Option<String>>,
    stats: &ServerCounters,
) {
    request_started(stats);
    let request = match read_request(&mut stream) {
        Ok(request) => request,
        Err(error) => {
            set_failure(failure, error.to_string());
            request_finished(stats, false);
            return;
        }
    };
    let mismatch = validate_request(&request, exchange);
    observed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(redact(request));
    if let Err(error) = mismatch {
        set_failure(failure, error.to_string());
        let _ = write_simple_error(&mut stream);
        request_finished(stats, false);
        return;
    }
    match write_response(&mut stream, &exchange.response) {
        Ok(()) => request_finished(stats, true),
        Err(error) => {
            set_failure(failure, error.to_string());
            request_finished(stats, false);
        }
    }
}

fn request_started(stats: &ServerCounters) {
    stats.accepted.fetch_add(1, Ordering::AcqRel);
    let current = stats.in_flight.fetch_add(1, Ordering::AcqRel) + 1;
    stats.max_in_flight.fetch_max(current, Ordering::AcqRel);
}

fn request_finished(stats: &ServerCounters, completed: bool) {
    stats.in_flight.fetch_sub(1, Ordering::AcqRel);
    if completed {
        stats.completed.fetch_add(1, Ordering::AcqRel);
    }
}

fn read_request(stream: &mut TcpStream) -> Result<ObservedRequest, CassetteError> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut buffer = [0_u8; 4096];
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(CassetteError::RequestMismatch(
                "connection closed before HTTP headers completed".into(),
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
        if let Some(position) = find_subslice(&bytes, b"\r\n\r\n") {
            break position + 4;
        }
        if bytes.len() > 1024 * 1024 {
            return Err(CassetteError::RequestMismatch(
                "request headers exceeded 1 MiB".into(),
            ));
        }
    };

    let header_text = std::str::from_utf8(&bytes[..header_end]).map_err(|_| {
        CassetteError::RequestMismatch("request headers were not valid UTF-8".into())
    })?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| CassetteError::RequestMismatch("missing request line".into()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = required_part(&mut request_parts, "method")?.to_owned();
    let path = required_part(&mut request_parts, "request target")?.to_owned();
    let mut headers = BTreeMap::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line.split_once(':').ok_or_else(|| {
            CassetteError::RequestMismatch(format!("malformed request header `{line}`"))
        })?;
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    let content_length = headers.get("content-length").map_or(Ok(0), |value| {
        value
            .parse::<usize>()
            .map_err(|_| CassetteError::RequestMismatch("invalid content-length header".into()))
    })?;
    while bytes.len() - header_end < content_length {
        let mut buffer = [0_u8; 4096];
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(CassetteError::RequestMismatch(
                "connection closed before request body completed".into(),
            ));
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
    Ok(ObservedRequest {
        method,
        path,
        headers,
        body: bytes[header_end..header_end + content_length].to_vec(),
    })
}

fn required_part<'a>(
    parts: &mut impl Iterator<Item = &'a str>,
    name: &str,
) -> Result<&'a str, CassetteError> {
    parts
        .next()
        .ok_or_else(|| CassetteError::RequestMismatch(format!("request line is missing {name}")))
}

fn validate_request(
    request: &ObservedRequest,
    exchange: &HttpExchange,
) -> Result<(), CassetteError> {
    if request.method != exchange.method {
        return Err(CassetteError::RequestMismatch(format!(
            "expected method {}, received {}",
            exchange.method, request.method
        )));
    }
    if request.path != exchange.path {
        return Err(CassetteError::RequestMismatch(format!(
            "expected path {}, received {}",
            exchange.path, request.path
        )));
    }
    if let Some(expected) = &exchange.json_body {
        let actual = request.json_body()?;
        if actual != *expected {
            return Err(CassetteError::RequestMismatch(format!(
                "JSON body differs: expected {expected}, received {actual}"
            )));
        }
    }
    Ok(())
}

fn redact(mut request: ObservedRequest) -> ObservedRequest {
    for name in ["authorization", "x-api-key", "x-goog-api-key", "api-key"] {
        if let Some(value) = request.headers.get_mut(name) {
            *value = "[REDACTED]".into();
        }
    }
    request
}

fn write_response(
    stream: &mut TcpStream,
    response: &ScriptedResponse,
) -> Result<(), CassetteError> {
    let reason = reason_phrase(response.status);
    write!(
        stream,
        "HTTP/1.1 {} {}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n",
        response.status, reason
    )?;
    for (name, value) in &response.headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    stream.flush()?;
    for chunk in &response.chunks {
        if !chunk.delay.is_zero() {
            thread::sleep(chunk.delay);
        }
        write!(stream, "{:X}\r\n", chunk.body.len())?;
        stream.write_all(&chunk.body)?;
        write!(stream, "\r\n")?;
        stream.flush()?;
    }
    if !response.disconnect {
        write!(stream, "0\r\n\r\n")?;
        stream.flush()?;
    }
    Ok(())
}

fn write_simple_error(stream: &mut TcpStream) -> Result<(), CassetteError> {
    stream.write_all(
        b"HTTP/1.1 500 Cassette Mismatch\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    )?;
    Ok(())
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        408 => "Request Timeout",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Scripted",
    }
}

fn set_failure(failure: &Mutex<Option<String>>, message: impl Into<String>) {
    *failure
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(message.into());
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpStream,
    };

    use serde_json::json;

    use super::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};

    #[test]
    fn captures_json_and_redacts_credentials() {
        let server = CassetteServer::start(vec![
            HttpExchange::new(
                "POST",
                "/messages",
                ScriptedResponse::ok(vec![ResponseChunk::text("hello")]),
            )
            .with_json_body(json!({"prompt": "hi"})),
        ])
        .unwrap();
        let mut stream = TcpStream::connect(server.address).unwrap();
        stream
            .write_all(
                b"POST /messages HTTP/1.1\r\nHost: localhost\r\nX-Api-Key: secret\r\nContent-Length: 15\r\n\r\n{\"prompt\":\"hi\"}",
            )
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();

        assert!(response.contains("5\r\nhello\r\n"));
        server.assert_finished().unwrap();
        let observed = server.observed_requests();
        assert_eq!(observed[0].headers["x-api-key"], "[REDACTED]");
        assert_eq!(observed[0].json_body().unwrap(), json!({"prompt": "hi"}));
    }
}
