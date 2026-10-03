//! A bounded JSON-RPC session with one Codex app-server control socket.
//!
//! The control socket speaks WebSocket over a Unix socket. A session performs
//! the `initialize` / `initialized` exchange once, then answers requests by id,
//! skipping server notifications. Every read and write is under a timeout and
//! every message under a size bound.

use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use serde_json::{Value, json};
use tungstenite::client::IntoClientRequest;
use tungstenite::protocol::WebSocketConfig;
use tungstenite::{Message, WebSocket};

const MESSAGE_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const READ_MESSAGE_LIMIT: usize = 256;

/// The per-request timeout for an app-server exchange.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppServerTimeout {
    request: Duration,
}

impl Default for AppServerTimeout {
    fn default() -> Self {
        Self {
            request: Duration::from_secs(3),
        }
    }
}

impl AppServerTimeout {
    pub fn new(request: Duration) -> Self {
        Self { request }
    }
}

/// Why an app-server exchange produced no result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppServerFailure {
    /// No server is listening at the socket.
    Absent,
    TimedOut,
    TransportFailed,
    /// The server answered the request with a JSON-RPC error.
    Rejected,
    Unreadable,
}

/// One open, initialized app-server session.
pub struct AppServerSession {
    socket: WebSocket<UnixStream>,
    next_identifier: i64,
    timeout: AppServerTimeout,
}

/// The JSON-RPC exchange role: one request, one matching result.
pub trait JsonRpcExchange {
    fn request(
        &mut self,
        method: &str,
        parameters: Option<Value>,
    ) -> Result<Value, AppServerFailure>;
}

impl AppServerFailure {
    fn from_io(error: &std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused => Self::Absent,
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => Self::TimedOut,
            _ => Self::TransportFailed,
        }
    }

    fn from_websocket(error: &tungstenite::Error) -> Self {
        match error {
            tungstenite::Error::Io(error) => Self::from_io(error),
            tungstenite::Error::Capacity(_) => Self::Unreadable,
            _ => Self::TransportFailed,
        }
    }
}

impl AppServerSession {
    /// Connect, upgrade and initialize, all under `timeout`.
    pub fn open(path: &Path, timeout: AppServerTimeout) -> Result<Self, AppServerFailure> {
        let stream =
            UnixStream::connect(path).map_err(|error| AppServerFailure::from_io(&error))?;
        stream
            .set_read_timeout(Some(timeout.request))
            .and_then(|()| stream.set_write_timeout(Some(timeout.request)))
            .map_err(|error| AppServerFailure::from_io(&error))?;
        let request = "ws://localhost/"
            .into_client_request()
            .map_err(|_| AppServerFailure::TransportFailed)?;
        let configuration = WebSocketConfig::default()
            .max_message_size(Some(MESSAGE_BYTE_LIMIT))
            .max_frame_size(Some(MESSAGE_BYTE_LIMIT));
        let (socket, _response) = tungstenite::client::client_with_config(
            request,
            stream,
            Some(configuration),
        )
        .map_err(|error| match error {
            tungstenite::HandshakeError::Failure(error) => AppServerFailure::from_websocket(&error),
            tungstenite::HandshakeError::Interrupted(_) => AppServerFailure::TimedOut,
        })?;
        let mut session = Self {
            socket,
            next_identifier: 1,
            timeout,
        };
        session.request(
            "initialize",
            Some(json!({ "clientInfo": { "name": "harness", "version": env!("CARGO_PKG_VERSION") } })),
        )?;
        session.send(json!({ "method": "initialized", "params": {} }))?;
        Ok(session)
    }

    fn send(&mut self, message: Value) -> Result<(), AppServerFailure> {
        self.socket
            .send(Message::text(message.to_string()))
            .map_err(|error| AppServerFailure::from_websocket(&error))
    }
}

impl JsonRpcExchange for AppServerSession {
    fn request(
        &mut self,
        method: &str,
        parameters: Option<Value>,
    ) -> Result<Value, AppServerFailure> {
        let identifier = self.next_identifier;
        self.next_identifier += 1;
        let mut message = json!({ "id": identifier, "method": method });
        if let Some(parameters) = parameters {
            message["params"] = parameters;
        }
        self.send(message)?;
        let deadline = std::time::Instant::now() + self.timeout.request;
        for _ in 0..READ_MESSAGE_LIMIT {
            if std::time::Instant::now() >= deadline {
                return Err(AppServerFailure::TimedOut);
            }
            let received = self
                .socket
                .read()
                .map_err(|error| AppServerFailure::from_websocket(&error))?;
            let Message::Text(text) = received else {
                continue;
            };
            let Ok(reply) = serde_json::from_str::<Value>(text.as_str()) else {
                return Err(AppServerFailure::Unreadable);
            };
            if reply.get("id").and_then(Value::as_i64) != Some(identifier) {
                continue;
            }
            if reply.get("error").is_some_and(|error| !error.is_null()) {
                return Err(AppServerFailure::Rejected);
            }
            return reply
                .get("result")
                .cloned()
                .ok_or(AppServerFailure::Unreadable);
        }
        Err(AppServerFailure::TimedOut)
    }
}

impl Drop for AppServerSession {
    fn drop(&mut self) {
        let _closed = self.socket.close(None);
        let _flushed = self.socket.flush();
    }
}
