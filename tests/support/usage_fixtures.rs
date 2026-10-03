//! Recorded-provider fixtures shared by the usage-snapshot tests: a fake
//! Claude usage endpoint, a fake Codex app-server control socket, and home
//! directories laid out like a real one.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tungstenite::Message;

/// A fixture bearer token. It must never appear in any reply.
pub const FIXTURE_TOKEN: &str = "fixture-secret-token-7f3a9c";

/// A temporary home directory.
pub struct FixtureHome {
    directory: tempfile::TempDir,
}

impl FixtureHome {
    pub fn new() -> Self {
        Self {
            directory: tempfile::Builder::new()
                .prefix("u")
                .tempdir()
                .expect("temporary home"),
        }
    }

    pub fn path(&self) -> &Path {
        self.directory.path()
    }

    pub fn write(&self, relative: &str, contents: &str) -> PathBuf {
        let path = self.path().join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("directories");
        std::fs::write(&path, contents).expect("write fixture");
        path
    }

    pub fn claude_credentials(&self, expires_at_milliseconds: i64) {
        self.write(
            ".claude/.credentials.json",
            &json!({ "claudeAiOauth": {
                "accessToken": FIXTURE_TOKEN,
                "refreshToken": "fixture-refresh-token-never-used",
                "expiresAt": expires_at_milliseconds,
                "subscriptionType": "max",
                "rateLimitTier": "default_claude_max_20x",
                "scopes": []
            } })
            .to_string(),
        );
    }

    /// A Codex home with a login file; its control socket is bound separately.
    pub fn codex_home(&self, name: &str) -> PathBuf {
        let home = self.path().join(name);
        std::fs::create_dir_all(home.join("app-server-control")).expect("codex home");
        std::fs::write(home.join("auth.json"), "{}").expect("login fixture");
        home
    }

    pub fn codex_socket(&self, name: &str) -> PathBuf {
        self.codex_home(name)
            .join("app-server-control")
            .join("app-server-control.sock")
    }
}

/// What the fake Claude endpoint answers, and what it received.
pub struct FakeClaudeEndpoint {
    pub url: String,
    pub requests: Arc<Mutex<Vec<String>>>,
}

impl FakeClaudeEndpoint {
    pub fn start(status: u16, body: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind endpoint");
        let url = format!(
            "http://{}/api/oauth/usage",
            listener.local_addr().expect("address")
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let mut head = Vec::new();
                let mut byte = [0_u8; 1];
                while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                    head.push(byte[0]);
                }
                recorded
                    .lock()
                    .expect("requests")
                    .push(String::from_utf8_lossy(&head).into_owned());
                let reply = format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _written = stream.write_all(reply.as_bytes());
            }
        });
        Self { url, requests }
    }

    pub fn request_count(&self) -> usize {
        self.requests.lock().expect("requests").len()
    }
}

/// How a fake app-server answers.
#[derive(Clone)]
pub enum AppServerBehavior {
    /// Answers every method from the table; methods absent from it get a
    /// JSON-RPC error.
    Answering(Arc<Vec<(String, Value)>>),
    /// Accepts and upgrades, then never answers.
    Hanging,
}

impl AppServerBehavior {
    pub fn answering(table: Vec<(&str, Value)>) -> Self {
        Self::Answering(Arc::new(
            table
                .into_iter()
                .map(|(method, result)| (method.to_owned(), result))
                .collect(),
        ))
    }

    fn answer(&self, method: &str, parameters: &Value) -> Option<Value> {
        let Self::Answering(table) = self else {
            return None;
        };
        if method == "initialize" {
            return Some(json!({ "result": {} }));
        }
        if method == "thread/read" {
            let thread = parameters
                .get("threadId")
                .and_then(Value::as_str)
                .unwrap_or("");
            let key = format!("thread/read {thread}");
            return Some(table.iter().find(|(name, _)| *name == key).map_or_else(
                || json!({ "error": { "code": -32600, "message": "unknown thread" } }),
                |(_, result)| json!({ "result": result }),
            ));
        }
        Some(table.iter().find(|(name, _)| name == method).map_or_else(
            || json!({ "error": { "code": -32601, "message": "unknown method" } }),
            |(_, result)| json!({ "result": result }),
        ))
    }
}

/// A fake Codex app-server bound to one control socket path.
pub struct FakeAppServer;

impl FakeAppServer {
    pub fn start(socket: &Path, behavior: AppServerBehavior) {
        let listener = UnixListener::bind(socket).expect("bind control socket");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let behavior = behavior.clone();
                std::thread::spawn(move || {
                    let Ok(mut socket) = tungstenite::accept(stream) else {
                        return;
                    };
                    while let Ok(message) = socket.read() {
                        let Message::Text(text) = message else {
                            continue;
                        };
                        let request: Value = serde_json::from_str(text.as_str()).expect("json");
                        let Some(identifier) = request.get("id").cloned() else {
                            continue;
                        };
                        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
                        let parameters = request.get("params").cloned().unwrap_or(Value::Null);
                        let Some(mut reply) = behavior.answer(method, &parameters) else {
                            std::thread::sleep(std::time::Duration::from_secs(30));
                            return;
                        };
                        // A notification first: clients must skip it.
                        let _notified = socket.send(Message::text(
                            json!({ "method": "thread/status/changed", "params": {} }).to_string(),
                        ));
                        reply["id"] = identifier;
                        if socket.send(Message::text(reply.to_string())).is_err() {
                            return;
                        }
                    }
                });
            }
        });
    }

    /// A socket file with nothing listening behind it.
    pub fn stale(socket: &Path) {
        drop(UnixListener::bind(socket).expect("bind stale socket"));
    }
}
