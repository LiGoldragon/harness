use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::thread;

use persona_harness::{HarnessCommandLine, HarnessDaemon, HarnessFrameCodec};
use signal_core::{FrameBody, Reply, Request};
use signal_persona_harness::{
    Frame as HarnessFrame, HarnessEvent, HarnessHealth, HarnessName, HarnessOperationKind,
    HarnessReadiness, HarnessRequest, HarnessRequestUnimplemented, HarnessStatus,
    HarnessStatusQuery, HarnessUnimplementedReason, InteractionPrompt,
};

struct SocketFixture {
    root: PathBuf,
    socket: PathBuf,
}

impl SocketFixture {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "persona-harness-{name}-{}-{}",
            std::process::id(),
            unique_nanos()
        ));
        let socket = root.join("harness.sock");
        std::fs::create_dir_all(&root).expect("fixture root created");
        Self { root, socket }
    }

    fn socket(&self) -> &PathBuf {
        &self.socket
    }
}

impl Drop for SocketFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn harness_command_line_requires_socket_path() {
    let error = HarnessCommandLine::from_arguments(std::iter::empty::<&str>())
        .daemon()
        .expect_err("missing socket is typed");

    assert_eq!(error.to_string(), "harness socket path is missing");
}

#[test]
fn harness_daemon_answers_status_readiness() {
    let fixture = SocketFixture::new("status");
    let server = HarnessDaemon::from_socket(fixture.socket())
        .with_harness(HarnessName::new("operator"))
        .bind()
        .expect("daemon binds before client connects");
    let socket = server.socket().clone();
    let handle = thread::spawn(move || server.serve_one());

    let mut stream = UnixStream::connect(socket).expect("client connects");
    write_request(
        &mut stream,
        HarnessStatusQuery {
            harness: HarnessName::new("operator"),
        }
        .into(),
    );
    let event = read_event(&mut stream);
    let server_event = handle
        .join()
        .expect("daemon thread joins")
        .expect("daemon handles one request");

    let expected = HarnessEvent::HarnessStatus(HarnessStatus {
        harness: HarnessName::new("operator"),
        health: HarnessHealth::Running,
        readiness: HarnessReadiness::Ready,
    });
    assert_eq!(event, expected);
    assert_eq!(server_event, expected);
}

#[test]
fn harness_daemon_returns_typed_unimplemented() {
    let fixture = SocketFixture::new("unimplemented");
    let server = HarnessDaemon::from_socket(fixture.socket())
        .with_harness(HarnessName::new("operator"))
        .bind()
        .expect("daemon binds before client connects");
    let socket = server.socket().clone();
    let handle = thread::spawn(move || server.serve_one());

    let mut stream = UnixStream::connect(socket).expect("client connects");
    write_request(
        &mut stream,
        InteractionPrompt {
            harness: HarnessName::new("operator"),
            interaction_id: "interaction-1".to_string(),
            prompt: "Approve?".to_string(),
            options: vec!["yes".to_string(), "no".to_string()],
        }
        .into(),
    );
    let event = read_event(&mut stream);
    let server_event = handle
        .join()
        .expect("daemon thread joins")
        .expect("daemon handles one request");

    let expected = HarnessEvent::HarnessRequestUnimplemented(HarnessRequestUnimplemented {
        harness: HarnessName::new("operator"),
        operation: HarnessOperationKind::InteractionPrompt,
        reason: HarnessUnimplementedReason::NotBuiltYet,
    });
    assert_eq!(event, expected);
    assert_eq!(server_event, expected);
}

fn write_request(stream: &mut UnixStream, request: HarnessRequest) {
    let frame = HarnessFrame::new(FrameBody::Request(Request::assert(request)));
    let bytes = frame.encode_length_prefixed().expect("request encodes");
    stream.write_all(&bytes).expect("request writes");
    stream.flush().expect("request flushes");
}

fn read_event(stream: &mut UnixStream) -> HarnessEvent {
    let frame = HarnessFrameCodec::default()
        .read_frame(stream)
        .expect("event frame reads");
    match frame.into_body() {
        FrameBody::Reply(Reply::Operation(event)) => event,
        other => panic!("expected harness event reply, got {other:?}"),
    }
}

fn unique_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos()
}
