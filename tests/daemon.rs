use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use persona_harness::{
    HarnessCommandLine, HarnessDaemon, HarnessFrameCodec, SocketMode, SupervisionFrameCodec,
};
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Operation, Reply, Request,
    RequestRejectionReason, SessionEpoch, SignalVerb, SubReply,
};
use signal_persona::{
    ComponentHealth, ComponentHealthQuery, ComponentHello, ComponentKind, ComponentName,
    ComponentReadinessQuery, SupervisionFrame, SupervisionFrameBody, SupervisionProtocolVersion,
    SupervisionReply, SupervisionRequest,
};
use signal_persona_harness::{
    HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessHealth, HarnessName, HarnessOperationKind,
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
            "ph-{name}-{}-{}",
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

    fn supervision_socket(&self) -> PathBuf {
        self.root.join("harness-supervision.sock")
    }
}

impl Drop for SocketFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn harness_daemon_applies_spawn_envelope_socket_mode() {
    let fixture = SocketFixture::new("socket-mode");
    let server = HarnessDaemon::from_socket(fixture.socket())
        .with_socket_mode(SocketMode::from_octal(0o600))
        .bind()
        .expect("daemon binds before client connects");

    let mode = std::fs::metadata(server.socket())
        .expect("harness socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode, 0o600);
}

#[test]
fn harness_command_line_requires_socket_path() {
    let error = HarnessCommandLine::from_arguments(std::iter::empty::<&str>())
        .daemon()
        .expect_err("missing socket is typed");

    assert_eq!(error.to_string(), "harness socket path is missing");
}

#[test]
fn harness_frame_codec_rejects_mismatched_signal_verb() {
    let request = Request::from_operations(NonEmpty::single(Operation::new(
        SignalVerb::Assert,
        HarnessRequest::HarnessStatusQuery(HarnessStatusQuery {
            harness: HarnessName::new("operator"),
        }),
    )));
    let frame = HarnessFrame::new(HarnessFrameBody::Request {
        exchange: test_exchange(),
        request,
    });
    let bytes = frame.encode_length_prefixed().expect("frame encodes");
    let mut input = bytes.as_slice();
    let error = HarnessFrameCodec::default()
        .read_request(&mut input)
        .expect_err("mismatched verb is rejected");

    match error {
        persona_harness::Error::InvalidSignalRequest { reason } => {
            assert_eq!(
                reason,
                RequestRejectionReason::VerbPayloadMismatch { index: 0 }
            );
        }
        other => panic!("expected typed signal request rejection, got {other:?}"),
    }
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
fn harness_daemon_answers_component_supervision_relation() {
    let fixture = SocketFixture::new("component-supervision");
    let supervision_socket = fixture.supervision_socket();
    let mut child = Command::new(env!("CARGO_BIN_EXE_persona-harness-daemon"))
        .arg(fixture.socket())
        .arg("operator")
        .env("PERSONA_SOCKET_MODE", "600")
        .env("PERSONA_SUPERVISION_SOCKET_PATH", &supervision_socket)
        .env("PERSONA_SUPERVISION_SOCKET_MODE", "600")
        .spawn()
        .expect("persona-harness-daemon starts");

    wait_for_socket(&supervision_socket);
    let mode = std::fs::metadata(&supervision_socket)
        .expect("supervision socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

    let mut stream = UnixStream::connect(&supervision_socket).expect("client connects");
    let codec = SupervisionFrameCodec::new(1024 * 1024);

    write_supervision_request(
        &mut stream,
        SupervisionRequest::ComponentHello(ComponentHello {
            expected_component: ComponentName::new("persona-harness"),
            expected_kind: ComponentKind::Harness,
            supervision_protocol_version: SupervisionProtocolVersion::new(1),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("identity reply"),
        SupervisionReply::ComponentIdentity(identity)
            if identity.name.as_str() == "persona-harness"
                && identity.kind == ComponentKind::Harness
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::ComponentReadinessQuery(ComponentReadinessQuery {
            component: ComponentName::new("persona-harness"),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("readiness reply"),
        SupervisionReply::ComponentReady(_)
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::ComponentHealthQuery(ComponentHealthQuery {
            component: ComponentName::new("persona-harness"),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("health reply"),
        SupervisionReply::ComponentHealthReport(report)
            if report.health == ComponentHealth::Running
    ));

    stop_child(&mut child);
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
    let frame = HarnessFrame::new(HarnessFrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
    });
    let bytes = frame.encode_length_prefixed().expect("request encodes");
    stream.write_all(&bytes).expect("request writes");
    stream.flush().expect("request flushes");
}

fn write_supervision_request(stream: &mut UnixStream, request: SupervisionRequest) {
    let frame = SupervisionFrame::new(SupervisionFrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
    });
    let bytes = frame
        .encode_length_prefixed()
        .expect("supervision request encodes");
    stream
        .write_all(bytes.as_slice())
        .expect("supervision request writes");
    stream.flush().expect("supervision request flushes");
}

fn wait_for_socket(socket: &PathBuf) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if socket.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("socket was not created: {}", socket.display());
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn read_event(stream: &mut UnixStream) -> HarnessEvent {
    let frame = HarnessFrameCodec::default()
        .read_frame(stream)
        .expect("event frame reads");
    match frame.into_body() {
        HarnessFrameBody::Reply { reply, .. } => match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok { payload, .. } => payload,
                other => panic!("expected ok harness sub-reply, got {other:?}"),
            },
            Reply::Rejected { reason } => panic!("expected harness event reply, got {reason:?}"),
        },
        other => panic!("expected harness event reply, got {other:?}"),
    }
}

fn test_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn unique_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos()
}
