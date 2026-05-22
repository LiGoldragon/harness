use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

use nota_config::ConfigurationSource;
use persona_harness::{
    HarnessDaemon, HarnessFrameCodec, HarnessKind, SocketMode, SupervisionFrameCodec,
};
use persona_terminal::supervisor::TerminalSupervisorFrameCodec;
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Operation, Reply, Request,
    RequestRejectionReason, SessionEpoch, SignalVerb, SubReply,
};
use signal_frame::{
    ExchangeIdentifier as FrameExchangeIdentifier, ExchangeLane as FrameExchangeLane,
    LaneSequence as FrameLaneSequence, Request as FrameRequest, SessionEpoch as FrameSessionEpoch,
};
use signal_persona::engine_management::{
    Frame as SupervisionFrame, FrameBody as SupervisionFrameBody, Operation as SupervisionRequest,
    Query as SupervisionQuery, Reply as SupervisionReply,
};
use signal_persona::{
    ComponentHealth, ComponentKind, ComponentName, EngineManagementProtocolVersion, Presence,
    SocketMode as WireSocketMode, WirePath,
};
use signal_persona_auth::{OwnerIdentity, UnixUserId};
use signal_persona_harness::{
    DeliveryCompleted, DeliveryFailed, DeliveryFailureReason, HarnessDaemonConfiguration,
    HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessHealth, HarnessName, HarnessOperationKind,
    HarnessReadiness, HarnessRequest, HarnessRequestUnimplemented, HarnessStatus,
    HarnessStatusQuery, HarnessUnimplementedReason, InteractionPrompt, MessageBody,
    MessageDelivery, MessageSender, MessageSlot,
};
use signal_persona_terminal::{
    TerminalGeneration, TerminalInputAccepted, TerminalReply, TerminalRequest,
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

struct TerminalAcceptanceSocket {
    path: PathBuf,
    received: Receiver<Vec<u8>>,
}

impl TerminalAcceptanceSocket {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ph-terminal-{name}-{}-{}.sock",
            std::process::id(),
            unique_nanos()
        ));
        let listener = UnixListener::bind(&path).expect("terminal acceptance socket binds");
        let (sender, received) = channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("terminal socket accepts input");
            let codec = TerminalSupervisorFrameCodec::default();
            let request = codec
                .read_request(&mut stream)
                .expect("terminal socket reads Signal input request");
            match request {
                TerminalRequest::TerminalInput(input) => {
                    sender
                        .send(input.bytes.as_slice().to_vec())
                        .expect("terminal socket reports bytes");
                    codec
                        .write_reply(
                            &mut stream,
                            TerminalReply::TerminalInputAccepted(TerminalInputAccepted {
                                terminal: input.terminal,
                                generation: TerminalGeneration::new(1),
                            }),
                        )
                        .expect("terminal socket writes Signal acceptance");
                }
                other => panic!("expected TerminalInput request, got {other:?}"),
            }
        });
        Self { path, received }
    }

    fn path(&self) -> &PathBuf {
        &self.path
    }

    fn received_text(&self) -> String {
        String::from_utf8(
            self.received
                .recv_timeout(Duration::from_secs(5))
                .expect("terminal socket receives input bytes"),
        )
        .expect("terminal input is utf8")
    }
}

impl Drop for TerminalAcceptanceSocket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
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
fn harness_daemon_accepts_fixture_kind_from_single_nota_configuration_argument() {
    let daemon = daemon_from_single_nota_configuration_argument(
        signal_persona_harness::HarnessKind::Fixture,
    );

    assert_eq!(daemon.kind(), &HarnessKind::Fixture);
}

#[test]
fn harness_daemon_accepts_codex_kind_from_single_nota_configuration_argument() {
    let daemon =
        daemon_from_single_nota_configuration_argument(signal_persona_harness::HarnessKind::Codex);

    assert_eq!(daemon.kind(), &HarnessKind::Codex);
}

#[test]
fn harness_daemon_configuration_rejects_multiple_arguments() {
    let error = ConfigurationSource::from_args(["(HarnessDaemonConfiguration)", "extra"])
        .expect_err("multiple arguments are rejected before daemon construction");

    assert!(matches!(error, nota_config::Error::MultipleArguments(2)));
}

fn daemon_from_single_nota_configuration_argument(
    harness_kind: signal_persona_harness::HarnessKind,
) -> HarnessDaemon {
    use nota_codec::{Encoder, NotaEncode};

    let configuration = HarnessDaemonConfiguration {
        harness_socket_path: WirePath::new("/tmp/harness.sock"),
        harness_socket_mode: WireSocketMode::new(0o600),
        supervision_socket_path: WirePath::new("/tmp/harness.supervision.sock"),
        supervision_socket_mode: WireSocketMode::new(0o600),
        harness_name: HarnessName::new("operator"),
        harness_kind,
        terminal_socket_path: None,
        owner_identity: OwnerIdentity::UnixUser(UnixUserId::new(1000)),
    };
    let mut encoder = Encoder::new();
    configuration
        .encode(&mut encoder)
        .expect("configuration encodes");
    let text = encoder.into_string();
    let decoded: HarnessDaemonConfiguration = ConfigurationSource::from_args([text.as_str()])
        .expect("single inline NOTA argument is accepted")
        .decode()
        .expect("configuration decodes");

    HarnessDaemon::from_configuration(decoded)
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
fn harness_daemon_delivers_message_to_terminal_endpoint() {
    let fixture = SocketFixture::new("message-delivery");
    let terminal = TerminalAcceptanceSocket::new("message-delivery");
    let server = HarnessDaemon::from_socket(fixture.socket())
        .with_harness(HarnessName::new("operator"))
        .with_terminal_socket(terminal.path())
        .bind()
        .expect("daemon binds before client connects");
    let socket = server.socket().clone();
    let handle = thread::spawn(move || server.serve_one());

    let mut stream = UnixStream::connect(socket).expect("client connects");
    write_request(
        &mut stream,
        MessageDelivery {
            harness: HarnessName::new("operator"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("deliver through harness daemon"),
            message_slot: MessageSlot::new(7),
        }
        .into(),
    );
    let event = read_event(&mut stream);
    let server_event = handle
        .join()
        .expect("daemon thread joins")
        .expect("daemon handles one request");

    let expected = HarnessEvent::DeliveryCompleted(DeliveryCompleted {
        harness: HarnessName::new("operator"),
        message_slot: MessageSlot::new(7),
    });
    assert_eq!(event, expected);
    assert_eq!(server_event, expected);
    assert!(
        terminal
            .received_text()
            .contains("deliver through harness daemon")
    );
}

#[test]
fn harness_daemon_rejects_message_delivery_without_terminal_endpoint() {
    let fixture = SocketFixture::new("message-no-terminal");
    let server = HarnessDaemon::from_socket(fixture.socket())
        .with_harness(HarnessName::new("operator"))
        .bind()
        .expect("daemon binds before client connects");
    let socket = server.socket().clone();
    let handle = thread::spawn(move || server.serve_one());

    let mut stream = UnixStream::connect(socket).expect("client connects");
    write_request(
        &mut stream,
        MessageDelivery {
            harness: HarnessName::new("operator"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("cannot deliver without terminal"),
            message_slot: MessageSlot::new(8),
        }
        .into(),
    );
    let event = read_event(&mut stream);
    let server_event = handle
        .join()
        .expect("daemon thread joins")
        .expect("daemon handles one request");

    let expected = HarnessEvent::DeliveryFailed(DeliveryFailed {
        harness: HarnessName::new("operator"),
        message_slot: MessageSlot::new(8),
        reason: DeliveryFailureReason::TransportRejected,
    });
    assert_eq!(event, expected);
    assert_eq!(server_event, expected);
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

/// Witnesses that the spawn-envelope socket-mode env vars flow through to
/// both the domain harness socket *and* the supervision socket as chmod
/// calls — not as hardcoded constants. Uses distinctive non-default modes
/// (`0o640` and `0o660`) so a regression that pins either chmod to a
/// fixed value fails this assertion.
///
/// Closes the witness gap recorded in
/// `~/primary/reports/designer/188-persona-harness-gap-scan.md` §11.4 and
/// §9 (the "MISSING: Daemon applies spawn-envelope socket mode" row of
/// the constraint-test table) for the supervision socket specifically.
#[test]
fn harness_daemon_applies_distinctive_spawn_envelope_socket_modes() {
    use nota_codec::{Encoder, NotaEncode};
    use signal_persona::{SocketMode as WireSocketMode, WirePath};
    use signal_persona_auth::{OwnerIdentity, UnixUserId};
    use signal_persona_harness::{
        HarnessDaemonConfiguration, HarnessKind as ContractHarnessKind, HarnessName,
    };

    let fixture = SocketFixture::new("distinctive-socket-modes");
    let supervision_socket = fixture.supervision_socket();
    let configuration_path = fixture.root.join("harness-daemon.nota");
    let configuration = HarnessDaemonConfiguration {
        harness_socket_path: WirePath::new(fixture.socket().display().to_string()),
        harness_socket_mode: WireSocketMode::new(0o640),
        supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
        supervision_socket_mode: WireSocketMode::new(0o660),
        harness_name: HarnessName::new("operator"),
        harness_kind: ContractHarnessKind::Fixture,
        terminal_socket_path: None,
        owner_identity: OwnerIdentity::UnixUser(UnixUserId::new(1000)),
    };
    let mut encoder = Encoder::new();
    configuration
        .encode(&mut encoder)
        .expect("encode harness config");
    let mut text = encoder.into_string();
    text.push('\n');
    std::fs::write(&configuration_path, text).expect("write harness config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_persona-harness-daemon"))
        .arg(&configuration_path)
        .spawn()
        .expect("persona-harness-daemon starts");

    wait_for_socket(fixture.socket());
    wait_for_socket(&supervision_socket);

    let domain_mode = std::fs::metadata(fixture.socket())
        .expect("domain socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;
    let supervision_mode = std::fs::metadata(&supervision_socket)
        .expect("supervision socket metadata is readable")
        .permissions()
        .mode()
        & 0o777;

    stop_child(&mut child);

    assert_eq!(
        domain_mode, 0o640,
        "domain socket mode did not pick up the configuration socket mode",
    );
    assert_eq!(
        supervision_mode, 0o660,
        "supervision socket mode did not pick up the configuration socket mode",
    );
}

#[test]
fn harness_daemon_answers_component_supervision_relation() {
    use nota_codec::{Encoder, NotaEncode};
    use signal_persona::{SocketMode as WireSocketMode, WirePath};
    use signal_persona_auth::{OwnerIdentity, UnixUserId};
    use signal_persona_harness::{
        HarnessDaemonConfiguration, HarnessKind as ContractHarnessKind, HarnessName,
    };

    let fixture = SocketFixture::new("component-supervision");
    let supervision_socket = fixture.supervision_socket();
    let configuration_path = fixture.root.join("harness-daemon.nota");
    let configuration = HarnessDaemonConfiguration {
        harness_socket_path: WirePath::new(fixture.socket().display().to_string()),
        harness_socket_mode: WireSocketMode::new(0o600),
        supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
        supervision_socket_mode: WireSocketMode::new(0o600),
        harness_name: HarnessName::new("operator"),
        harness_kind: ContractHarnessKind::Fixture,
        terminal_socket_path: None,
        owner_identity: OwnerIdentity::UnixUser(UnixUserId::new(1000)),
    };
    let mut encoder = Encoder::new();
    configuration
        .encode(&mut encoder)
        .expect("encode harness config");
    let mut text = encoder.into_string();
    text.push('\n');
    std::fs::write(&configuration_path, text).expect("write harness config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_persona-harness-daemon"))
        .arg(&configuration_path)
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
        SupervisionRequest::Announce(Presence {
            expected_component: ComponentName::new("persona-harness"),
            expected_kind: ComponentKind::Harness,
            engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("identity reply"),
        SupervisionReply::Identified(identity)
            if identity.name.as_str() == "persona-harness"
                && identity.kind == ComponentKind::Harness
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::Query(SupervisionQuery::ReadinessStatus(ComponentName::new(
            "persona-harness",
        ))),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("readiness reply"),
        SupervisionReply::Ready(_)
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::Query(SupervisionQuery::HealthStatus(ComponentName::new(
            "persona-harness",
        ))),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("health reply"),
        SupervisionReply::HealthReport(report)
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
        exchange: test_supervision_exchange(),
        request: FrameRequest::from_payload(request),
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

fn test_supervision_exchange() -> FrameExchangeIdentifier {
    FrameExchangeIdentifier::new(
        FrameSessionEpoch::new(0),
        FrameExchangeLane::Connector,
        FrameLaneSequence::first(),
    )
}

fn unique_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos()
}
