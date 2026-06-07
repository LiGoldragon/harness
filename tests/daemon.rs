use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

use harness::{
    HarnessDaemon, HarnessDaemonCommand, HarnessDaemonConfigurationFile, HarnessFrameCodec,
    HarnessKind, HarnessRuntimeConfiguration, PiRpcDeliveryCommand, PiRpcProcessConfiguration,
    SocketMode, SupervisionFrameCodec,
};
use signal_engine_management::{
    ComponentHealth, ComponentKind, ComponentName, EngineManagementProtocolVersion,
    Frame as SupervisionFrame, FrameBody as SupervisionFrameBody, Operation as SupervisionRequest,
    Presence, Query as SupervisionQuery, Reply as SupervisionReply, SocketMode as WireSocketMode,
    WirePath,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, Request, SessionEpoch,
    SubReply,
};
use signal_harness::{
    DeliveryCompleted, DeliveryFailed, DeliveryFailureReason, HarnessDaemonConfiguration,
    HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessHealth, HarnessInstanceConfiguration,
    HarnessName, HarnessOperationKind, HarnessReadiness, HarnessRequest,
    HarnessRequestUnimplemented, HarnessStatus, HarnessStatusQuery, HarnessUnimplementedReason,
    InteractionPrompt, MessageBody, MessageDelivery, MessageSender, MessageSlot,
};
use signal_persona_origin::{OwnerIdentity, UnixUserIdentifier};
use signal_terminal::{
    TerminalFrame, TerminalFrameBody, TerminalGeneration, TerminalInputAccepted, TerminalReply,
    TerminalRequest,
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
            let codec = TerminalFixtureFrameCodec::default();
            let received_request = codec
                .read_request(&mut stream)
                .expect("terminal socket reads Signal input request");
            match received_request.request {
                TerminalRequest::TerminalInput(input) => {
                    sender
                        .send(input.bytes.as_slice().to_vec())
                        .expect("terminal socket reports bytes");
                    codec
                        .write_reply(
                            &mut stream,
                            received_request.exchange,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TerminalFixtureFrameCodec {
    maximum_frame_bytes: usize,
}

impl TerminalFixtureFrameCodec {
    fn read_frame(&self, stream: &mut UnixStream) -> harness::Result<TerminalFrame> {
        let mut prefix = [0_u8; 4];
        std::io::Read::read_exact(stream, &mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(harness::Error::UnexpectedSignalFrame {
                got: format!("terminal frame length {length} exceeds maximum"),
            });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        std::io::Read::read_exact(stream, &mut bytes[4..])?;
        Ok(TerminalFrame::decode_length_prefixed(bytes.as_slice())?)
    }

    fn read_request(&self, stream: &mut UnixStream) -> harness::Result<ReceivedTerminalRequest> {
        match self.read_frame(stream)?.into_body() {
            TerminalFrameBody::Request { exchange, request } => {
                let (request, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(harness::Error::UnexpectedSignalFrame {
                        got: format!(
                            "expected one terminal request payload, got {}",
                            tail.len() + 1
                        ),
                    });
                }
                Ok(ReceivedTerminalRequest { exchange, request })
            }
            other => Err(harness::Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    fn write_reply(
        &self,
        stream: &mut UnixStream,
        exchange: ExchangeIdentifier,
        reply: TerminalReply,
    ) -> harness::Result<()> {
        let frame = TerminalFrame::new(TerminalFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        let bytes = frame.encode_length_prefixed()?;
        stream.write_all(&bytes)?;
        stream.flush()?;
        Ok(())
    }
}

impl Default for TerminalFixtureFrameCodec {
    fn default() -> Self {
        Self {
            maximum_frame_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReceivedTerminalRequest {
    exchange: ExchangeIdentifier,
    request: TerminalRequest,
}

struct PiRpcFixture {
    root: PathBuf,
    command_path: PathBuf,
    capture_path: PathBuf,
}

impl PiRpcFixture {
    fn new(name: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "ph-pi-rpc-{name}-{}-{}",
            std::process::id(),
            unique_nanos()
        ));
        std::fs::create_dir_all(&root).expect("pi rpc fixture root created");
        let command_path = root.join("pi-rpc-fixture");
        let capture_path = root.join("commands.jsonl");
        let script = "#!/bin/sh\n\
             capture=\"$1\"\n\
             while IFS= read -r line; do\n\
             printf '%s\\n' \"$line\" >> \"$capture\"\n\
             identifier=$(printf '%s\\n' \"$line\" | sed -n 's/.*\"id\":\"\\([^\"]*\\)\".*/\\1/p')\n\
             command=$(printf '%s\\n' \"$line\" | sed -n 's/.*\"type\":\"\\([^\"]*\\)\".*/\\1/p')\n\
             printf '{\"id\":\"%s\",\"type\":\"response\",\"command\":\"%s\",\"success\":true}\\n' \"$identifier\" \"$command\"\n\
             done\n";
        std::fs::write(&command_path, script).expect("pi rpc fixture script writes");
        std::fs::set_permissions(&command_path, std::fs::Permissions::from_mode(0o700))
            .expect("pi rpc fixture script is executable");
        Self {
            root,
            command_path,
            capture_path,
        }
    }

    fn configuration(&self) -> PiRpcProcessConfiguration {
        PiRpcProcessConfiguration::new(&self.command_path, self.root.join("session"))
            .with_command_arguments(vec![self.capture_path.display().to_string()])
            .with_session_name("operator")
            .with_delivery_command(PiRpcDeliveryCommand::Steer)
            .with_response_timeout(Duration::from_secs(5))
    }

    fn captured_command(&self) -> serde_json::Value {
        let text = std::fs::read_to_string(&self.capture_path)
            .expect("pi rpc fixture captured one command");
        let line = text.lines().next().expect("pi rpc command line exists");
        serde_json::from_str(line).expect("pi rpc command is json")
    }
}

impl Drop for PiRpcFixture {
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
fn harness_daemon_accepts_fixture_kind_from_single_binary_configuration_argument() {
    let daemon =
        daemon_from_single_binary_configuration_argument(signal_harness::HarnessKind::Fixture);

    assert_eq!(daemon.kind(), &HarnessKind::Fixture);
}

#[test]
fn harness_daemon_accepts_codex_kind_from_single_binary_configuration_argument() {
    let daemon =
        daemon_from_single_binary_configuration_argument(signal_harness::HarnessKind::Codex);

    assert_eq!(daemon.kind(), &HarnessKind::Codex);
}

#[test]
fn harness_daemon_configuration_rejects_multiple_arguments() {
    let error = HarnessDaemonCommand::from_arguments(["configuration.rkyv", "extra"])
        .configuration()
        .expect_err("multiple arguments are rejected before daemon construction");

    assert!(matches!(
        error,
        harness::Error::Argument(triad_runtime::ArgumentError::ArgumentCount { count: 2 })
    ));
}

#[test]
fn harness_daemon_configuration_rejects_inline_nota_argument() {
    let error = HarnessDaemonCommand::from_arguments(["(HarnessDaemonConfiguration)"])
        .configuration()
        .expect_err("daemon rejects inline NOTA");

    assert!(matches!(
        error,
        harness::Error::Argument(triad_runtime::ArgumentError::ExpectedSignalFile)
    ));
}

#[test]
fn harness_daemon_configuration_rejects_nota_file_argument() {
    let fixture = SocketFixture::new("reject-nota-config");
    let configuration_path = fixture.root.join("harness-daemon.nota");
    std::fs::write(&configuration_path, "(HarnessDaemonConfiguration)").expect("write NOTA file");

    let error = HarnessDaemonCommand::from_arguments([configuration_path.display().to_string()])
        .configuration()
        .expect_err("daemon rejects NOTA files");

    assert!(matches!(
        error,
        harness::Error::Argument(triad_runtime::ArgumentError::ExpectedSignalFile)
    ));
}

fn daemon_from_single_binary_configuration_argument(
    harness_kind: signal_harness::HarnessKind,
) -> HarnessDaemon {
    let fixture = SocketFixture::new("binary-configuration");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    let configuration = HarnessDaemonConfiguration {
        harness_socket_path: WirePath::new("/tmp/harness.sock"),
        harness_socket_mode: WireSocketMode::new(0o600),
        supervision_socket_path: WirePath::new("/tmp/harness.supervision.sock"),
        supervision_socket_mode: WireSocketMode::new(0o600),
        owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(1000)),
        harnesses: vec![configured_instance("operator", harness_kind)],
    };
    HarnessDaemonConfigurationFile::new(configuration_path.clone())
        .write_configuration(&configuration)
        .expect("write binary configuration");
    let decoded = HarnessDaemonCommand::from_arguments([configuration_path.display().to_string()])
        .configuration()
        .expect("binary configuration decodes");

    HarnessDaemon::from_configuration(decoded)
}

fn configured_instance(
    harness_name: &str,
    harness_kind: signal_harness::HarnessKind,
) -> HarnessInstanceConfiguration {
    HarnessInstanceConfiguration {
        harness_name: HarnessName::new(harness_name),
        harness_kind,
        terminal_socket_path: None,
        pi_rpc_adapter: None,
    }
}

#[test]
fn harness_frame_codec_reads_contract_local_request() {
    let request = HarnessRequest::HarnessStatusQuery(HarnessStatusQuery {
        harness: HarnessName::new("operator"),
    });
    let frame = HarnessFrame::new(HarnessFrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request.clone()),
    });
    let bytes = frame.encode_length_prefixed().expect("frame encodes");
    let mut input = bytes.as_slice();
    let received = HarnessFrameCodec::default()
        .read_request(&mut input)
        .expect("contract-local request is read");

    assert_eq!(received.request(), &request);
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
fn harness_daemon_dispatches_two_harness_instances_inside_one_process() {
    let fixture = SocketFixture::new("two-instance-dispatch");
    let operator_terminal = TerminalAcceptanceSocket::new("two-instance-operator");
    let designer_terminal = TerminalAcceptanceSocket::new("two-instance-designer");
    let server = HarnessDaemon::from_socket(fixture.socket())
        .with_harnesses(vec![
            HarnessRuntimeConfiguration::new(HarnessName::new("operator"), HarnessKind::Fixture)
                .with_terminal_socket(operator_terminal.path()),
            HarnessRuntimeConfiguration::new(HarnessName::new("designer"), HarnessKind::Fixture)
                .with_terminal_socket(designer_terminal.path()),
        ])
        .bind()
        .expect("daemon binds before client connects");
    let socket = server.socket().clone();
    let handle = thread::spawn(move || server.serve_requests(2));

    let mut operator_stream = UnixStream::connect(&socket).expect("operator client connects");
    write_request(
        &mut operator_stream,
        MessageDelivery {
            harness: HarnessName::new("operator"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("operator message"),
            message_slot: MessageSlot::new(11),
        }
        .into(),
    );
    let operator_event = read_event(&mut operator_stream);

    let mut designer_stream = UnixStream::connect(socket).expect("designer client connects");
    write_request(
        &mut designer_stream,
        MessageDelivery {
            harness: HarnessName::new("designer"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("designer message"),
            message_slot: MessageSlot::new(12),
        }
        .into(),
    );
    let designer_event = read_event(&mut designer_stream);
    let server_events = handle
        .join()
        .expect("daemon thread joins")
        .expect("daemon handles both requests");

    assert_eq!(
        operator_event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness: HarnessName::new("operator"),
            message_slot: MessageSlot::new(11),
        })
    );
    assert_eq!(
        designer_event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness: HarnessName::new("designer"),
            message_slot: MessageSlot::new(12),
        })
    );
    assert_eq!(server_events, vec![operator_event, designer_event]);
    assert!(
        operator_terminal
            .received_text()
            .contains("operator message")
    );
    assert!(
        designer_terminal
            .received_text()
            .contains("designer message")
    );
}

#[test]
fn harness_daemon_delivers_message_to_pi_rpc_endpoint() {
    let fixture = SocketFixture::new("message-pi-rpc");
    let pi_rpc = PiRpcFixture::new("message-pi-rpc");
    let server = HarnessDaemon::from_socket(fixture.socket())
        .with_harness(HarnessName::new("operator"))
        .with_kind(HarnessKind::Pi)
        .with_pi_rpc_process(pi_rpc.configuration())
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
            body: MessageBody::new("deliver through pi rpc"),
            message_slot: MessageSlot::new(9),
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
        message_slot: MessageSlot::new(9),
    });
    assert_eq!(event, expected);
    assert_eq!(server_event, expected);

    let command = pi_rpc.captured_command();
    assert_eq!(
        command.get("type").and_then(serde_json::Value::as_str),
        Some("steer")
    );
    assert_eq!(
        command.get("message").and_then(serde_json::Value::as_str),
        Some("deliver through pi rpc")
    );
    assert_eq!(
        command.get("id").and_then(serde_json::Value::as_str),
        Some("harness-1")
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
/// `~/primary/reports/designer/188-harness-gap-scan.md` §11.4 and
/// §9 (the "MISSING: Daemon applies spawn-envelope socket mode" row of
/// the constraint-test table) for the supervision socket specifically.
#[test]
fn harness_daemon_applies_distinctive_spawn_envelope_socket_modes() {
    use signal_engine_management::{SocketMode as WireSocketMode, WirePath};
    use signal_harness::{HarnessDaemonConfiguration, HarnessKind as ContractHarnessKind};
    use signal_persona_origin::{OwnerIdentity, UnixUserIdentifier};

    let fixture = SocketFixture::new("distinctive-socket-modes");
    let supervision_socket = fixture.supervision_socket();
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    let configuration = HarnessDaemonConfiguration {
        harness_socket_path: WirePath::new(fixture.socket().display().to_string()),
        harness_socket_mode: WireSocketMode::new(0o640),
        supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
        supervision_socket_mode: WireSocketMode::new(0o660),
        owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(1000)),
        harnesses: vec![configured_instance(
            "operator",
            ContractHarnessKind::Fixture,
        )],
    };
    HarnessDaemonConfigurationFile::new(configuration_path.clone())
        .write_configuration(&configuration)
        .expect("write binary harness config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_harness-daemon"))
        .arg(&configuration_path)
        .spawn()
        .expect("harness-daemon starts");

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
    use signal_engine_management::{SocketMode as WireSocketMode, WirePath};
    use signal_harness::{HarnessDaemonConfiguration, HarnessKind as ContractHarnessKind};
    use signal_persona_origin::{OwnerIdentity, UnixUserIdentifier};

    let fixture = SocketFixture::new("component-supervision");
    let supervision_socket = fixture.supervision_socket();
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    let configuration = HarnessDaemonConfiguration {
        harness_socket_path: WirePath::new(fixture.socket().display().to_string()),
        harness_socket_mode: WireSocketMode::new(0o600),
        supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
        supervision_socket_mode: WireSocketMode::new(0o600),
        owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(1000)),
        harnesses: vec![configured_instance(
            "operator",
            ContractHarnessKind::Fixture,
        )],
    };
    HarnessDaemonConfigurationFile::new(configuration_path.clone())
        .write_configuration(&configuration)
        .expect("write binary harness config");

    let mut child = Command::new(env!("CARGO_BIN_EXE_harness-daemon"))
        .arg(&configuration_path)
        .spawn()
        .expect("harness-daemon starts");

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
            expected_component: ComponentName::new("harness"),
            expected_kind: ComponentKind::Harness,
            engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
        }),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("identity reply"),
        SupervisionReply::Identified(identity)
            if identity.name.as_str() == "harness"
                && identity.kind == ComponentKind::Harness
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::Query(SupervisionQuery::ReadinessStatus(ComponentName::new(
            "harness",
        ))),
    );
    assert!(matches!(
        codec.read_reply(&mut stream).expect("readiness reply"),
        SupervisionReply::Ready(_)
    ));

    write_supervision_request(
        &mut stream,
        SupervisionRequest::Query(SupervisionQuery::HealthStatus(ComponentName::new(
            "harness",
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

fn wait_for_socket(socket: &Path) {
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
                SubReply::Ok(payload) => payload,
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

fn test_supervision_exchange() -> ExchangeIdentifier {
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
