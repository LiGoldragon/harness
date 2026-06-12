//! End-to-end coverage for the schema-emitted `harness-daemon` shell.
//!
//! The daemon binds three listener tiers through the generated
//! `AsyncMultiListenerDaemon` shell: the ordinary working socket speaks the
//! `signal-harness` `HarnessFrame` contract (component-decoded), and the
//! owner-only supervision socket speaks the `signal-persona` engine-management
//! `Frame`. Each tier rides a length-prefixed envelope: the daemon shell's
//! `LengthPrefixedCodec` frames one bare contract frame per body.
//!
//! Every test spawns the real `harness-daemon` binary against a written binary
//! `HarnessDaemonConfiguration`, since the daemon's async listener shell is the
//! product surface — there is no in-process synchronous serve loop anymore.

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::mpsc::{Receiver, channel};
use std::thread;
use std::time::{Duration, Instant};

use harness::HarnessDaemonConfigurationFile;
use meta_signal_harness::{
    MetaHarnessFrame, MetaHarnessFrameBody, MetaHarnessReply, MetaHarnessRequest,
    RequestUnimplemented as MetaRequestUnimplemented,
    UnimplementedReason as MetaUnimplementedReason,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, Request, SessionEpoch, SubReply,
};
use signal_harness::{
    DeliveryCompleted, DeliveryFailed, DeliveryFailureReason, HarnessDaemonConfiguration,
    HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessHealth, HarnessInstanceConfiguration,
    HarnessKind as ContractHarnessKind, HarnessName, HarnessOperationKind, HarnessReadiness,
    HarnessRequest, HarnessRequestUnimplemented, HarnessStatus, HarnessStatusQuery,
    HarnessUnimplementedReason, InteractionPrompt, MessageBody, MessageDelivery, MessageSender,
    MessageSlot,
};
use signal_persona::origin::{OwnerIdentity, UnixUserIdentifier};
use signal_persona::{
    ComponentHealth, ComponentKind, ComponentName, EngineManagementProtocolVersion,
    Frame as SupervisionFrame, FrameBody as SupervisionFrameBody, Operation as SupervisionRequest,
    Presence, Query as SupervisionQuery, Reply as SupervisionReply, SocketMode as WireSocketMode,
    WirePath,
};
use signal_terminal::{
    Frame as TerminalFrame, FrameBody as TerminalFrameBody, Input as TerminalInputRoot,
    Output as TerminalOutput, TerminalGeneration, TerminalInputAccepted,
};

const MAXIMUM_FRAME_BYTES: usize = 1024 * 1024;

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

/// A spawned `harness-daemon` process bound to one fixture's sockets. Built from
/// a written binary configuration; cleans up the child on drop.
struct SpawnedHarnessDaemon {
    child: Child,
}

impl SpawnedHarnessDaemon {
    fn spawn(configuration_path: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_harness-daemon"))
            .arg(configuration_path)
            .spawn()
            .expect("harness-daemon starts");
        Self { child }
    }
}

impl Drop for SpawnedHarnessDaemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A terminal acceptance socket that answers one `TerminalInput` request with a
/// `TerminalInputAccepted` reply over the new schema-derived `signal-terminal`
/// wire, reporting the delivered bytes back to the test.
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
            let received_request =
                read_terminal_request(&mut stream).expect("terminal socket reads Signal input");
            match received_request.request {
                TerminalInputRoot::TerminalInput(input) => {
                    let bytes = input
                        .bytes
                        .payload()
                        .iter()
                        .map(|byte| *byte as u8)
                        .collect::<Vec<u8>>();
                    sender.send(bytes).expect("terminal socket reports bytes");
                    write_terminal_reply(
                        &mut stream,
                        received_request.exchange,
                        TerminalOutput::TerminalInputAccepted(TerminalInputAccepted {
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

struct ReceivedTerminalRequest {
    exchange: ExchangeIdentifier,
    request: TerminalInputRoot,
}

fn read_terminal_request(stream: &mut UnixStream) -> Option<ReceivedTerminalRequest> {
    let frame = read_length_prefixed_frame(stream)?;
    match TerminalFrame::decode_length_prefixed(&frame)
        .ok()?
        .into_body()
    {
        TerminalFrameBody::Request { exchange, request } => {
            let (request, _tail) = request.payloads.into_head_and_tail();
            Some(ReceivedTerminalRequest { exchange, request })
        }
        _ => None,
    }
}

fn write_terminal_reply(
    stream: &mut UnixStream,
    exchange: ExchangeIdentifier,
    output: TerminalOutput,
) -> std::io::Result<()> {
    let frame = output.into_reply_frame(exchange);
    let bytes = frame
        .encode_length_prefixed()
        .expect("terminal reply frame encodes");
    stream.write_all(&bytes)?;
    stream.flush()
}

/// Reads one length-prefixed frame body off a blocking `UnixStream`. The
/// terminal acceptance socket speaks the same prefix the daemon shell does, so
/// this matches `signal-terminal`'s own length-prefixed framing.
fn read_length_prefixed_frame(stream: &mut UnixStream) -> Option<Vec<u8>> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).ok()?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAXIMUM_FRAME_BYTES {
        return None;
    }
    let mut bytes = Vec::with_capacity(4 + length);
    bytes.extend_from_slice(&prefix);
    bytes.resize(4 + length, 0);
    stream.read_exact(&mut bytes[4..]).ok()?;
    Some(bytes)
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
        let session_directory = root.join("session");
        let capture_path = session_directory.join("commands.jsonl");
        // The harness daemon spawns the Pi adapter with `--session-dir <dir>`
        // (the `signal-harness` contract carries no extra argv), so the fixture
        // derives its capture file from that flag rather than a positional arg.
        let script = "#!/bin/sh\n\
             session_dir=.\n\
             while [ $# -gt 0 ]; do\n\
             case \"$1\" in\n\
             --session-dir) session_dir=\"$2\"; shift 2 ;;\n\
             *) shift ;;\n\
             esac\n\
             done\n\
             capture=\"$session_dir/commands.jsonl\"\n\
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

    fn command_path(&self) -> &Path {
        &self.command_path
    }

    fn session_directory(&self) -> PathBuf {
        self.root.join("session")
    }

    fn captured_command(&self) -> serde_json::Value {
        let text = wait_for_capture(&self.capture_path);
        let line = text.lines().next().expect("pi rpc command line exists");
        serde_json::from_str(line).expect("pi rpc command is json")
    }
}

impl Drop for PiRpcFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn wait_for_capture(path: &Path) -> String {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if let Ok(text) = std::fs::read_to_string(path)
            && !text.trim().is_empty()
        {
            return text;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("pi rpc fixture captured no command: {}", path.display());
}

#[test]
fn harness_daemon_binds_working_socket_with_configured_mode() {
    let fixture = SocketFixture::new("socket-mode");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture)
            .with_harness_socket_mode(0o600)
            .build(vec![fixture_instance("operator").build()]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);

    wait_for_socket(fixture.socket());
    let mode = socket_mode(fixture.socket());

    assert_eq!(mode, 0o600);
}

/// The working socket mode flows from the configured spawn envelope, while the
/// owner-only supervision (meta) socket is owner-only by the daemon shape — the
/// emitted shell binds the meta tier at a compile-time `0o600` regardless of the
/// configured supervision mode. Uses a distinctive non-default working mode
/// (`0o640`) so a regression that pins the working chmod to a fixed value fails.
#[test]
fn harness_daemon_applies_configured_working_socket_mode_and_owner_only_supervision() {
    let fixture = SocketFixture::new("distinctive-socket-modes");
    let supervision_socket = fixture.supervision_socket();
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture)
            .with_harness_socket_mode(0o640)
            .with_supervision_socket_mode(0o660)
            .build(vec![fixture_instance("operator").build()]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);

    wait_for_socket(fixture.socket());
    wait_for_socket(&supervision_socket);

    assert_eq!(
        socket_mode(fixture.socket()),
        0o640,
        "working socket mode did not pick up the configuration socket mode",
    );
    assert_eq!(
        socket_mode(&supervision_socket),
        0o600,
        "supervision socket is owner-only by the daemon shape, not the config",
    );
}

#[test]
fn harness_daemon_delivers_message_to_terminal_endpoint() {
    let fixture = SocketFixture::new("message-delivery");
    let terminal = TerminalAcceptanceSocket::new("message-delivery");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            fixture_instance("operator")
                .with_terminal_socket_path(terminal.path())
                .build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(fixture.socket());

    let mut stream = UnixStream::connect(fixture.socket()).expect("client connects");
    write_working_request(
        &mut stream,
        MessageDelivery {
            harness: HarnessName::new("operator"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("deliver through harness daemon"),
            message_slot: MessageSlot::new(7),
        }
        .into(),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness: HarnessName::new("operator"),
            message_slot: MessageSlot::new(7),
        })
    );
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
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            fixture_instance("operator")
                .with_terminal_socket_path(operator_terminal.path())
                .build(),
            fixture_instance("designer")
                .with_terminal_socket_path(designer_terminal.path())
                .build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(fixture.socket());

    let mut operator_stream = UnixStream::connect(fixture.socket()).expect("operator connects");
    write_working_request(
        &mut operator_stream,
        MessageDelivery {
            harness: HarnessName::new("operator"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("operator message"),
            message_slot: MessageSlot::new(11),
        }
        .into(),
    );
    let operator_event = read_working_event(&mut operator_stream);

    let mut designer_stream = UnixStream::connect(fixture.socket()).expect("designer connects");
    write_working_request(
        &mut designer_stream,
        MessageDelivery {
            harness: HarnessName::new("designer"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("designer message"),
            message_slot: MessageSlot::new(12),
        }
        .into(),
    );
    let designer_event = read_working_event(&mut designer_stream);

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
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            HarnessInstanceConfigurationBuilder::new("operator", ContractHarnessKind::Pi)
                .with_pi_rpc(&pi_rpc)
                .build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(fixture.socket());

    let mut stream = UnixStream::connect(fixture.socket()).expect("client connects");
    write_working_request(
        &mut stream,
        MessageDelivery {
            harness: HarnessName::new("operator"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("deliver through pi rpc"),
            message_slot: MessageSlot::new(9),
        }
        .into(),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness: HarnessName::new("operator"),
            message_slot: MessageSlot::new(9),
        })
    );

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
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![fixture_instance("operator").build()]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(fixture.socket());

    let mut stream = UnixStream::connect(fixture.socket()).expect("client connects");
    write_working_request(
        &mut stream,
        MessageDelivery {
            harness: HarnessName::new("operator"),
            sender: MessageSender::new("router"),
            body: MessageBody::new("cannot deliver without terminal"),
            message_slot: MessageSlot::new(8),
        }
        .into(),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::DeliveryFailed(DeliveryFailed {
            harness: HarnessName::new("operator"),
            message_slot: MessageSlot::new(8),
            reason: DeliveryFailureReason::TransportRejected,
        })
    );
}

#[test]
fn harness_daemon_answers_status_readiness() {
    let fixture = SocketFixture::new("status");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![fixture_instance("operator").build()]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(fixture.socket());

    let mut stream = UnixStream::connect(fixture.socket()).expect("client connects");
    write_working_request(
        &mut stream,
        HarnessStatusQuery {
            harness: HarnessName::new("operator"),
        }
        .into(),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::HarnessStatus(HarnessStatus {
            harness: HarnessName::new("operator"),
            health: HarnessHealth::Running,
            readiness: HarnessReadiness::Ready,
        })
    );
}

#[test]
fn harness_daemon_returns_typed_unimplemented() {
    let fixture = SocketFixture::new("unimplemented");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![fixture_instance("operator").build()]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(fixture.socket());

    let mut stream = UnixStream::connect(fixture.socket()).expect("client connects");
    write_working_request(
        &mut stream,
        InteractionPrompt {
            harness: HarnessName::new("operator"),
            interaction_id: "interaction-1".to_string(),
            prompt: "Approve?".to_string(),
            options: vec!["yes".to_string(), "no".to_string()],
        }
        .into(),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::HarnessRequestUnimplemented(HarnessRequestUnimplemented {
            harness: HarnessName::new("operator"),
            operation: HarnessOperationKind::InteractionPrompt,
            reason: HarnessUnimplementedReason::NotBuiltYet,
        })
    );
}

#[test]
fn harness_daemon_answers_meta_harness_relation_with_typed_unimplemented() {
    let fixture = SocketFixture::new("meta-harness");
    let supervision_socket = fixture.supervision_socket();
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    let configuration = DaemonConfigurationBuilder::new(&fixture)
        .with_supervision_socket_mode(0o600)
        .build(vec![fixture_instance("operator").build()]);
    write_configuration(&configuration_path, configuration.clone());
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);

    wait_for_socket(&supervision_socket);

    let mut stream =
        UnixStream::connect(&supervision_socket).expect("meta-harness client connects");
    write_meta_harness_request(&mut stream, MetaHarnessRequest::Configure(configuration));
    let reply = read_meta_harness_reply(&mut stream);

    assert_eq!(
        reply,
        MetaHarnessReply::RequestUnimplemented(MetaRequestUnimplemented {
            operation: meta_signal_harness::OperationKind::Configure,
            reason: MetaUnimplementedReason::NotBuiltYet,
        })
    );
}

#[test]
fn harness_daemon_answers_component_supervision_relation() {
    let fixture = SocketFixture::new("component-supervision");
    let supervision_socket = fixture.supervision_socket();
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture)
            .with_supervision_socket_mode(0o600)
            .build(vec![fixture_instance("operator").build()]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);

    wait_for_socket(&supervision_socket);
    assert_eq!(socket_mode(&supervision_socket), 0o600);

    // The emitted meta tier serves one engine-management request per accepted
    // connection, so each exchange opens a fresh supervision connection.
    assert!(matches!(
        supervision_exchange(
            &supervision_socket,
            SupervisionRequest::Announce(Presence {
                expected_component: ComponentName::new("harness"),
                expected_kind: ComponentKind::Harness,
                engine_management_protocol_version: EngineManagementProtocolVersion::new(1),
            }),
        ),
        SupervisionReply::Identified(identity)
            if identity.name.as_str() == "harness"
                && identity.kind == ComponentKind::Harness
    ));

    assert!(matches!(
        supervision_exchange(
            &supervision_socket,
            SupervisionRequest::Query(SupervisionQuery::ReadinessStatus(ComponentName::new(
                "harness",
            ))),
        ),
        SupervisionReply::Ready(_)
    ));

    assert!(matches!(
        supervision_exchange(
            &supervision_socket,
            SupervisionRequest::Query(SupervisionQuery::HealthStatus(ComponentName::new(
                "harness",
            ))),
        ),
        SupervisionReply::HealthReport(report)
            if report.health == ComponentHealth::Running
    ));
}

/// One owner-only supervision exchange: open a fresh connection, send one
/// engine-management request, read its reply, and drop the connection. The
/// emitted meta tier serves exactly one request per accepted connection.
fn supervision_exchange(socket: &Path, request: SupervisionRequest) -> SupervisionReply {
    let mut stream = UnixStream::connect(socket).expect("supervision client connects");
    write_supervision_request(&mut stream, request);
    read_supervision_reply(&mut stream)
}

/// Builds a binary `HarnessDaemonConfiguration` against one fixture's sockets.
struct DaemonConfigurationBuilder {
    harness_socket_path: WirePath,
    harness_socket_mode: u32,
    supervision_socket_path: WirePath,
    supervision_socket_mode: u32,
}

impl DaemonConfigurationBuilder {
    fn new(fixture: &SocketFixture) -> Self {
        Self {
            harness_socket_path: WirePath::new(fixture.socket().display().to_string()),
            harness_socket_mode: 0o600,
            supervision_socket_path: WirePath::new(
                fixture.supervision_socket().display().to_string(),
            ),
            supervision_socket_mode: 0o600,
        }
    }

    fn with_harness_socket_mode(mut self, mode: u32) -> Self {
        self.harness_socket_mode = mode;
        self
    }

    fn with_supervision_socket_mode(mut self, mode: u32) -> Self {
        self.supervision_socket_mode = mode;
        self
    }

    fn build(self, harnesses: Vec<HarnessInstanceConfiguration>) -> HarnessDaemonConfiguration {
        HarnessDaemonConfiguration {
            harness_socket_path: self.harness_socket_path,
            harness_socket_mode: WireSocketMode::new(self.harness_socket_mode),
            supervision_socket_path: self.supervision_socket_path,
            supervision_socket_mode: WireSocketMode::new(self.supervision_socket_mode),
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(1000)),
            harnesses,
        }
    }
}

/// Builds one harness instance configuration record.
struct HarnessInstanceConfigurationBuilder {
    harness_name: HarnessName,
    harness_kind: ContractHarnessKind,
    terminal_socket_path: Option<WirePath>,
    pi_rpc_adapter: Option<signal_harness::PiRpcJsonlAdapterConfiguration>,
}

impl HarnessInstanceConfigurationBuilder {
    fn new(harness_name: &str, harness_kind: ContractHarnessKind) -> Self {
        Self {
            harness_name: HarnessName::new(harness_name),
            harness_kind,
            terminal_socket_path: None,
            pi_rpc_adapter: None,
        }
    }

    fn with_terminal_socket_path(mut self, path: &Path) -> Self {
        self.terminal_socket_path = Some(WirePath::new(path.display().to_string()));
        self
    }

    fn with_pi_rpc(mut self, fixture: &PiRpcFixture) -> Self {
        self.pi_rpc_adapter = Some(signal_harness::PiRpcJsonlAdapterConfiguration {
            command_path: WirePath::new(fixture.command_path().display().to_string()),
            session_directory_path: WirePath::new(
                fixture.session_directory().display().to_string(),
            ),
            delivery_mode: signal_harness::PiRpcDeliveryMode::Steer,
            model_pattern: None,
        });
        self
    }

    fn build(self) -> HarnessInstanceConfiguration {
        HarnessInstanceConfiguration {
            harness_name: self.harness_name,
            harness_kind: self.harness_kind,
            terminal_socket_path: self.terminal_socket_path,
            pi_rpc_adapter: self.pi_rpc_adapter,
        }
    }
}

fn fixture_instance(harness_name: &str) -> HarnessInstanceConfigurationBuilder {
    HarnessInstanceConfigurationBuilder::new(harness_name, ContractHarnessKind::Fixture)
}

fn write_configuration(path: &Path, configuration: HarnessDaemonConfiguration) {
    HarnessDaemonConfigurationFile::new(path.to_path_buf())
        .write_configuration(&configuration)
        .expect("write binary harness configuration");
}

/// Writes one working harness request through the daemon shell's length-prefixed
/// envelope: a single bare `HarnessFrame` per body.
fn write_working_request(stream: &mut UnixStream, request: HarnessRequest) {
    let frame = HarnessFrame::new(HarnessFrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
    });
    write_length_prefixed(stream, &frame.encode().expect("harness request encodes"));
}

fn read_working_event(stream: &mut UnixStream) -> HarnessEvent {
    let body = read_length_prefixed_frame(stream).expect("event frame reads");
    let frame = HarnessFrame::decode(&body).expect("event frame decodes");
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

fn write_meta_harness_request(stream: &mut UnixStream, request: MetaHarnessRequest) {
    let frame = MetaHarnessFrame::new(MetaHarnessFrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
    });
    write_length_prefixed(
        stream,
        &frame.encode().expect("meta-harness request encodes"),
    );
}

fn read_meta_harness_reply(stream: &mut UnixStream) -> MetaHarnessReply {
    let body = read_length_prefixed_frame(stream).expect("meta-harness reply reads");
    let frame = MetaHarnessFrame::decode(&body).expect("meta-harness reply decodes");
    match frame.into_body() {
        MetaHarnessFrameBody::Reply { reply, .. } => match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(payload) => payload,
                other => panic!("expected ok meta-harness sub-reply, got {other:?}"),
            },
            Reply::Rejected { reason } => panic!("expected meta-harness reply, got {reason:?}"),
        },
        other => panic!("expected meta-harness reply, got {other:?}"),
    }
}

fn write_supervision_request(stream: &mut UnixStream, request: SupervisionRequest) {
    let frame = SupervisionFrame::new(SupervisionFrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
    });
    write_length_prefixed(
        stream,
        &frame.encode().expect("supervision request encodes"),
    );
}

fn read_supervision_reply(stream: &mut UnixStream) -> SupervisionReply {
    let body = read_length_prefixed_frame(stream).expect("supervision reply reads");
    let frame = SupervisionFrame::decode(&body).expect("supervision reply decodes");
    match frame.into_body() {
        SupervisionFrameBody::Reply { reply, .. } => match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(payload) => payload,
                other => panic!("expected ok supervision sub-reply, got {other:?}"),
            },
            Reply::Rejected { reason } => panic!("expected supervision reply, got {reason:?}"),
        },
        other => panic!("expected supervision reply, got {other:?}"),
    }
}

fn write_length_prefixed(stream: &mut UnixStream, body: &[u8]) {
    let length = u32::try_from(body.len()).expect("frame body fits a u32 length prefix");
    stream
        .write_all(&length.to_be_bytes())
        .expect("length prefix writes");
    stream.write_all(body).expect("frame body writes");
    stream.flush().expect("frame flushes");
}

fn socket_mode(socket: &Path) -> u32 {
    std::fs::metadata(socket)
        .expect("socket metadata is readable")
        .permissions()
        .mode()
        & 0o777
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
