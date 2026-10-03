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

use harness::{HarnessDaemonConfigurationFile, HarnessEngine, SignalWire};
use meta_signal_harness::{
    MetaOperationKind, Query as MetaHarnessRequest,
    RequestUnimplemented as MetaRequestUnimplemented, Response as MetaHarnessReply,
    UnimplementedReason as MetaUnimplementedReason,
};
use signal_harness::{
    CapabilityProfile, ClaudeSessionIdentifier, ContinuationHandle, ContinuationRequest,
    DeliveryCompleted, DeliveryFailed, DeliveryFailureReason, EffortRequest,
    HarnessDaemonConfiguration, HarnessHealth, HarnessInstanceConfiguration,
    HarnessKind as ContractHarnessKind, HarnessName, HarnessOperationKind, HarnessReadiness,
    HarnessRequestUnimplemented, HarnessStatus, HarnessStatusQuery, HarnessStreamEvent,
    HarnessTranscriptToken, HarnessUnimplementedReason, InteractionPrompt, MessageDelivery,
    ModelRequest, ModelResolutionRequest, ModelResolved, ModelSelector, ModelUnavailable,
    ModelUnavailableReason, NamedModel, PiContinuationIdentifier, Query as HarnessRequest,
    Response as HarnessEvent, TranscriptObservation, WatchHarnessTranscript,
};
use signal_persona::{
    ComponentHealth, ComponentKind, LifecycleQuery, OwnerIdentity, Presence,
    Query as SupervisionRequest, Response as SupervisionReply,
};
use signal_terminal::{
    ByteViewable, Query as TerminalInputRoot, Response as TerminalOutput, Restorable, Signal,
    Signalizable, TerminalInputAcceptedReply,
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

    fn meta_socket(&self) -> PathBuf {
        self.root.join("meta-harness.sock")
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
            match received_request {
                TerminalInputRoot::TerminalInput(input) => {
                    let bytes = input
                        .input_bytes
                        .iter()
                        .map(|byte| *byte as u8)
                        .collect::<Vec<u8>>();
                    sender.send(bytes).expect("terminal socket reports bytes");
                    write_terminal_reply(
                        &mut stream,
                        TerminalOutput::TerminalInputAccepted(TerminalInputAcceptedReply {
                            terminal: input.terminal,
                            generation: 1,
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

fn read_terminal_request(stream: &mut UnixStream) -> Option<TerminalInputRoot> {
    let bytes = read_length_prefixed_frame(stream)?;
    Signal::<TerminalInputRoot>::from(bytes).restore().ok()
}

fn write_terminal_reply(stream: &mut UnixStream, output: TerminalOutput) -> std::io::Result<()> {
    let bytes = output
        .signalize()
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .bytes()
        .to_vec();
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
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

/// The working and supervision socket modes flow from the configuration,
/// while the meta socket is owner-only by the daemon shape — the emitted shell
/// binds the meta tier at a compile-time `0o600`. Distinctive non-default modes
/// (`0o640`, `0o660`) make a regression that pins either chmod fail.
#[test]
fn harness_daemon_applies_configured_socket_modes_and_owner_only_meta() {
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
    wait_for_socket(&fixture.meta_socket());

    assert_eq!(
        socket_mode(fixture.socket()),
        0o640,
        "working socket mode did not pick up the configuration socket mode",
    );
    assert_eq!(
        socket_mode(&supervision_socket),
        0o660,
        "supervision socket mode did not pick up the configuration socket mode",
    );
    assert_eq!(
        socket_mode(&fixture.meta_socket()),
        0o600,
        "meta socket is owner-only by the daemon shape",
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
        HarnessRequest::MessageDelivery(MessageDelivery {
            harness_name: HarnessName::from("operator"),
            message_sender: "router".into(),
            message_body: "deliver through harness daemon".into(),
            message_slot: 7,
        }),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness_name: HarnessName::from("operator"),
            message_slot: 7,
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
        HarnessRequest::MessageDelivery(MessageDelivery {
            harness_name: HarnessName::from("operator"),
            message_sender: "router".into(),
            message_body: "operator message".into(),
            message_slot: 11,
        }),
    );
    let operator_event = read_working_event(&mut operator_stream);

    let mut designer_stream = UnixStream::connect(fixture.socket()).expect("designer connects");
    write_working_request(
        &mut designer_stream,
        HarnessRequest::MessageDelivery(MessageDelivery {
            harness_name: HarnessName::from("designer"),
            message_sender: "router".into(),
            message_body: "designer message".into(),
            message_slot: 12,
        }),
    );
    let designer_event = read_working_event(&mut designer_stream);

    assert_eq!(
        operator_event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness_name: HarnessName::from("operator"),
            message_slot: 11,
        })
    );
    assert_eq!(
        designer_event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness_name: HarnessName::from("designer"),
            message_slot: 12,
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
        HarnessRequest::MessageDelivery(MessageDelivery {
            harness_name: HarnessName::from("operator"),
            message_sender: "router".into(),
            message_body: "deliver through pi rpc".into(),
            message_slot: 9,
        }),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness_name: HarnessName::from("operator"),
            message_slot: 9,
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
        HarnessRequest::MessageDelivery(MessageDelivery {
            harness_name: HarnessName::from("operator"),
            message_sender: "router".into(),
            message_body: "cannot deliver without terminal".into(),
            message_slot: 8,
        }),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::DeliveryFailed(DeliveryFailed {
            harness_name: HarnessName::from("operator"),
            message_slot: 8,
            delivery_failure_reason: DeliveryFailureReason::TransportRejected,
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
        HarnessRequest::HarnessStatusQuery(HarnessStatusQuery {
            harness_name: HarnessName::from("operator"),
        }),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::HarnessStatus(HarnessStatus {
            harness_name: HarnessName::from("operator"),
            harness_health: HarnessHealth::Running,
            harness_readiness: HarnessReadiness::Ready,
        })
    );
}

#[test]
fn harness_daemon_watch_transcript_returns_typed_snapshot() {
    let fixture = SocketFixture::new("watch-transcript");
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
        HarnessRequest::WatchHarnessTranscript(WatchHarnessTranscript {
            harness_name: HarnessName::from("operator"),
        }),
    );
    let event = read_working_event(&mut stream);

    match event {
        HarnessEvent::HarnessTranscriptSnapshot(snapshot) => {
            assert_eq!(
                snapshot.harness_transcript_token.harness_name,
                HarnessName::from("operator")
            );
            assert_eq!(
                snapshot
                    .harness_transcript_token
                    .harness_transcript_subscription_identifier,
                1
            );
            assert_eq!(snapshot.harness_transcript_sequence, 0);
        }
        other => panic!("expected transcript snapshot, got {other:?}"),
    }
}

#[test]
fn harness_daemon_unwatch_transcript_returns_final_retraction_ack_on_subscribed_stream() {
    let fixture = SocketFixture::new("unwatch-transcript");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![fixture_instance("operator").build()]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(fixture.socket());

    let mut watch_stream = UnixStream::connect(fixture.socket()).expect("watch client connects");
    write_working_request(
        &mut watch_stream,
        HarnessRequest::WatchHarnessTranscript(WatchHarnessTranscript {
            harness_name: HarnessName::from("operator"),
        }),
    );
    let token = transcript_snapshot_token(read_working_event(&mut watch_stream));

    write_working_request(
        &mut watch_stream,
        HarnessRequest::UnwatchHarnessTranscript(token.clone()),
    );
    let event = read_working_event(&mut watch_stream);

    assert_eq!(
        event,
        HarnessEvent::HarnessSubscriptionRetracted(signal_harness::HarnessSubscriptionRetracted {
            harness_transcript_token: token
        })
    );
}

#[tokio::test]
async fn harness_daemon_watch_transcript_stream_delivers_published_observation_and_final_ack() {
    let fixture = SocketFixture::new("watch-transcript-stream");
    let configuration =
        DaemonConfigurationBuilder::new(&fixture).build(vec![fixture_instance("operator").build()]);
    let engine = std::sync::Arc::new(HarnessEngine::from_configuration(configuration));
    let (mut client_stream, mut server_stream) =
        tokio::net::UnixStream::pair().expect("socket pair");
    let server_engine = engine.clone();
    let server = tokio::spawn(async move {
        server_engine
            .handle_working_stream(&mut server_stream)
            .await
            .expect("server handles transcript stream");
    });

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::WatchHarnessTranscript(WatchHarnessTranscript {
            harness_name: HarnessName::from("operator"),
        }),
    )
    .await;
    let token = transcript_snapshot_token(read_working_event_async(&mut client_stream).await);

    let receipt = engine
        .publish_transcript_observation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 1,
            transcript_line: "ready".to_string(),
        })
        .await
        .expect("publish transcript observation");
    assert!(receipt.published);
    assert_eq!(receipt.fanned_out, 1);

    let event = read_working_stream_event_async(&mut client_stream).await;
    assert_eq!(
        event,
        HarnessStreamEvent::TranscriptObservation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 1,
            transcript_line: "ready".to_string(),
        })
    );

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::UnwatchHarnessTranscript(token.clone()),
    )
    .await;
    let final_ack = read_working_event_async(&mut client_stream).await;
    assert_eq!(
        final_ack,
        HarnessEvent::HarnessSubscriptionRetracted(signal_harness::HarnessSubscriptionRetracted {
            harness_transcript_token: token
        })
    );

    server.await.expect("server task joins");
}

#[tokio::test]
async fn harness_daemon_allows_nested_watchers_for_same_harness_without_cross_closing() {
    let fixture = SocketFixture::new("nested-watch-transcript-stream");
    let configuration =
        DaemonConfigurationBuilder::new(&fixture).build(vec![fixture_instance("operator").build()]);
    let engine = std::sync::Arc::new(HarnessEngine::from_configuration(configuration));
    let (mut client_stream, mut server_stream) =
        tokio::net::UnixStream::pair().expect("socket pair");
    let server_engine = engine.clone();
    let server = tokio::spawn(async move {
        server_engine
            .handle_working_stream(&mut server_stream)
            .await
            .expect("server handles transcript stream");
    });

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::WatchHarnessTranscript(WatchHarnessTranscript {
            harness_name: HarnessName::from("operator"),
        }),
    )
    .await;
    let first_token = transcript_snapshot_token(read_working_event_async(&mut client_stream).await);

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::WatchHarnessTranscript(WatchHarnessTranscript {
            harness_name: HarnessName::from("operator"),
        }),
    )
    .await;
    let second_token =
        transcript_snapshot_token(read_working_event_async(&mut client_stream).await);
    assert_eq!(first_token.harness_name, HarnessName::from("operator"));
    assert_eq!(second_token.harness_name, HarnessName::from("operator"));
    assert_ne!(first_token, second_token);

    let receipt = engine
        .publish_transcript_observation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 1,
            transcript_line: "first".to_string(),
        })
        .await
        .expect("publish transcript observation");
    assert!(receipt.published);
    assert_eq!(
        receipt.fanned_out, 2,
        "both nested watchers should receive the first observation"
    );

    let first_delivery = read_working_stream_frame_async(&mut client_stream).await;
    let second_delivery = read_working_stream_frame_async(&mut client_stream).await;
    let delivered_tokens = [first_delivery.token, second_delivery.token];
    assert!(delivered_tokens.contains(&first_token.clone()));
    assert!(delivered_tokens.contains(&second_token.clone()));
    assert_eq!(
        first_delivery.event,
        HarnessStreamEvent::TranscriptObservation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 1,
            transcript_line: "first".to_string(),
        })
    );
    assert_eq!(
        second_delivery.event,
        HarnessStreamEvent::TranscriptObservation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 1,
            transcript_line: "first".to_string(),
        })
    );

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::UnwatchHarnessTranscript(first_token.clone()),
    )
    .await;
    assert_eq!(
        read_working_event_async(&mut client_stream).await,
        HarnessEvent::HarnessSubscriptionRetracted(signal_harness::HarnessSubscriptionRetracted {
            harness_transcript_token: first_token.clone(),
        })
    );

    let receipt = engine
        .publish_transcript_observation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 2,
            transcript_line: "second-only".to_string(),
        })
        .await
        .expect("publish after first close");
    assert!(receipt.published);
    assert_eq!(
        receipt.fanned_out, 1,
        "closing the first watcher must not close the second"
    );
    let remaining_delivery = read_working_stream_frame_async(&mut client_stream).await;
    assert_eq!(remaining_delivery.token, second_token.clone());
    assert_eq!(
        remaining_delivery.event,
        HarnessStreamEvent::TranscriptObservation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 2,
            transcript_line: "second-only".to_string(),
        })
    );

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::UnwatchHarnessTranscript(second_token.clone()),
    )
    .await;
    assert_eq!(
        read_working_event_async(&mut client_stream).await,
        HarnessEvent::HarnessSubscriptionRetracted(signal_harness::HarnessSubscriptionRetracted {
            harness_transcript_token: second_token.clone(),
        })
    );
    server.await.expect("server task joins");

    let receipt = engine
        .publish_transcript_observation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 3,
            transcript_line: "after-close".to_string(),
        })
        .await
        .expect("publish after both close");
    assert!(receipt.published);
    assert_eq!(receipt.fanned_out, 0);
}

#[tokio::test]
async fn harness_daemon_rejects_cross_harness_nested_watch_without_leaking_subscription() {
    let fixture = SocketFixture::new("cross-harness-nested-watch");
    let configuration = DaemonConfigurationBuilder::new(&fixture).build(vec![
        fixture_instance("operator").build(),
        fixture_instance("designer").build(),
    ]);
    let engine = std::sync::Arc::new(HarnessEngine::from_configuration(configuration));
    let (mut client_stream, mut server_stream) =
        tokio::net::UnixStream::pair().expect("socket pair");
    let server_engine = engine.clone();
    let server = tokio::spawn(async move {
        server_engine
            .handle_working_stream(&mut server_stream)
            .await
            .expect("server handles transcript stream");
    });

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::WatchHarnessTranscript(WatchHarnessTranscript {
            harness_name: HarnessName::from("operator"),
        }),
    )
    .await;
    let operator_token =
        transcript_snapshot_token(read_working_event_async(&mut client_stream).await);

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::WatchHarnessTranscript(WatchHarnessTranscript {
            harness_name: HarnessName::from("designer"),
        }),
    )
    .await;
    assert_eq!(
        read_working_event_async(&mut client_stream).await,
        HarnessEvent::HarnessRequestUnimplemented(HarnessRequestUnimplemented {
            harness_name: HarnessName::from("designer"),
            harness_operation_kind: HarnessOperationKind::WatchTranscript,
            harness_unimplemented_reason: HarnessUnimplementedReason::NotBuiltYet,
        })
    );

    let operator_receipt = engine
        .publish_transcript_observation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 1,
            transcript_line: "operator-only".to_string(),
        })
        .await
        .expect("publish operator transcript observation");
    assert!(operator_receipt.published);
    assert_eq!(
        operator_receipt.fanned_out, 1,
        "cross-harness nested watch must not create a second subscription in the bound manager"
    );

    let event = read_working_stream_event_async(&mut client_stream).await;
    assert_eq!(
        event,
        HarnessStreamEvent::TranscriptObservation(TranscriptObservation {
            harness_name: HarnessName::from("operator"),
            harness_transcript_sequence: 1,
            transcript_line: "operator-only".to_string(),
        })
    );

    let designer_receipt = engine
        .publish_transcript_observation(TranscriptObservation {
            harness_name: HarnessName::from("designer"),
            harness_transcript_sequence: 1,
            transcript_line: "designer-not-subscribed".to_string(),
        })
        .await
        .expect("publish designer transcript observation");
    assert!(designer_receipt.published);
    assert_eq!(
        designer_receipt.fanned_out, 0,
        "rejected cross-harness watch must not subscribe the requested harness either"
    );

    write_working_request_async(
        &mut client_stream,
        HarnessRequest::UnwatchHarnessTranscript(operator_token.clone()),
    )
    .await;
    assert_eq!(
        read_working_event_async(&mut client_stream).await,
        HarnessEvent::HarnessSubscriptionRetracted(signal_harness::HarnessSubscriptionRetracted {
            harness_transcript_token: operator_token,
        })
    );
    server.await.expect("server task joins");
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
        HarnessRequest::InteractionPrompt(InteractionPrompt {
            harness_name: HarnessName::from("operator"),
            interaction_identifier: "interaction-1".to_string(),
            interaction_prompt_text: "Approve?".to_string(),
            interaction_options: vec!["yes".to_string(), "no".to_string()],
        }),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::HarnessRequestUnimplemented(HarnessRequestUnimplemented {
            harness_name: HarnessName::from("operator"),
            harness_operation_kind: HarnessOperationKind::PromptInteraction,
            harness_unimplemented_reason: HarnessUnimplementedReason::NotBuiltYet,
        })
    );
}

#[test]
fn harness_daemon_answers_meta_harness_relation_with_typed_unimplemented() {
    let fixture = SocketFixture::new("meta-harness");
    let meta_socket = fixture.meta_socket();
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    let configuration = DaemonConfigurationBuilder::new(&fixture)
        .with_supervision_socket_mode(0o600)
        .build(vec![fixture_instance("operator").build()]);
    write_configuration(&configuration_path, configuration.clone());
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);

    wait_for_socket(&meta_socket);

    let mut stream = UnixStream::connect(&meta_socket).expect("meta-harness client connects");
    let wire = SignalWire::default();
    wire.write(&mut stream, &MetaHarnessRequest::Configure(configuration))
        .expect("meta request writes");
    let reply: MetaHarnessReply = wire.read(&mut stream).expect("meta reply reads");

    assert_eq!(
        reply,
        MetaHarnessReply::RequestUnimplemented(MetaRequestUnimplemented {
            meta_operation_kind: MetaOperationKind::ConfigureDaemon,
            unimplemented_reason: MetaUnimplementedReason::NotBuiltYet,
        })
    );
}

#[test]
fn harness_daemon_resolves_exact_pi_model_request() {
    let fixture = SocketFixture::new("resolve-exact-pi");
    let meta_socket = fixture.meta_socket();
    let pi_rpc = PiRpcFixture::new("resolve-exact-pi");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            HarnessInstanceConfigurationBuilder::new("operator", ContractHarnessKind::Pi)
                .with_pi_rpc(&pi_rpc)
                .with_pi_model_pattern("gemma-4-26b-a4b-ud-q4-k-xl")
                .build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(&meta_socket);

    let request = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl")),
        EffortRequest::Low,
        ContinuationRequest::Fresh,
    );
    let reply = meta_harness_exchange(&meta_socket, MetaHarnessRequest::ResolveModel(request));

    assert_eq!(
        reply,
        MetaHarnessReply::ModelResolved(ModelResolved {
            harness_name: HarnessName::from("operator"),
            harness_kind: ContractHarnessKind::Pi,
            named_model: NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl"),
            effort_request: EffortRequest::Low,
            continuation_handle: ContinuationHandle::Pi(PiContinuationIdentifier::from("operator")),
        })
    );
}

#[test]
fn harness_daemon_resolves_capability_profile_request() {
    let fixture = SocketFixture::new("resolve-capability-pi");
    let meta_socket = fixture.meta_socket();
    let pi_rpc = PiRpcFixture::new("resolve-capability-pi");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            HarnessInstanceConfigurationBuilder::new("operator", ContractHarnessKind::Pi)
                .with_pi_rpc(&pi_rpc)
                .with_pi_model_pattern("gemma-4-26b-a4b-ud-q4-k-xl")
                .build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(&meta_socket);

    let request = model_resolution_request(
        ModelSelector::CapabilityProfile(CapabilityProfile::from("local")),
        EffortRequest::Minimal,
        ContinuationRequest::Fresh,
    );
    let reply = meta_harness_exchange(&meta_socket, MetaHarnessRequest::ResolveModel(request));

    assert_eq!(
        reply,
        MetaHarnessReply::ModelResolved(ModelResolved {
            harness_name: HarnessName::from("operator"),
            harness_kind: ContractHarnessKind::Pi,
            named_model: NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl"),
            effort_request: EffortRequest::Minimal,
            continuation_handle: ContinuationHandle::Pi(PiContinuationIdentifier::from("operator")),
        })
    );
}

#[test]
fn harness_daemon_returns_typed_model_unavailable_reasons() {
    let fixture = SocketFixture::new("resolve-unavailable");
    let meta_socket = fixture.meta_socket();
    let pi_rpc = PiRpcFixture::new("resolve-unavailable");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            HarnessInstanceConfigurationBuilder::new("operator", ContractHarnessKind::Pi)
                .with_pi_rpc(&pi_rpc)
                .with_pi_model_pattern("gemma-4-26b-a4b-ud-q4-k-xl")
                .build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(&meta_socket);

    let unknown_model = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("unknown-model")),
        EffortRequest::Low,
        ContinuationRequest::Fresh,
    );
    assert_eq!(
        meta_harness_exchange(
            &meta_socket,
            MetaHarnessRequest::ResolveModel(unknown_model.clone())
        ),
        MetaHarnessReply::ModelUnavailable(ModelUnavailable {
            model_resolution_request: unknown_model,
            model_unavailable_reason: ModelUnavailableReason::ModelNotKnown,
        })
    );

    let unsupported_capability = model_resolution_request(
        ModelSelector::CapabilityProfile(CapabilityProfile::from("cloud-reasoning")),
        EffortRequest::Low,
        ContinuationRequest::Fresh,
    );
    assert_eq!(
        meta_harness_exchange(
            &meta_socket,
            MetaHarnessRequest::ResolveModel(unsupported_capability.clone())
        ),
        MetaHarnessReply::ModelUnavailable(ModelUnavailable {
            model_resolution_request: unsupported_capability,
            model_unavailable_reason: ModelUnavailableReason::CapabilityUnsupported,
        })
    );

    let unsupported_effort = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl")),
        EffortRequest::ExtraHigh,
        ContinuationRequest::Fresh,
    );
    assert_eq!(
        meta_harness_exchange(
            &meta_socket,
            MetaHarnessRequest::ResolveModel(unsupported_effort.clone())
        ),
        MetaHarnessReply::ModelUnavailable(ModelUnavailable {
            model_resolution_request: unsupported_effort,
            model_unavailable_reason: ModelUnavailableReason::EffortUnsupported,
        })
    );
}

#[test]
fn harness_daemon_validates_continuation_handles_at_harness_boundary() {
    let fixture = SocketFixture::new("resolve-continuation");
    let meta_socket = fixture.meta_socket();
    let pi_rpc = PiRpcFixture::new("resolve-continuation");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            HarnessInstanceConfigurationBuilder::new("operator", ContractHarnessKind::Pi)
                .with_pi_rpc(&pi_rpc)
                .with_pi_model_pattern("gemma-4-26b-a4b-ud-q4-k-xl")
                .build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(&meta_socket);

    let required_pi = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl")),
        EffortRequest::Low,
        ContinuationRequest::Require(ContinuationHandle::Pi(PiContinuationIdentifier::from(
            "operator",
        ))),
    );
    assert_eq!(
        meta_harness_exchange(&meta_socket, MetaHarnessRequest::ResolveModel(required_pi)),
        MetaHarnessReply::ModelResolved(ModelResolved {
            harness_name: HarnessName::from("operator"),
            harness_kind: ContractHarnessKind::Pi,
            named_model: NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl"),
            effort_request: EffortRequest::Low,
            continuation_handle: ContinuationHandle::Pi(PiContinuationIdentifier::from("operator")),
        })
    );

    let preferred_pi = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl")),
        EffortRequest::Low,
        ContinuationRequest::Prefer(ContinuationHandle::Pi(PiContinuationIdentifier::from(
            "operator",
        ))),
    );
    assert_eq!(
        meta_harness_exchange(&meta_socket, MetaHarnessRequest::ResolveModel(preferred_pi)),
        MetaHarnessReply::ModelResolved(ModelResolved {
            harness_name: HarnessName::from("operator"),
            harness_kind: ContractHarnessKind::Pi,
            named_model: NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl"),
            effort_request: EffortRequest::Low,
            continuation_handle: ContinuationHandle::Pi(PiContinuationIdentifier::from("operator")),
        })
    );

    let wrong_provider = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl")),
        EffortRequest::Low,
        ContinuationRequest::Prefer(ContinuationHandle::Claude(ClaudeSessionIdentifier::from(
            "claude-session",
        ))),
    );
    assert_eq!(
        meta_harness_exchange(
            &meta_socket,
            MetaHarnessRequest::ResolveModel(wrong_provider.clone())
        ),
        MetaHarnessReply::ModelUnavailable(ModelUnavailable {
            model_resolution_request: wrong_provider,
            model_unavailable_reason: ModelUnavailableReason::ContinuationUnavailable,
        })
    );

    let wrong_session_require = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl")),
        EffortRequest::Low,
        ContinuationRequest::Require(ContinuationHandle::Pi(PiContinuationIdentifier::from(
            "elsewhere",
        ))),
    );
    assert_eq!(
        meta_harness_exchange(
            &meta_socket,
            MetaHarnessRequest::ResolveModel(wrong_session_require.clone())
        ),
        MetaHarnessReply::ModelUnavailable(ModelUnavailable {
            model_resolution_request: wrong_session_require,
            model_unavailable_reason: ModelUnavailableReason::ContinuationUnavailable,
        })
    );

    let wrong_session_prefer = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl")),
        EffortRequest::Low,
        ContinuationRequest::Prefer(ContinuationHandle::Pi(PiContinuationIdentifier::from(
            "elsewhere",
        ))),
    );
    assert_eq!(
        meta_harness_exchange(
            &meta_socket,
            MetaHarnessRequest::ResolveModel(wrong_session_prefer.clone())
        ),
        MetaHarnessReply::ModelUnavailable(ModelUnavailable {
            model_resolution_request: wrong_session_prefer,
            model_unavailable_reason: ModelUnavailableReason::ContinuationUnavailable,
        })
    );
}

#[test]
fn harness_daemon_resolves_required_continuation_against_later_matching_pi_candidate() {
    let fixture = SocketFixture::new("resolve-continuation-candidates");
    let meta_socket = fixture.meta_socket();
    let first_pi_rpc = PiRpcFixture::new("resolve-continuation-candidates-first");
    let second_pi_rpc = PiRpcFixture::new("resolve-continuation-candidates-second");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            HarnessInstanceConfigurationBuilder::new("first", ContractHarnessKind::Pi)
                .with_pi_rpc(&first_pi_rpc)
                .with_pi_model_pattern("gemma-4-26b-a4b-ud-q4-k-xl")
                .build(),
            HarnessInstanceConfigurationBuilder::new("second", ContractHarnessKind::Pi)
                .with_pi_rpc(&second_pi_rpc)
                .with_pi_model_pattern("gemma-4-26b-a4b-ud-q4-k-xl")
                .build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(&meta_socket);

    let request = model_resolution_request(
        ModelSelector::Exact(NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl")),
        EffortRequest::Low,
        ContinuationRequest::Require(ContinuationHandle::Pi(PiContinuationIdentifier::from(
            "second",
        ))),
    );
    assert_eq!(
        meta_harness_exchange(&meta_socket, MetaHarnessRequest::ResolveModel(request)),
        MetaHarnessReply::ModelResolved(ModelResolved {
            harness_name: HarnessName::from("second"),
            harness_kind: ContractHarnessKind::Pi,
            named_model: NamedModel::from("gemma-4-26b-a4b-ud-q4-k-xl"),
            effort_request: EffortRequest::Low,
            continuation_handle: ContinuationHandle::Pi(PiContinuationIdentifier::from("second")),
        })
    );
}

#[test]
fn harness_daemon_reports_adapter_configuration_missing_for_unlaunchable_match() {
    let fixture = SocketFixture::new("resolve-adapter-missing");
    let meta_socket = fixture.meta_socket();
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    write_configuration(
        &configuration_path,
        DaemonConfigurationBuilder::new(&fixture).build(vec![
            HarnessInstanceConfigurationBuilder::new("operator", ContractHarnessKind::Pi).build(),
        ]),
    );
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(&meta_socket);

    let request = model_resolution_request(
        ModelSelector::CapabilityProfile(CapabilityProfile::from("pi")),
        EffortRequest::Low,
        ContinuationRequest::Fresh,
    );
    assert_eq!(
        meta_harness_exchange(
            &meta_socket,
            MetaHarnessRequest::ResolveModel(request.clone())
        ),
        MetaHarnessReply::ModelUnavailable(ModelUnavailable {
            model_resolution_request: request,
            model_unavailable_reason: ModelUnavailableReason::AdapterConfigurationMissing,
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

    assert!(matches!(
        supervision_exchange(
            &supervision_socket,
            SupervisionRequest::Announce(Presence {
                expected_component: "harness".into(),
                expected_kind: ComponentKind::Harness,
                engine_management_protocol_version: 1,
            }),
        ),
        SupervisionReply::Identified(identity)
            if identity.component_name == "harness"
                && identity.component_kind == ComponentKind::Harness
    ));

    assert!(matches!(
        supervision_exchange(
            &supervision_socket,
            SupervisionRequest::Query(LifecycleQuery::ReadinessStatus("harness".into())),
        ),
        SupervisionReply::Ready(_)
    ));

    assert!(matches!(
        supervision_exchange(
            &supervision_socket,
            SupervisionRequest::Query(LifecycleQuery::HealthStatus("harness".into())),
        ),
        SupervisionReply::HealthReport(ComponentHealth::Running)
    ));
}

/// The supervision socket carries only the engine-management lifecycle: a
/// meta request sent to it is not answered as one.
#[test]
fn harness_daemon_keeps_meta_and_supervision_on_separate_sockets() {
    let fixture = SocketFixture::new("separate-management-sockets");
    let configuration_path = fixture.root.join("harness-daemon.rkyv");
    let configuration =
        DaemonConfigurationBuilder::new(&fixture).build(vec![fixture_instance("operator").build()]);
    write_configuration(&configuration_path, configuration);
    let _daemon = SpawnedHarnessDaemon::spawn(&configuration_path);
    wait_for_socket(&fixture.supervision_socket());
    wait_for_socket(&fixture.meta_socket());
    assert_ne!(fixture.supervision_socket(), fixture.meta_socket());
    assert_eq!(socket_mode(&fixture.meta_socket()), 0o600);

    let mut stream =
        UnixStream::connect(fixture.supervision_socket()).expect("supervision client connects");
    let wire = SignalWire::default();
    wire.write(
        &mut stream,
        &MetaHarnessRequest::LaunchSession(signal_harness::SessionLaunchRequest {
            harness_kind: ContractHarnessKind::Codex,
            agent_identity_token: "xk3f".into(),
            initial_prompt: "never launched".into(),
            continuation_request: ContinuationRequest::Fresh,
        }),
    )
    .expect("write a meta frame to the supervision socket");
    let reply: Result<MetaHarnessReply, _> = wire.read(&mut stream);
    assert!(
        reply.is_err(),
        "the supervision socket must not answer a meta request: {reply:?}"
    );
}

/// One supervision exchange: open a fresh connection to the supervision
/// socket, send one engine-management request, read its reply.
fn supervision_exchange(socket: &Path, request: SupervisionRequest) -> SupervisionReply {
    let mut stream = UnixStream::connect(socket).expect("supervision client connects");
    let wire = SignalWire::default();
    wire.write(&mut stream, &request)
        .expect("supervision request writes");
    wire.read(&mut stream).expect("supervision reply reads")
}

/// One meta exchange on the meta socket.
fn meta_harness_exchange(socket: &Path, request: MetaHarnessRequest) -> MetaHarnessReply {
    let mut stream = UnixStream::connect(socket).expect("meta-harness client connects");
    let wire = SignalWire::default();
    wire.write(&mut stream, &request)
        .expect("meta-harness request writes");
    wire.read(&mut stream).expect("meta-harness reply reads")
}

fn model_resolution_request(
    selector: ModelSelector,
    effort: EffortRequest,
    continuation: ContinuationRequest,
) -> ModelResolutionRequest {
    ModelResolutionRequest {
        model_request: ModelRequest {
            model_selector: selector,
            effort_request: effort,
        },
        continuation_request: continuation,
    }
}

/// Builds a binary `HarnessDaemonConfiguration` against one fixture's sockets.
struct DaemonConfigurationBuilder {
    harness_socket_path: String,
    harness_socket_mode: u32,
    meta_socket_path: String,
    supervision_socket_path: String,
    supervision_socket_mode: u32,
}

impl DaemonConfigurationBuilder {
    fn new(fixture: &SocketFixture) -> Self {
        Self {
            harness_socket_path: fixture.socket().display().to_string(),
            harness_socket_mode: 0o600,
            meta_socket_path: fixture.meta_socket().display().to_string(),
            supervision_socket_path: fixture.supervision_socket().display().to_string(),
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
            domain_socket_path: self.harness_socket_path,
            domain_socket_mode: self.harness_socket_mode.into(),
            meta_socket_path: self.meta_socket_path,
            meta_socket_mode: 0o600,
            engine_management_socket_path: self.supervision_socket_path,
            engine_management_socket_mode: self.supervision_socket_mode.into(),
            owner_identity: OwnerIdentity::UnixUser(1000),
            harness_instance_configurations: harnesses,
        }
    }
}

/// Builds one harness instance configuration record.
struct HarnessInstanceConfigurationBuilder {
    harness_name: HarnessName,
    harness_kind: ContractHarnessKind,
    terminal_socket_path: Option<String>,
    pi_rpc_adapter: Option<signal_harness::PiRpcJsonlAdapterConfiguration>,
}

impl HarnessInstanceConfigurationBuilder {
    fn new(harness_name: &str, harness_kind: ContractHarnessKind) -> Self {
        Self {
            harness_name: HarnessName::from(harness_name),
            harness_kind,
            terminal_socket_path: None,
            pi_rpc_adapter: None,
        }
    }

    fn with_terminal_socket_path(mut self, path: &Path) -> Self {
        self.terminal_socket_path = Some(path.display().to_string());
        self
    }

    fn with_pi_rpc(mut self, fixture: &PiRpcFixture) -> Self {
        self.pi_rpc_adapter = Some(signal_harness::PiRpcJsonlAdapterConfiguration {
            pi_rpc_command_path: fixture.command_path().display().to_string(),
            pi_rpc_session_directory_path: fixture.session_directory().display().to_string(),
            pi_rpc_model_pattern_option: None,
            pi_rpc_delivery_mode: signal_harness::PiRpcDeliveryMode::Steer,
        });
        self
    }

    fn with_pi_model_pattern(mut self, model_pattern: &str) -> Self {
        let Some(adapter) = self.pi_rpc_adapter.as_mut() else {
            panic!("pi model pattern requires pi rpc adapter");
        };
        adapter.pi_rpc_model_pattern_option = Some(model_pattern.to_owned());
        self
    }

    fn build(self) -> HarnessInstanceConfiguration {
        HarnessInstanceConfiguration {
            harness_name: self.harness_name,
            harness_kind: self.harness_kind,
            terminal_socket_path_option: self.terminal_socket_path,
            pi_rpc_jsonl_adapter_configuration_option: self.pi_rpc_adapter,
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

/// Writes one ordinary `Query` frame.
fn write_working_request(stream: &mut UnixStream, request: HarnessRequest) {
    SignalWire::default()
        .write(stream, &request)
        .expect("harness request writes");
}

/// Reads one ordinary `Response` frame.
fn read_working_event(stream: &mut UnixStream) -> HarnessEvent {
    SignalWire::default()
        .read(stream)
        .expect("harness response reads")
}

async fn write_working_request_async(stream: &mut tokio::net::UnixStream, request: HarnessRequest) {
    SignalWire::default()
        .write_async(stream, &request)
        .await
        .expect("write harness request");
}

async fn read_working_event_async(stream: &mut tokio::net::UnixStream) -> HarnessEvent {
    SignalWire::default()
        .read_async(stream)
        .await
        .expect("harness response reads")
}

fn transcript_snapshot_token(event: HarnessEvent) -> HarnessTranscriptToken {
    match event {
        HarnessEvent::HarnessTranscriptSnapshot(snapshot) => snapshot.harness_transcript_token,
        other => panic!("expected transcript snapshot, got {other:?}"),
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ReceivedWorkingStreamEvent {
    token: HarnessTranscriptToken,
    event: HarnessStreamEvent,
}

async fn read_working_stream_event_async(
    stream: &mut tokio::net::UnixStream,
) -> HarnessStreamEvent {
    read_working_stream_frame_async(stream).await.event
}

async fn read_working_stream_frame_async(
    stream: &mut tokio::net::UnixStream,
) -> ReceivedWorkingStreamEvent {
    match read_working_event_async(stream).await {
        HarnessEvent::HarnessTranscriptEvent(event) => ReceivedWorkingStreamEvent {
            token: event.harness_transcript_token,
            event: event.harness_stream_event,
        },
        other => panic!("expected harness transcript event, got {other:?}"),
    }
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

fn unique_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos()
}
