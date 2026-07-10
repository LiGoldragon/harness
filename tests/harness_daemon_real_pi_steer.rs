//! Live proof that the `harness-daemon` delivers a routed message body to a real
//! running `pi` session as an RPC steer.
//!
//! The sibling `daemon.rs` test `harness_daemon_delivers_message_to_pi_rpc_endpoint`
//! proves the harness's Pi-RPC delivery arm against a fixture pi script. This
//! test keeps the exact same working-socket exchange but points the configured
//! `pi_rpc_adapter` at the genuine `pi` binary (wrapped by a transparent tee, so
//! the delivered steer is observable at the pi boundary). A `DeliveryCompleted`
//! event is only produced once the real pi answers the harness-authored steer
//! command with its matching RPC success response, so the assertion witnesses the
//! whole harness -> live pi last mile.
//!
//! This test needs the real `pi` binary and a reachable model, so it is gated on
//! environment and skips when they are absent:
//!
//!   PI_STEER_TEE_WRAPPER  the pi tee wrapper executable (invoked in pi's place)
//!   PI_STEER_MODEL        the pi model pattern for the ingesting turn
//!
//! The tee wrapper reads `PI_REAL` and the log sinks this test exports
//! (`PI_WRAPPER_INLOG`, `PI_WRAPPER_OUTLOG`) plus the optional
//! `PI_WRAPPER_INJECT_PROMPT` that starts the next natural turn so the queued
//! steer is ingested by the model.

use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use harness::HarnessDaemonConfigurationFile;
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, Request, SessionEpoch, SubReply,
};
use signal_harness::{
    DeliveryCompleted, HarnessDaemonConfiguration, HarnessEvent, HarnessFrame, HarnessFrameBody,
    HarnessInstanceConfiguration, HarnessKind, HarnessName, HarnessRequest, MessageBody,
    MessageDelivery, MessageSender, MessageSlot, PiRpcCommandPath, PiRpcDeliveryMode,
    PiRpcJsonlAdapterConfiguration, PiRpcModelPattern, PiRpcSessionDirectoryPath,
};
use signal_persona::{
    DomainSocketMode, DomainSocketPath, EngineManagementSocketMode, EngineManagementSocketPath,
    OwnerIdentity, UnixUserIdentifier,
};
use tempfile::TempDir;

const TARGET: &str = "operator";
const MESSAGE_BODY: &str = "hello from the messenger";

#[test]
fn harness_daemon_delivers_routed_body_to_real_pi_as_steer() {
    let (Some(tee_wrapper), Ok(model)) = (
        std::env::var_os("PI_STEER_TEE_WRAPPER"),
        std::env::var("PI_STEER_MODEL"),
    ) else {
        eprintln!("skipping live pi steer test; set PI_STEER_TEE_WRAPPER and PI_STEER_MODEL");
        return;
    };
    let fixture = LivePiHarness::new(PathBuf::from(tee_wrapper), model);
    let _daemon = fixture.spawn();
    wait_for_socket(&fixture.harness_socket());

    let mut stream = UnixStream::connect(fixture.harness_socket()).expect("client connects");
    write_working_request(
        &mut stream,
        MessageDelivery {
            harness: HarnessName::new(TARGET),
            sender: MessageSender::new("router"),
            body: MessageBody::new(MESSAGE_BODY),
            message_slot: MessageSlot::new(1),
        }
        .into(),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness: HarnessName::new(TARGET),
            message_slot: MessageSlot::new(1),
        }),
        "the real pi did not acknowledge the steer with an RPC success"
    );

    let inbound = wait_for_file_contains(
        &fixture.pi_inbound_log(),
        &["\"type\":\"steer\"", MESSAGE_BODY],
    );
    let outbound = wait_for_file_contains(
        &fixture.pi_outbound_log(),
        &["\"command\":\"steer\"", "\"success\":true"],
    );
    eprintln!("=== pi inbound (harness -> live pi) ===\n{inbound}");
    eprintln!("=== pi outbound (live pi -> harness) ===\n{outbound}");
}

struct LivePiHarness {
    root: TempDir,
    tee_wrapper: PathBuf,
    model: String,
}

impl LivePiHarness {
    fn new(tee_wrapper: PathBuf, model: String) -> Self {
        Self {
            root: TempDir::new().expect("tempdir"),
            tee_wrapper,
            model,
        }
    }

    fn path(&self) -> &Path {
        self.root.path()
    }

    fn current_uid(&self) -> u32 {
        self.path().metadata().expect("tempdir metadata").uid()
    }

    fn harness_socket(&self) -> PathBuf {
        self.path().join("harness.sock")
    }

    fn supervision_socket(&self) -> PathBuf {
        self.path().join("harness-supervision.sock")
    }

    fn pi_session_directory(&self) -> PathBuf {
        self.path().join("pi-session")
    }

    fn pi_inbound_log(&self) -> PathBuf {
        self.path().join("pi-inbound.jsonl")
    }

    fn pi_outbound_log(&self) -> PathBuf {
        self.path().join("pi-outbound.jsonl")
    }

    fn spawn(&self) -> SpawnedDaemon {
        let configuration_path = self.path().join("harness.rkyv");
        let configuration = HarnessDaemonConfiguration {
            domain_socket_path: DomainSocketPath::new(self.harness_socket().display().to_string()),
            domain_socket_mode: DomainSocketMode::new(0o600),
            engine_management_socket_path: EngineManagementSocketPath::new(
                self.supervision_socket().display().to_string(),
            ),
            engine_management_socket_mode: EngineManagementSocketMode::new(0o600),
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(
                self.current_uid().into(),
            )),
            harnesses: vec![HarnessInstanceConfiguration {
                harness_name: HarnessName::new(TARGET),
                harness_kind: HarnessKind::Pi,
                terminal_socket_path: None,
                pi_rpc_adapter: Some(PiRpcJsonlAdapterConfiguration {
                    command_path: PiRpcCommandPath::new(self.tee_wrapper.display().to_string()),
                    session_directory_path: PiRpcSessionDirectoryPath::new(
                        self.pi_session_directory().display().to_string(),
                    ),
                    delivery_mode: PiRpcDeliveryMode::Steer,
                    model_pattern: Some(PiRpcModelPattern::new(&self.model)),
                }),
            }],
        };
        HarnessDaemonConfigurationFile::new(configuration_path.clone())
            .write_configuration(&configuration)
            .expect("write binary harness configuration");

        let child = Command::new(env!("CARGO_BIN_EXE_harness-daemon"))
            .arg(&configuration_path)
            .env(
                "PI_REAL",
                std::env::var("PI_REAL")
                    .unwrap_or_else(|_| "/home/li/.nix-profile/bin/pi".to_string()),
            )
            .env("PI_WRAPPER_INLOG", self.pi_inbound_log())
            .env("PI_WRAPPER_OUTLOG", self.pi_outbound_log())
            .env(
                "PI_WRAPPER_INJECT_PROMPT",
                "Report any steering instruction you have received.",
            )
            .spawn()
            .expect("harness-daemon starts");
        SpawnedDaemon { child }
    }
}

struct SpawnedDaemon {
    child: Child,
}

impl Drop for SpawnedDaemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_working_request(stream: &mut UnixStream, request: HarnessRequest) {
    let frame = HarnessFrame::new(HarnessFrameBody::Request {
        exchange: test_exchange(),
        request: Request::from_payload(request),
    });
    let body = frame.encode().expect("harness request encodes");
    let length = u32::try_from(body.len()).expect("frame body fits a u32 length prefix");
    stream
        .write_all(&length.to_be_bytes())
        .expect("length prefix writes");
    stream.write_all(&body).expect("frame body writes");
    stream.flush().expect("frame flushes");
}

fn read_working_event(stream: &mut UnixStream) -> HarnessEvent {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).expect("event length reads");
    let length = u32::from_be_bytes(prefix) as usize;
    let mut body = vec![0_u8; length];
    stream.read_exact(&mut body).expect("event body reads");
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

fn test_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

fn wait_for_socket(socket: &Path) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(20) {
        if socket.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("socket did not appear at {}", socket.display());
}

fn wait_for_file_contains(path: &Path, needles: &[&str]) -> String {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if let Ok(text) = std::fs::read_to_string(path)
            && needles.iter().all(|needle| text.contains(needle))
        {
            return text;
        }
        thread::sleep(Duration::from_millis(50));
    }
    let text = std::fs::read_to_string(path).unwrap_or_default();
    panic!(
        "file {} never contained all of {needles:?}; current contents:\n{text}",
        path.display()
    );
}
