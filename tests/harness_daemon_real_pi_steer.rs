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

use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

use harness::{HarnessDaemonConfigurationFile, SignalWire};
use signal_harness::{
    DeliveryCompleted, HarnessDaemonConfiguration, HarnessInstanceConfiguration, HarnessKind,
    HarnessName, MessageDelivery, PiRpcDeliveryMode, PiRpcJsonlAdapterConfiguration,
    Query as HarnessRequest, Response as HarnessEvent,
};
use signal_persona::OwnerIdentity;
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
        HarnessRequest::MessageDelivery(MessageDelivery {
            harness_name: HarnessName::from(TARGET),
            message_sender: "router".into(),
            message_body: MESSAGE_BODY.into(),
            message_slot: 1,
        }),
    );
    let event = read_working_event(&mut stream);

    assert_eq!(
        event,
        HarnessEvent::DeliveryCompleted(DeliveryCompleted {
            harness_name: HarnessName::from(TARGET),
            message_slot: 1,
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
            domain_socket_path: self.harness_socket().display().to_string(),
            domain_socket_mode: 0o600,
            meta_socket_path: self.path().join("meta-harness.sock").display().to_string(),
            meta_socket_mode: 0o600,
            engine_management_socket_path: self.supervision_socket().display().to_string(),
            engine_management_socket_mode: 0o600,
            owner_identity: OwnerIdentity::UnixUser(i64::from(self.current_uid())),
            harness_instance_configurations: vec![HarnessInstanceConfiguration {
                harness_name: HarnessName::from(TARGET),
                harness_kind: HarnessKind::Pi,
                terminal_socket_path_option: None,
                pi_rpc_jsonl_adapter_configuration_option: Some(PiRpcJsonlAdapterConfiguration {
                    pi_rpc_command_path: self.tee_wrapper.display().to_string(),
                    pi_rpc_session_directory_path: self
                        .pi_session_directory()
                        .display()
                        .to_string(),
                    pi_rpc_model_pattern_option: Some(self.model.clone()),
                    pi_rpc_delivery_mode: PiRpcDeliveryMode::Steer,
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
    SignalWire::default()
        .write(stream, &request)
        .expect("harness request writes");
}

fn read_working_event(stream: &mut UnixStream) -> HarnessEvent {
    SignalWire::default()
        .read(stream)
        .expect("harness response reads")
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
