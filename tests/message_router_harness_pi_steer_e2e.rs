//! Live proof of the whole messenger last mile: a real `message` CLI `(Send ...)`
//! command reaches a live `pi` session as an RPC steer.
//!
//! This mirrors the three-daemon wiring of `message_router_harness_e2e.rs`
//! (message CLI -> message-daemon -> router-daemon -> harness-daemon) but the
//! recipient harness instance is configured with a `pi_rpc_adapter` in
//! `delivery_mode: Steer` pointed at the genuine `pi` binary (wrapped by the
//! transparent tee from `harness_daemon_real_pi_steer.rs`, so the delivered
//! steer is observable at the pi boundary). The routed message body therefore
//! travels the entire messenger chain and lands as a steer that the live model
//! ingests on its next natural turn.
//!
//! Gated on both the external binary environment shared with the terminal-arm
//! e2e and the live-pi environment shared with `harness_daemon_real_pi_steer.rs`:
//!
//!   MESSAGE_CLI_BINARY     the `message` CLI executable
//!   MESSAGE_DAEMON_BINARY  the `message-daemon` executable
//!   ROUTER_DAEMON_BINARY   the `router-daemon` executable
//!   PI_STEER_TEE_WRAPPER   the pi tee wrapper executable (invoked in pi's place)
//!   PI_STEER_MODEL         the pi model pattern for the ingesting turn
//!
//! The tee wrapper reads `PI_REAL` and the log sinks this test exports
//! (`PI_WRAPPER_INLOG`, `PI_WRAPPER_OUTLOG`) plus the optional
//! `PI_WRAPPER_INJECT_PROMPT` that starts the next natural turn so the queued
//! steer is ingested by the model.

use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use harness::HarnessDaemonConfigurationFile;
use message::{Configuration as MessageConfiguration, command::Output as MessageCommandOutput};
use signal_harness::{
    HarnessDaemonConfiguration, HarnessInstanceConfiguration, HarnessKind, HarnessName,
    PiRpcCommandPath, PiRpcDeliveryMode, PiRpcJsonlAdapterConfiguration, PiRpcModelPattern,
    PiRpcSessionDirectoryPath,
};
use signal_persona::{
    DomainSocketMode, DomainSocketPath, EngineManagementSocketMode, EngineManagementSocketPath,
    OwnerIdentity, UnixUserIdentifier,
};
use signal_router::{
    Actor, EndpointKind, EndpointTransport, GrantDirectMessage,
    OwnerIdentity as RouterOwnerIdentity, RouterBootstrapDocument, RouterBootstrapOperation,
    RouterDaemonConfiguration, RouterDaemonConfigurationParts,
};
use tempfile::TempDir;

/// The sending actor; only a message-daemon backs it.
const SENDER: &str = "agent-a";
/// The receiving actor; a live-pi harness instance backs it.
const RECIPIENT: &str = "agent-b";
/// The body carried end to end and delivered to the live model as a steer.
const MESSAGE_BODY: &str = "steer from the messenger chain";
/// The explicit thread the sender names on the submission. The router takes a
/// `(Named ...)` thread verbatim into the stored message's `ThreadIdentifier`
/// (router `signal_message` handling), so sending with this name exercises the
/// whole 4-field explicit-thread path — CLI parse, daemon stamp, router
/// verbatim consume, delivery — end to end. The router does not surface the
/// thread over any socket observation (`RouterMessageTrace` reports only slot
/// and delivery status); the thread-value assertion (`Named` verbatim vs
/// absent-derives-direct) lives in the router suite's
/// `explicit_thread_name_is_used_verbatim_while_absent_thread_derives_direct`.
const THREAD_NAME: &str = "messenger-launch-plan";

#[test]
fn message_cli_send_reaches_live_pi_as_a_steer_through_router_and_harness() {
    let Some(test) = MessageRouterHarnessPiSteerE2e::new() else {
        eprintln!(
            "skipping message/router/harness live pi steer e2e; set MESSAGE_CLI_BINARY, \
             MESSAGE_DAEMON_BINARY, ROUTER_DAEMON_BINARY, PI_STEER_TEE_WRAPPER, and PI_STEER_MODEL"
        );
        return;
    };

    let harness_daemon = test.spawn_harness_daemon();
    let router_daemon = test.spawn_router_daemon();
    let sender_message_daemon = test.spawn_message_daemon(SENDER);

    let output = Command::new(test.binaries().message_cli())
        .env("MESSAGE_SOCKET", test.message_socket(SENDER))
        .arg(format!("(Send {RECIPIENT} [{MESSAGE_BODY}] (Named {THREAD_NAME}))"))
        .output()
        .expect("run sender message CLI");
    assert!(
        output.status.success(),
        "sender message CLI failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("message CLI stdout is utf8");
    match MessageCommandOutput::from_nota(stdout.trim()).expect("decode message CLI NOTA output") {
        MessageCommandOutput::SubmissionAccepted(message_slot) => assert_eq!(message_slot, 1),
        other => panic!("expected SubmissionAccepted, got {other:?}"),
    }

    // The routed body is only visible at the pi boundary once the harness has
    // delivered it as a steer over the real pi RPC channel. The tee wrapper
    // records the harness -> pi direction (the queued steer carrying the body)
    // and the pi -> harness direction (the steer command's success reply that
    // the harness turns into `DeliveryCompleted`).
    let inbound = test.wait_for_pi_log_contains(
        &test.pi_inbound_log(),
        &["\"type\":\"steer\"", MESSAGE_BODY],
    );
    let outbound = test.wait_for_pi_log_contains(
        &test.pi_outbound_log(),
        &["\"command\":\"steer\"", "\"success\":true"],
    );
    // The live model surfaces the queued steer in a `queue_update` event as it
    // ingests the harness-authored command on its next natural turn.
    let queued = test.wait_for_pi_log_contains(&test.pi_outbound_log(), &["\"type\":\"queue_update\"", MESSAGE_BODY]);

    eprintln!("=== pi inbound (harness -> live pi) ===\n{inbound}");
    eprintln!("=== pi outbound (live pi -> harness) ===\n{outbound}");
    eprintln!("=== pi queue_update carrying the routed body ===\n{queued}");

    drop(sender_message_daemon);
    drop(router_daemon);
    drop(harness_daemon);
}

struct MessageRouterHarnessPiSteerE2e {
    root: TempDir,
    tee_wrapper: PathBuf,
    model: String,
    binaries: ExternalBinaries,
}

impl MessageRouterHarnessPiSteerE2e {
    fn new() -> Option<Self> {
        let tee_wrapper = std::env::var_os("PI_STEER_TEE_WRAPPER")?;
        let model = std::env::var("PI_STEER_MODEL").ok()?;
        let binaries = ExternalBinaries::build()?;
        Some(Self {
            root: TempDir::new().expect("tempdir"),
            tee_wrapper: PathBuf::from(tee_wrapper),
            model,
            binaries,
        })
    }

    fn root_path(&self) -> &Path {
        self.root.path()
    }

    fn current_uid(&self) -> u32 {
        self.root_path()
            .metadata()
            .expect("read tempdir metadata")
            .uid()
    }

    fn binaries(&self) -> &ExternalBinaries {
        &self.binaries
    }

    fn harness_socket(&self) -> PathBuf {
        self.root_path().join("harness.sock")
    }

    fn harness_supervision_socket(&self) -> PathBuf {
        self.root_path().join("harness-supervision.sock")
    }

    fn router_socket(&self) -> PathBuf {
        self.root_path().join("router.sock")
    }

    fn router_meta_socket(&self) -> PathBuf {
        self.root_path().join("router-meta.sock")
    }

    fn router_supervision_socket(&self) -> PathBuf {
        self.root_path().join("router-supervision.sock")
    }

    fn message_socket(&self, actor: &str) -> PathBuf {
        self.root_path().join(format!("{actor}-message.sock"))
    }

    fn message_meta_socket(&self, actor: &str) -> PathBuf {
        self.root_path().join(format!("{actor}-message-meta.sock"))
    }

    fn pi_session_directory(&self) -> PathBuf {
        self.root_path().join("pi-session")
    }

    fn pi_inbound_log(&self) -> PathBuf {
        self.root_path().join("pi-inbound.jsonl")
    }

    fn pi_outbound_log(&self) -> PathBuf {
        self.root_path().join("pi-outbound.jsonl")
    }

    fn spawn_harness_daemon(&self) -> ManagedProcess {
        let configuration_path = self.root_path().join("harness.rkyv");
        let configuration = HarnessDaemonConfiguration {
            domain_socket_path: DomainSocketPath::new(self.harness_socket().display().to_string()),
            domain_socket_mode: DomainSocketMode::new(0o600),
            engine_management_socket_path: EngineManagementSocketPath::new(
                self.harness_supervision_socket().display().to_string(),
            ),
            engine_management_socket_mode: EngineManagementSocketMode::new(0o600),
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(
                self.current_uid().into(),
            )),
            harnesses: vec![HarnessInstanceConfiguration {
                harness_name: HarnessName::new(RECIPIENT),
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
        let process = ManagedProcess::spawn_with_environment(
            "harness-daemon",
            env!("CARGO_BIN_EXE_harness-daemon"),
            &[configuration_path],
            &[
                (
                    "PI_REAL",
                    std::env::var("PI_REAL")
                        .unwrap_or_else(|_| "/home/li/.nix-profile/bin/pi".to_string()),
                ),
                (
                    "PI_WRAPPER_INLOG",
                    self.pi_inbound_log().display().to_string(),
                ),
                (
                    "PI_WRAPPER_OUTLOG",
                    self.pi_outbound_log().display().to_string(),
                ),
                (
                    "PI_WRAPPER_INJECT_PROMPT",
                    "Report any steering instruction you have received.".to_string(),
                ),
            ],
        );
        SocketWait::new(&self.harness_socket()).wait();
        SocketWait::new(&self.harness_supervision_socket()).wait();
        process
    }

    fn spawn_router_daemon(&self) -> ManagedProcess {
        let bootstrap_path = self.root_path().join("router-bootstrap.rkyv");
        BootstrapFile::write(&bootstrap_path, &self.harness_socket());
        let configuration_path = self.root_path().join("router.rkyv");
        let configuration = RouterDaemonConfiguration::from(RouterDaemonConfigurationParts {
            router_socket_path: signal_router::WirePath::new(
                self.router_socket().display().to_string(),
            ),
            router_socket_mode: signal_router::SocketMode::new(0o600),
            meta_router_socket_path: signal_router::WirePath::new(
                self.router_meta_socket().display().to_string(),
            ),
            meta_router_socket_mode: signal_router::SocketMode::new(0o600),
            supervision_socket_path: signal_router::WirePath::new(
                self.router_supervision_socket().display().to_string(),
            ),
            supervision_socket_mode: signal_router::SocketMode::new(0o600),
            store_path: signal_router::WirePath::new(
                self.root_path().join("router.sema").display().to_string(),
            ),
            bootstrap_path: Some(signal_router::WirePath::new(
                bootstrap_path.display().to_string(),
            )),
            owner_identity: RouterOwnerIdentity::UnixUser(signal_router::UnixUserIdentifier::new(
                u64::from(self.current_uid()),
            )),
            tailnet_listen_address: None,
            router_identity: signal_router::CriomeHostId::new("local-router"),
            criome_socket_path: None,
        });
        RouterConfigurationFile::write(&configuration_path, &configuration);
        let process = ManagedProcess::spawn(
            "router-daemon",
            self.binaries.router_daemon(),
            &[configuration_path],
        );
        SocketWait::new(&self.router_socket()).wait();
        SocketWait::new(&self.router_meta_socket()).wait();
        process
    }

    fn spawn_message_daemon(&self, actor: &str) -> ManagedProcess {
        let configuration_path = self.root_path().join(format!("{actor}-message.rkyv"));
        MessageConfiguration::new(
            &self.message_socket(actor),
            &self.message_meta_socket(actor),
            self.router_socket(),
            self.root_path().join("message.unused"),
            actor,
            self.current_uid(),
        )
        .write_binary_file(&configuration_path)
        .expect("write message daemon configuration");
        let process = ManagedProcess::spawn(
            "message-daemon",
            self.binaries.message_daemon(),
            &[configuration_path],
        );
        SocketWait::new(&self.message_socket(actor)).wait();
        SocketWait::new(&self.message_meta_socket(actor)).wait();
        process
    }

    fn wait_for_pi_log_contains(&self, path: &Path, needles: &[&str]) -> String {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(45) {
            if let Ok(text) = std::fs::read_to_string(path)
                && needles.iter().all(|needle| text.contains(needle))
            {
                return text;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let text = std::fs::read_to_string(path).unwrap_or_default();
        panic!(
            "pi log {} never contained all of {needles:?}; current contents:\n{text}",
            path.display()
        );
    }
}

struct BootstrapFile;

impl BootstrapFile {
    fn write(path: &Path, harness_socket: &Path) {
        let endpoint = EndpointTransport::new(
            EndpointKind::HarnessSocket,
            harness_socket.display().to_string(),
            None,
        );
        let document = RouterBootstrapDocument::from_operations(vec![
            RouterBootstrapOperation::RegisterActor(signal_router::RegisterActor::new(
                Actor::new(
                    signal_router::ActorIdentifier::new(SENDER),
                    u64::from(std::process::id()),
                    Some(endpoint.clone()),
                ),
                None,
            )),
            RouterBootstrapOperation::RegisterActor(signal_router::RegisterActor::new(
                Actor::new(
                    signal_router::ActorIdentifier::new(RECIPIENT),
                    u64::from(std::process::id()),
                    Some(endpoint),
                ),
                None,
            )),
            RouterBootstrapOperation::GrantDirectMessage(GrantDirectMessage {
                source_actor: signal_router::SourceActor::new(signal_router::ActorIdentifier::new(
                    SENDER,
                )),
                destination_actor: signal_router::DestinationActor::new(
                    signal_router::ActorIdentifier::new(RECIPIENT),
                ),
            }),
        ]);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&document)
            .expect("encode router bootstrap archive");
        std::fs::write(path, bytes.as_ref()).expect("write router bootstrap archive");
    }
}

struct RouterConfigurationFile;

impl RouterConfigurationFile {
    fn write(path: &Path, configuration: &RouterDaemonConfiguration) {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(configuration)
            .expect("encode router configuration archive");
        std::fs::write(path, bytes.as_ref()).expect("write router configuration archive");
    }
}

struct ExternalBinaries {
    message_cli: PathBuf,
    message_daemon: PathBuf,
    router_daemon: PathBuf,
}

impl ExternalBinaries {
    fn build() -> Option<Self> {
        Some(Self {
            message_cli: PathBuf::from(std::env::var_os("MESSAGE_CLI_BINARY")?),
            message_daemon: PathBuf::from(std::env::var_os("MESSAGE_DAEMON_BINARY")?),
            router_daemon: PathBuf::from(std::env::var_os("ROUTER_DAEMON_BINARY")?),
        })
    }

    fn message_cli(&self) -> &Path {
        &self.message_cli
    }

    fn message_daemon(&self) -> &Path {
        &self.message_daemon
    }

    fn router_daemon(&self) -> &Path {
        &self.router_daemon
    }
}

struct ManagedProcess {
    child: Child,
}

impl ManagedProcess {
    fn spawn(name: &'static str, binary: impl AsRef<Path>, arguments: &[PathBuf]) -> Self {
        Self::spawn_with_environment(name, binary, arguments, &[])
    }

    fn spawn_with_environment(
        name: &'static str,
        binary: impl AsRef<Path>,
        arguments: &[PathBuf],
        environment: &[(&'static str, String)],
    ) -> Self {
        let mut command = Command::new(binary.as_ref());
        command.args(arguments);
        for (key, value) in environment {
            command.env(key, value);
        }
        let child = command
            .spawn()
            .unwrap_or_else(|error| panic!("spawn {name}: {error}"));
        Self { child }
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct SocketWait<'path> {
    path: &'path Path,
}

impl<'path> SocketWait<'path> {
    fn new(path: &'path Path) -> Self {
        Self { path }
    }

    fn wait(&self) {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(20) {
            if self.path.exists() {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("socket did not appear at {}", self.path.display());
    }
}
