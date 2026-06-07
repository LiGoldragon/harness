use std::{
    io::Write,
    os::unix::{fs::MetadataExt, net::UnixListener},
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::mpsc::{Receiver, channel},
    thread,
    time::{Duration, Instant},
};

use harness::HarnessDaemonConfigurationFile;
use message::{Configuration as MessageConfiguration, command::Output as MessageCommandOutput};
use nota_codec::{Encoder, NotaEncode};
use signal_core::{ExchangeIdentifier, NonEmpty, Reply, SignalVerb, SubReply};
use signal_harness::{
    HarnessDaemonConfiguration, HarnessInstanceConfiguration, HarnessKind, HarnessName,
};
use signal_persona::{SocketMode as WireSocketMode, WirePath};
use signal_persona_origin::{OwnerIdentity, UnixUserIdentifier};
use signal_persona_terminal::{
    TerminalFrame, TerminalFrameBody, TerminalGeneration, TerminalInputAccepted, TerminalReply,
    TerminalRequest,
};
use signal_router::{
    Actor, ActorIdentifier, EndpointKind, EndpointTransport, GrantDirectMessage, RegisterActor,
    RouterBootstrapDocument, RouterBootstrapOperation, RouterDaemonConfiguration,
};
use tempfile::TempDir;

const AGENT_A: &str = "agent-a";
const AGENT_B: &str = "agent-b";

#[test]
fn message_cli_round_trips_between_two_agents_through_one_harness_daemon() {
    let Some(test) = MessageRouterHarnessE2e::new() else {
        eprintln!(
            "skipping message/router/harness e2e; set MESSAGE_CLI_BINARY, MESSAGE_DAEMON_BINARY, and ROUTER_DAEMON_BINARY or provide sibling repositories"
        );
        return;
    };

    let harness_daemon = test.spawn_harness_daemon();
    let router_daemon = test.spawn_router_daemon();
    let agent_a_message_daemon = test.spawn_message_daemon(AGENT_A);
    let agent_b_message_daemon = test.spawn_message_daemon(AGENT_B);

    let output = Command::new(test.binaries().message_cli())
        .env("MESSAGE_SOCKET", test.message_socket(AGENT_A))
        .arg("(Send agent-b [question from agent a])")
        .output()
        .expect("run agent A message CLI");

    assert!(
        output.status.success(),
        "agent A message CLI failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("message CLI stdout is utf8");
    match MessageCommandOutput::from_nota(stdout.trim()).expect("decode message CLI NOTA output") {
        MessageCommandOutput::SubmissionAccepted(acceptance) => {
            assert_eq!(acceptance.message_slot, 1);
        }
        other => panic!("expected SubmissionAccepted, got {other:?}"),
    }

    let agent_b_text = test.agent_b_terminal().received_text();
    assert!(
        agent_b_text.contains("question from agent a"),
        "agent B terminal did not receive routed message body: {agent_b_text:?}"
    );
    match test.agent_b_terminal().reply_output() {
        MessageCommandOutput::SubmissionAccepted(acceptance) => {
            assert_eq!(acceptance.message_slot, 2);
        }
        other => panic!("expected agent B reply SubmissionAccepted, got {other:?}"),
    }

    let agent_a_text = test.agent_a_terminal().received_text();
    assert!(
        agent_a_text.contains("response from agent b"),
        "agent A terminal did not receive response body: {agent_a_text:?}"
    );

    drop(agent_b_message_daemon);
    drop(agent_a_message_daemon);
    drop(router_daemon);
    drop(harness_daemon);
}

struct MessageRouterHarnessE2e {
    root: TempDir,
    agent_a_terminal: RecordingTerminalSocket,
    agent_b_terminal: ReplyingTerminalSocket,
    binaries: ExternalBinaries,
}

impl MessageRouterHarnessE2e {
    fn new() -> Option<Self> {
        let root = TempDir::new().expect("tempdir");
        let binaries = ExternalBinaries::build()?;
        let agent_b_message_socket = Self::message_socket_for(root.path(), AGENT_B);
        Some(Self {
            agent_a_terminal: RecordingTerminalSocket::new("agent-a"),
            agent_b_terminal: ReplyingTerminalSocket::new(
                "agent-b",
                binaries.message_cli().to_path_buf(),
                agent_b_message_socket,
            ),
            root,
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

    fn agent_a_terminal(&self) -> &RecordingTerminalSocket {
        &self.agent_a_terminal
    }

    fn agent_b_terminal(&self) -> &ReplyingTerminalSocket {
        &self.agent_b_terminal
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
        Self::message_socket_for(self.root_path(), actor)
    }

    fn message_socket_for(root: &Path, actor: &str) -> PathBuf {
        root.join(format!("{actor}-message.sock"))
    }

    fn spawn_harness_daemon(&self) -> ManagedProcess {
        let harness_socket = self.harness_socket();
        let supervision_socket = self.harness_supervision_socket();
        let configuration_path = self.root_path().join("harness.rkyv");
        let configuration = HarnessDaemonConfiguration {
            harness_socket_path: WirePath::new(harness_socket.display().to_string()),
            harness_socket_mode: WireSocketMode::new(0o600),
            supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
            supervision_socket_mode: WireSocketMode::new(0o600),
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(self.current_uid())),
            harnesses: vec![
                HarnessInstanceConfiguration {
                    harness_name: HarnessName::new(AGENT_A),
                    harness_kind: HarnessKind::Pi,
                    terminal_socket_path: Some(WirePath::new(
                        self.agent_a_terminal().path().display().to_string(),
                    )),
                    pi_rpc_adapter: None,
                },
                HarnessInstanceConfiguration {
                    harness_name: HarnessName::new(AGENT_B),
                    harness_kind: HarnessKind::Pi,
                    terminal_socket_path: Some(WirePath::new(
                        self.agent_b_terminal().path().display().to_string(),
                    )),
                    pi_rpc_adapter: None,
                },
            ],
        };
        HarnessDaemonConfigurationFile::new(configuration_path.clone())
            .write_configuration(&configuration)
            .expect("write binary harness configuration");
        let process = ManagedProcess::spawn(
            "harness-daemon",
            env!("CARGO_BIN_EXE_harness-daemon"),
            &[configuration_path],
        );
        SocketWait::new(&harness_socket).wait();
        SocketWait::new(&supervision_socket).wait();
        process
    }

    fn spawn_router_daemon(&self) -> ManagedProcess {
        let bootstrap_path = self.root_path().join("router-bootstrap.nota");
        BootstrapFile::write(&bootstrap_path, &self.harness_socket());
        let configuration_path = self.root_path().join("router.nota");
        let router_socket = self.router_socket();
        let meta_socket = self.router_meta_socket();
        let supervision_socket = self.router_supervision_socket();
        let configuration = RouterDaemonConfiguration {
            router_socket_path: WirePath::new(router_socket.display().to_string()),
            router_socket_mode: WireSocketMode::new(0o600),
            meta_router_socket_path: WirePath::new(meta_socket.display().to_string()),
            meta_router_socket_mode: WireSocketMode::new(0o600),
            supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
            supervision_socket_mode: WireSocketMode::new(0o600),
            store_path: WirePath::new(self.root_path().join("router.redb").display().to_string()),
            bootstrap_path: Some(WirePath::new(bootstrap_path.display().to_string())),
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(self.current_uid())),
        };
        NotaFile::write(&configuration_path, &configuration);
        let process = ManagedProcess::spawn(
            "router-daemon",
            self.binaries.router_daemon(),
            &[configuration_path],
        );
        SocketWait::new(&router_socket).wait();
        SocketWait::new(&meta_socket).wait();
        SocketWait::new(&supervision_socket).wait();
        process
    }

    fn spawn_message_daemon(&self, actor: &str) -> ManagedProcess {
        let configuration_path = self.root_path().join(format!("{actor}-message.rkyv"));
        MessageConfiguration::new(
            self.message_socket(actor),
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
        process
    }
}

struct BootstrapFile;

impl BootstrapFile {
    fn write(path: &Path, harness_socket: &Path) {
        let document = RouterBootstrapDocument::new(vec![
            RouterBootstrapOperation::RegisterActor(RegisterActor::new(Actor::new(
                ActorIdentifier::new(AGENT_A),
                std::process::id(),
                Some(EndpointTransport::new(
                    EndpointKind::HarnessSocket,
                    harness_socket.display().to_string(),
                    None,
                )),
            ))),
            RouterBootstrapOperation::RegisterActor(RegisterActor::new(Actor::new(
                ActorIdentifier::new(AGENT_B),
                std::process::id(),
                Some(EndpointTransport::new(
                    EndpointKind::HarnessSocket,
                    harness_socket.display().to_string(),
                    None,
                )),
            ))),
            RouterBootstrapOperation::GrantDirectMessage(GrantDirectMessage::new(
                ActorIdentifier::new(AGENT_A),
                ActorIdentifier::new(AGENT_B),
            )),
            RouterBootstrapOperation::GrantDirectMessage(GrantDirectMessage::new(
                ActorIdentifier::new(AGENT_B),
                ActorIdentifier::new(AGENT_A),
            )),
        ]);
        std::fs::write(
            path,
            document
                .to_nota_lines()
                .expect("encode router bootstrap document"),
        )
        .expect("write router bootstrap document");
    }
}

struct NotaFile;

impl NotaFile {
    fn write<T: NotaEncode>(path: &Path, value: &T) {
        let mut encoder = Encoder::new();
        value
            .encode(&mut encoder)
            .expect("encode NOTA configuration");
        let mut text = encoder.into_string();
        text.push('\n');
        std::fs::write(path, text).expect("write NOTA configuration");
    }
}

struct ExternalBinaries {
    message_cli: PathBuf,
    message_daemon: PathBuf,
    router_daemon: PathBuf,
}

impl ExternalBinaries {
    fn build() -> Option<Self> {
        if let Some(binaries) = Self::from_binary_environment() {
            return Some(binaries);
        }
        let repositories = RepositoryPaths::from_environment()?;
        Some(Self {
            message_cli: CargoBinary::build(repositories.message(), "message"),
            message_daemon: CargoBinary::build(repositories.message(), "message-daemon"),
            router_daemon: CargoBinary::build(repositories.router(), "router-daemon"),
        })
    }

    fn from_binary_environment() -> Option<Self> {
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

struct RepositoryPaths {
    message: PathBuf,
    router: PathBuf,
}

impl RepositoryPaths {
    fn from_environment() -> Option<Self> {
        let paths = Self {
            message: std::env::var_os("MESSAGE_REPOSITORY")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/git/github.com/LiGoldragon/message")),
            router: std::env::var_os("ROUTER_REPOSITORY")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/git/github.com/LiGoldragon/router")),
        };
        if paths.message.join("Cargo.toml").exists() && paths.router.join("Cargo.toml").exists() {
            Some(paths)
        } else {
            None
        }
    }

    fn message(&self) -> &Path {
        &self.message
    }

    fn router(&self) -> &Path {
        &self.router
    }
}

struct CargoBinary;

impl CargoBinary {
    fn build(repository: &Path, name: &str) -> PathBuf {
        let manifest_path = repository.join("Cargo.toml");
        let output = Command::new("cargo")
            .arg("build")
            .arg("--quiet")
            .arg("--manifest-path")
            .arg(&manifest_path)
            .arg("--bin")
            .arg(name)
            .output()
            .unwrap_or_else(|error| panic!("build {name}: {error}"));
        assert!(
            output.status.success(),
            "cargo build failed for {name}\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        repository.join("target").join("debug").join(name)
    }
}

struct ManagedProcess {
    child: Child,
}

impl ManagedProcess {
    fn spawn(name: &'static str, binary: impl AsRef<Path>, arguments: &[PathBuf]) -> Self {
        let child = Command::new(binary.as_ref())
            .args(arguments)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TerminalFixtureFrameCodec {
    maximum_frame_bytes: usize,
}

impl TerminalFixtureFrameCodec {
    fn read_frame(
        &self,
        stream: &mut std::os::unix::net::UnixStream,
    ) -> harness::Result<TerminalFrame> {
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

    fn read_request(
        &self,
        stream: &mut std::os::unix::net::UnixStream,
    ) -> harness::Result<ReceivedTerminalRequest> {
        match self.read_frame(stream)?.into_body() {
            TerminalFrameBody::Request { exchange, request } => {
                let checked = request
                    .into_checked()
                    .map_err(|(reason, _)| harness::Error::InvalidSignalRequest { reason })?;
                let operation = checked.operations.into_head();
                Ok(ReceivedTerminalRequest {
                    exchange,
                    verb: operation.verb,
                    request: operation.payload,
                })
            }
            other => Err(harness::Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    fn write_reply(
        &self,
        stream: &mut std::os::unix::net::UnixStream,
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
        reply: TerminalReply,
    ) -> harness::Result<()> {
        let frame = TerminalFrame::new(TerminalFrameBody::Reply {
            exchange,
            reply: Reply::completed(NonEmpty::single(SubReply::Ok {
                verb,
                payload: reply,
            })),
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
    verb: SignalVerb,
    request: TerminalRequest,
}

struct RecordingTerminalSocket {
    path: PathBuf,
    received: Receiver<Vec<u8>>,
}

impl RecordingTerminalSocket {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "harness-terminal-{name}-{}-{}.sock",
            std::process::id(),
            UniqueNanos::now()
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
                            received_request.verb,
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

    fn path(&self) -> &Path {
        &self.path
    }

    fn received_text(&self) -> String {
        String::from_utf8(
            self.received
                .recv_timeout(Duration::from_secs(15))
                .expect("terminal socket receives input bytes"),
        )
        .expect("terminal input is utf8")
    }
}

impl Drop for RecordingTerminalSocket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct ReplyingTerminalSocket {
    path: PathBuf,
    received: Receiver<Vec<u8>>,
    reply_output: Receiver<MessageCommandOutput>,
}

impl ReplyingTerminalSocket {
    fn new(name: &str, message_cli: PathBuf, message_socket: PathBuf) -> Self {
        let path = std::env::temp_dir().join(format!(
            "harness-terminal-{name}-{}-{}.sock",
            std::process::id(),
            UniqueNanos::now()
        ));
        let listener = UnixListener::bind(&path).expect("terminal acceptance socket binds");
        let (sender, received) = channel();
        let (reply_sender, reply_output) = channel();
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
                            received_request.verb,
                            TerminalReply::TerminalInputAccepted(TerminalInputAccepted {
                                terminal: input.terminal,
                                generation: TerminalGeneration::new(1),
                            }),
                        )
                        .expect("terminal socket writes Signal acceptance");
                    let output = Command::new(message_cli)
                        .env("MESSAGE_SOCKET", message_socket)
                        .arg("(Send agent-a [response from agent b])")
                        .output()
                        .expect("run agent B message CLI");
                    assert!(
                        output.status.success(),
                        "agent B message CLI failed\nstdout:\n{}\nstderr:\n{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    let stdout = String::from_utf8(output.stdout)
                        .expect("agent B message CLI stdout is utf8");
                    let reply = MessageCommandOutput::from_nota(stdout.trim())
                        .expect("decode agent B message CLI NOTA output");
                    reply_sender
                        .send(reply)
                        .expect("terminal socket reports reply CLI output");
                }
                other => panic!("expected TerminalInput request, got {other:?}"),
            }
        });
        Self {
            path,
            received,
            reply_output,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn received_text(&self) -> String {
        String::from_utf8(
            self.received
                .recv_timeout(Duration::from_secs(15))
                .expect("terminal socket receives input bytes"),
        )
        .expect("terminal input is utf8")
    }

    fn reply_output(&self) -> MessageCommandOutput {
        self.reply_output
            .recv_timeout(Duration::from_secs(15))
            .expect("terminal socket receives reply CLI output")
    }
}

impl Drop for ReplyingTerminalSocket {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
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
        while started.elapsed() < Duration::from_secs(15) {
            if self.path.exists() {
                return;
            }
            thread::sleep(Duration::from_millis(25));
        }
        panic!("socket did not appear at {}", self.path.display());
    }
}

struct UniqueNanos;

impl UniqueNanos {
    fn now() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos()
    }
}
