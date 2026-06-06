use std::{
    os::unix::{fs::MetadataExt, net::UnixListener},
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::mpsc::{Receiver, channel},
    thread,
    time::{Duration, Instant},
};

use message::{Configuration as MessageConfiguration, command::Output as MessageCommandOutput};
use nota_codec::{Encoder, NotaEncode};
use persona_terminal::supervisor::TerminalSupervisorFrameCodec;
use signal_harness::{HarnessDaemonConfiguration, HarnessKind, HarnessName};
use signal_persona::{SocketMode as WireSocketMode, WirePath};
use signal_persona_origin::{OwnerIdentity, UnixUserIdentifier};
use signal_persona_terminal::{
    TerminalGeneration, TerminalInputAccepted, TerminalReply, TerminalRequest,
};
use signal_router::{
    Actor, ActorIdentifier, EndpointKind, EndpointTransport, GrantDirectMessage, RegisterActor,
    RouterBootstrapDocument, RouterBootstrapOperation, RouterDaemonConfiguration,
};
use tempfile::TempDir;

#[test]
fn message_cli_reaches_pi_harness_through_real_message_and_router_daemons() {
    let test = MessageRouterHarnessE2e::new();

    let harness_daemon = test.spawn_harness_daemon();
    let router_daemon = test.spawn_router_daemon();
    let message_daemon = test.spawn_message_daemon();

    let output = Command::new(test.binaries().message_cli())
        .env("MESSAGE_SOCKET", test.message_socket())
        .arg("(Send operator [full route to pi harness])")
        .output()
        .expect("run message CLI");

    assert!(
        output.status.success(),
        "message CLI failed\nstdout:\n{}\nstderr:\n{}",
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

    let terminal_text = test.terminal().received_text();
    assert!(
        terminal_text.contains("full route to pi harness"),
        "terminal endpoint did not receive routed message body: {terminal_text:?}"
    );

    drop(message_daemon);
    drop(router_daemon);
    drop(harness_daemon);
}

struct MessageRouterHarnessE2e {
    root: TempDir,
    terminal: TerminalAcceptanceSocket,
    binaries: ExternalBinaries,
}

impl MessageRouterHarnessE2e {
    fn new() -> Self {
        let root = TempDir::new().expect("tempdir");
        Self {
            root,
            terminal: TerminalAcceptanceSocket::new("message-router-harness"),
            binaries: ExternalBinaries::build(),
        }
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

    fn terminal(&self) -> &TerminalAcceptanceSocket {
        &self.terminal
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

    fn message_socket(&self) -> PathBuf {
        self.root_path().join("message.sock")
    }

    fn spawn_harness_daemon(&self) -> ManagedProcess {
        let harness_socket = self.harness_socket();
        let supervision_socket = self.harness_supervision_socket();
        let configuration_path = self.root_path().join("harness.nota");
        let configuration = HarnessDaemonConfiguration {
            harness_socket_path: WirePath::new(harness_socket.display().to_string()),
            harness_socket_mode: WireSocketMode::new(0o600),
            supervision_socket_path: WirePath::new(supervision_socket.display().to_string()),
            supervision_socket_mode: WireSocketMode::new(0o600),
            harness_name: HarnessName::new("operator"),
            harness_kind: HarnessKind::Pi,
            terminal_socket_path: Some(WirePath::new(self.terminal.path().display().to_string())),
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(self.current_uid())),
        };
        NotaFile::write(&configuration_path, &configuration);
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

    fn spawn_message_daemon(&self) -> ManagedProcess {
        let configuration_path = self.root_path().join("message.rkyv");
        MessageConfiguration::new(
            self.message_socket(),
            self.router_socket(),
            self.root_path().join("message.unused"),
            "owner",
            self.current_uid(),
        )
        .write_binary_file(&configuration_path)
        .expect("write message daemon configuration");
        let process = ManagedProcess::spawn(
            "message-daemon",
            self.binaries.message_daemon(),
            &[configuration_path],
        );
        SocketWait::new(&self.message_socket()).wait();
        process
    }
}

struct BootstrapFile;

impl BootstrapFile {
    fn write(path: &Path, harness_socket: &Path) {
        let document = RouterBootstrapDocument::new(vec![
            RouterBootstrapOperation::RegisterActor(RegisterActor::new(Actor::new(
                ActorIdentifier::new("operator"),
                std::process::id(),
                Some(EndpointTransport::new(
                    EndpointKind::HarnessSocket,
                    harness_socket.display().to_string(),
                    None,
                )),
            ))),
            RouterBootstrapOperation::GrantDirectMessage(GrantDirectMessage::new(
                ActorIdentifier::new("owner"),
                ActorIdentifier::new("operator"),
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
    fn build() -> Self {
        let repositories = RepositoryPaths::from_environment();
        Self {
            message_cli: CargoBinary::build(repositories.message(), "message"),
            message_daemon: CargoBinary::build(repositories.message(), "message-daemon"),
            router_daemon: CargoBinary::build(repositories.router(), "router-daemon"),
        }
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
    fn from_environment() -> Self {
        Self {
            message: std::env::var_os("MESSAGE_REPOSITORY")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/git/github.com/LiGoldragon/message")),
            router: std::env::var_os("ROUTER_REPOSITORY")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/git/github.com/LiGoldragon/router")),
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

struct TerminalAcceptanceSocket {
    path: PathBuf,
    received: Receiver<Vec<u8>>,
}

impl TerminalAcceptanceSocket {
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

impl Drop for TerminalAcceptanceSocket {
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
