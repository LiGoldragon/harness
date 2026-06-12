use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use meta_signal_harness::{
    MetaHarnessFrame, MetaHarnessFrameBody, MetaHarnessReply, MetaHarnessRequest,
    RequestUnimplemented, UnimplementedReason,
};
use nota_next::NotaEncode;
use signal_frame::{NonEmpty, Reply, SubReply};
use signal_harness::{
    HarnessDaemonConfiguration, HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessHealth,
    HarnessName, HarnessReadiness, HarnessRequest, HarnessStatus, HarnessStatusQuery,
};
use signal_persona::origin::{OwnerIdentity, UnixUserIdentifier};
use signal_persona::{SocketMode, WirePath};
use triad_runtime::{FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

#[derive(Debug)]
struct CliSocketFixture {
    root: PathBuf,
}

impl CliSocketFixture {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("harness-cli-{name}-{}-{now}", std::process::id()));
        std::fs::create_dir_all(&root).expect("create harness cli fixture directory");
        Self { root }
    }

    fn socket(&self) -> PathBuf {
        self.root.join("harness.sock")
    }

    fn meta_socket(&self) -> PathBuf {
        self.root.join("meta-harness.sock")
    }

    fn configuration(&self) -> HarnessDaemonConfiguration {
        HarnessDaemonConfiguration {
            harness_socket_path: WirePath::new(self.socket().display().to_string()),
            harness_socket_mode: SocketMode::new(0o600),
            supervision_socket_path: WirePath::new(self.meta_socket().display().to_string()),
            supervision_socket_mode: SocketMode::new(0o600),
            owner_identity: OwnerIdentity::UnixUser(UnixUserIdentifier::new(1000)),
            harnesses: Vec::new(),
        }
    }
}

impl Drop for CliSocketFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[test]
fn harness_cli_reaches_working_socket_and_prints_typed_reply() {
    let fixture = CliSocketFixture::new("working");
    let listener = UnixListener::bind(fixture.socket()).expect("fake harness socket binds");
    let server = thread::spawn(move || {
        let (mut stream, _address) = listener.accept().expect("harness cli connects");
        let (exchange, request) = HarnessCliServer::read_request(&mut stream);
        assert_eq!(
            request,
            HarnessRequest::HarnessStatusQuery(HarnessStatusQuery {
                harness: HarnessName::new("operator"),
            })
        );
        HarnessCliServer::write_reply(
            &mut stream,
            exchange,
            HarnessEvent::HarnessStatus(HarnessStatus {
                harness: HarnessName::new("operator"),
                health: HarnessHealth::Running,
                readiness: HarnessReadiness::Ready,
            }),
        );
    });

    let request = HarnessRequest::HarnessStatusQuery(HarnessStatusQuery {
        harness: HarnessName::new("operator"),
    })
    .to_nota();
    let output = Command::new(env!("CARGO_BIN_EXE_harness"))
        .env("HARNESS_SOCKET", fixture.socket())
        .arg(request)
        .output()
        .expect("run harness cli");

    assert!(
        output.status.success(),
        "harness cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("harness cli stdout is utf8");
    assert!(
        stdout.contains("HarnessStatus"),
        "unexpected stdout: {stdout}"
    );
    assert!(stdout.contains("Running"), "unexpected stdout: {stdout}");
    server.join().expect("fake harness server exits");
}

#[test]
fn meta_harness_cli_reaches_policy_socket_and_prints_typed_reply() {
    let fixture = CliSocketFixture::new("meta");
    let configuration = fixture.configuration();
    let listener =
        UnixListener::bind(fixture.meta_socket()).expect("fake meta-harness socket binds");
    let expected = configuration.clone();
    let server = thread::spawn(move || {
        let (mut stream, _address) = listener.accept().expect("meta-harness cli connects");
        let (exchange, request) = MetaHarnessCliServer::read_request(&mut stream);
        assert_eq!(request, MetaHarnessRequest::Configure(expected));
        MetaHarnessCliServer::write_reply(
            &mut stream,
            exchange,
            MetaHarnessReply::RequestUnimplemented(RequestUnimplemented {
                operation: meta_signal_harness::OperationKind::Configure,
                reason: UnimplementedReason::NotBuiltYet,
            }),
        );
    });

    let request = MetaHarnessRequest::Configure(configuration).to_nota();
    let output = Command::new(env!("CARGO_BIN_EXE_meta-harness"))
        .env("HARNESS_META_SOCKET", fixture.meta_socket())
        .arg(request)
        .output()
        .expect("run meta-harness cli");

    assert!(
        output.status.success(),
        "meta-harness cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("meta-harness cli stdout is utf8");
    assert!(
        stdout.contains("RequestUnimplemented"),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains("NotBuiltYet"),
        "unexpected stdout: {stdout}"
    );
    server.join().expect("fake meta-harness server exits");
}

#[derive(Debug)]
struct HarnessCliServer;

impl HarnessCliServer {
    fn read_request(stream: &mut UnixStream) -> (signal_frame::ExchangeIdentifier, HarnessRequest) {
        let body = RuntimeFrame::read(stream);
        match HarnessFrame::decode(body.bytes())
            .expect("decode harness signal frame")
            .into_body()
        {
            HarnessFrameBody::Request { exchange, request } => {
                let (payload, tail) = request.payloads.into_head_and_tail();
                assert!(tail.is_empty(), "harness cli should send one payload");
                (exchange, payload)
            }
            other => panic!("expected harness request frame, got {other:?}"),
        }
    }

    fn write_reply(
        stream: &mut UnixStream,
        exchange: signal_frame::ExchangeIdentifier,
        reply: HarnessEvent,
    ) {
        let frame = HarnessFrame::new(HarnessFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        RuntimeFrame::write(stream, frame.encode().expect("encode harness reply"));
    }
}

#[derive(Debug)]
struct MetaHarnessCliServer;

impl MetaHarnessCliServer {
    fn read_request(
        stream: &mut UnixStream,
    ) -> (signal_frame::ExchangeIdentifier, MetaHarnessRequest) {
        let body = RuntimeFrame::read(stream);
        match MetaHarnessFrame::decode(body.bytes())
            .expect("decode meta-harness signal frame")
            .into_body()
        {
            MetaHarnessFrameBody::Request { exchange, request } => {
                let (payload, tail) = request.payloads.into_head_and_tail();
                assert!(tail.is_empty(), "meta-harness cli should send one payload");
                (exchange, payload)
            }
            other => panic!("expected meta-harness request frame, got {other:?}"),
        }
    }

    fn write_reply(
        stream: &mut UnixStream,
        exchange: signal_frame::ExchangeIdentifier,
        reply: MetaHarnessReply,
    ) {
        let frame = MetaHarnessFrame::new(MetaHarnessFrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        RuntimeFrame::write(stream, frame.encode().expect("encode meta-harness reply"));
    }
}

#[derive(Debug)]
struct RuntimeFrame;

impl RuntimeFrame {
    fn read(stream: &mut UnixStream) -> RuntimeFrameBody {
        LengthPrefixedCodec::default()
            .read_body(stream)
            .expect("read runtime frame body")
    }

    fn write(stream: &mut UnixStream, bytes: Vec<u8>) {
        LengthPrefixedCodec::default()
            .write_body(stream, &RuntimeFrameBody::new(bytes))
            .expect("write runtime frame body");
    }
}
