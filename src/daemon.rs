use std::ffi::OsString;
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use kameo::actor::ActorRef;
use signal_core::{FrameBody, Reply, Request};
use signal_persona_harness::{
    Frame as HarnessFrame, HarnessEvent, HarnessHealth, HarnessName, HarnessReadiness,
    HarnessRequest, HarnessRequestUnimplemented, HarnessStatus, HarnessStatusQuery,
    HarnessUnimplementedReason,
};

use crate::{
    Error, Harness, HarnessBinding, HarnessId, HarnessKind, HarnessLifecycle, HarnessState,
    ReadState, Result, SetHarnessLifecycle,
};

#[derive(Debug)]
pub struct HarnessDaemon {
    socket: PathBuf,
    harness: HarnessName,
    socket_mode: Option<SocketMode>,
}

impl HarnessDaemon {
    pub fn from_socket(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            harness: HarnessName::new("harness"),
            socket_mode: SocketMode::from_environment(),
        }
    }

    pub fn with_harness(mut self, harness: HarnessName) -> Self {
        self.harness = harness;
        self
    }

    pub fn with_socket_mode(mut self, socket_mode: SocketMode) -> Self {
        self.socket_mode = Some(socket_mode);
        self
    }

    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn harness(&self) -> &HarnessName {
        &self.harness
    }

    pub fn run(self) -> Result<()> {
        let bound = self.bind()?;
        eprintln!("persona-harness-daemon socket={}", bound.socket.display());
        bound.serve_forever()
    }

    pub fn bind(self) -> Result<BoundHarnessDaemon> {
        if let Some(parent) = self.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        if let Some(socket_mode) = self.socket_mode {
            std::fs::set_permissions(
                &self.socket,
                std::fs::Permissions::from_mode(socket_mode.as_octal()),
            )?;
        }
        let runtime = tokio::runtime::Runtime::new()?;
        let harness = runtime.block_on(self.start_harness())?;
        Ok(BoundHarnessDaemon {
            socket: self.socket,
            runtime,
            listener,
            harness,
        })
    }

    pub fn serve_one(self) -> Result<HarnessEvent> {
        self.bind()?.serve_one()
    }

    async fn start_harness(&self) -> Result<ActorRef<Harness>> {
        let reference = Harness::start(self.binding()).await;
        reference
            .ask(SetHarnessLifecycle {
                lifecycle: HarnessLifecycle::Running,
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reference)
    }

    async fn stop_harness(reference: ActorRef<Harness>) -> Result<()> {
        reference
            .stop_gracefully()
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        reference.wait_for_shutdown().await;
        Ok(())
    }

    fn binding(&self) -> HarnessBinding {
        HarnessBinding::new(
            HarnessId::new(self.harness.as_str()),
            HarnessKind::Pi,
            std::env::current_dir()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|_| ".".to_string()),
        )
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        harness: &ActorRef<Harness>,
        stream: UnixStream,
    ) -> Result<HarnessEvent> {
        let mut connection = HarnessConnection::from_stream(stream);
        let request = connection.read_signal_request()?;
        let event = runtime.block_on(async {
            HarnessRequestHandler::new(harness.clone())
                .event_for_request(request)
                .await
        })?;
        connection.write_signal_event(event.clone())?;
        Ok(event)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketMode(u32);

impl SocketMode {
    pub const fn from_octal(value: u32) -> Self {
        Self(value)
    }

    pub fn from_environment() -> Option<Self> {
        std::env::var("PERSONA_SOCKET_MODE")
            .ok()
            .and_then(|value| u32::from_str_radix(value.as_str(), 8).ok())
            .map(Self::from_octal)
    }

    pub const fn as_octal(self) -> u32 {
        self.0
    }
}

pub struct BoundHarnessDaemon {
    socket: PathBuf,
    runtime: tokio::runtime::Runtime,
    listener: UnixListener,
    harness: ActorRef<Harness>,
}

impl BoundHarnessDaemon {
    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn serve_one(self) -> Result<HarnessEvent> {
        let (stream, _address) = self.listener.accept()?;
        let event = HarnessDaemon::handle_connection(&self.runtime, &self.harness, stream)?;
        self.runtime
            .block_on(HarnessDaemon::stop_harness(self.harness))?;
        let _ = std::fs::remove_file(&self.socket);
        Ok(event)
    }

    pub fn serve_forever(self) -> Result<()> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            let _ = HarnessDaemon::handle_connection(&self.runtime, &self.harness, stream)?;
        }
        Ok(())
    }
}

pub struct HarnessConnection {
    stream: BufReader<UnixStream>,
    signal: HarnessFrameCodec,
}

impl HarnessConnection {
    pub fn from_stream(stream: UnixStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            signal: HarnessFrameCodec::default(),
        }
    }

    pub fn read_signal_request(&mut self) -> Result<HarnessRequest> {
        self.signal.read_request(&mut self.stream)
    }

    pub fn write_signal_event(&mut self, event: HarnessEvent) -> Result<()> {
        let stream = self.stream.get_mut();
        self.signal.write_event(stream, event)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HarnessFrameCodec {
    maximum_frame_bytes: usize,
}

impl HarnessFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_frame(&self, reader: &mut impl Read) -> Result<HarnessFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("frame length {length} exceeds {}", self.maximum_frame_bytes),
            });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(HarnessFrame::decode_length_prefixed(&bytes)?)
    }

    pub fn read_request(&self, reader: &mut impl Read) -> Result<HarnessRequest> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Request(Request::Operation { payload, .. }) => Ok(payload),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_event(&self, writer: &mut impl Write, event: HarnessEvent) -> Result<()> {
        let frame = HarnessFrame::new(FrameBody::Reply(Reply::operation(event)));
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }
}

impl Default for HarnessFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

#[derive(Debug, Clone)]
pub struct HarnessRequestHandler {
    harness: ActorRef<Harness>,
}

impl HarnessRequestHandler {
    pub fn new(harness: ActorRef<Harness>) -> Self {
        Self { harness }
    }

    pub async fn event_for_request(&self, request: HarnessRequest) -> Result<HarnessEvent> {
        match request {
            HarnessRequest::HarnessStatusQuery(query) => self.status_event(query).await,
            other => Ok(HarnessRequestUnimplemented {
                harness: Self::request_harness(&other),
                operation: other.operation_kind(),
                reason: HarnessUnimplementedReason::NotBuiltYet,
            }
            .into()),
        }
    }

    async fn status_event(&self, query: HarnessStatusQuery) -> Result<HarnessEvent> {
        let state = self
            .harness
            .ask(ReadState::expecting_at_least(0))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(HarnessStatus {
            harness: query.harness,
            health: Self::health(&state),
            readiness: Self::readiness(&state),
        }
        .into())
    }

    fn health(state: &HarnessState) -> HarnessHealth {
        match state.lifecycle {
            HarnessLifecycle::Running | HarnessLifecycle::Paused | HarnessLifecycle::Starting => {
                HarnessHealth::Running
            }
            HarnessLifecycle::Stopped => HarnessHealth::Stopped,
        }
    }

    fn readiness(state: &HarnessState) -> HarnessReadiness {
        match state.lifecycle {
            HarnessLifecycle::Running | HarnessLifecycle::Paused => HarnessReadiness::Ready,
            HarnessLifecycle::Starting => HarnessReadiness::Starting,
            HarnessLifecycle::Stopped => HarnessReadiness::Unavailable,
        }
    }

    fn request_harness(request: &HarnessRequest) -> HarnessName {
        match request {
            HarnessRequest::MessageDelivery(payload) => payload.harness.clone(),
            HarnessRequest::InteractionPrompt(payload) => payload.harness.clone(),
            HarnessRequest::DeliveryCancellation(payload) => payload.harness.clone(),
            HarnessRequest::HarnessStatusQuery(payload) => payload.harness.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessCommandLine {
    arguments: Vec<OsString>,
}

impl HarnessCommandLine {
    pub fn from_environment() -> Self {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments<I, S>(arguments: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        Self {
            arguments: arguments.into_iter().map(Into::into).collect(),
        }
    }

    pub fn daemon(&self) -> Result<HarnessDaemon> {
        let socket = self.arguments.first().ok_or(Error::MissingSocket)?;
        if let Some(extra) = self.arguments.get(2) {
            return Err(Error::UnexpectedArgument {
                got: extra.to_string_lossy().to_string(),
            });
        }
        let daemon = HarnessDaemon::from_socket(PathBuf::from(socket));
        Ok(match self.arguments.get(1) {
            Some(harness) => {
                daemon.with_harness(HarnessName::new(harness.to_string_lossy().to_string()))
            }
            None => daemon,
        })
    }

    pub fn run(&self) -> Result<()> {
        self.daemon()?.run()
    }
}
