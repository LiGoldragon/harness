use std::ffi::OsString;
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use kameo::actor::ActorRef;
use signal_core::{ExchangeIdentifier, NonEmpty, Reply, SignalVerb, SubReply};
use signal_persona_harness::{
    DeliveryCompleted, DeliveryFailed, DeliveryFailureReason, HarnessEvent, HarnessFrame,
    HarnessFrameBody as FrameBody, HarnessHealth, HarnessName, HarnessReadiness, HarnessRequest,
    HarnessRequestUnimplemented, HarnessStatus, HarnessStatusQuery, HarnessUnimplementedReason,
    MessageDelivery,
};

use crate::{
    Error, Harness, HarnessBinding, HarnessId, HarnessKind, HarnessLifecycle, HarnessState,
    HarnessTerminalBinding, HarnessTerminalDelivery, HarnessTerminalEndpoint, ReadState, Result,
    SetHarnessLifecycle,
    supervision::{SupervisionListener, SupervisionProfile},
};

#[derive(Debug)]
pub struct HarnessDaemon {
    socket: PathBuf,
    harness: HarnessName,
    kind: HarnessKind,
    socket_mode: Option<SocketMode>,
    terminal_endpoint: Option<HarnessTerminalEndpoint>,
}

impl HarnessDaemon {
    pub fn from_socket(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            harness: HarnessName::new("harness"),
            kind: HarnessKind::Fixture,
            socket_mode: SocketMode::from_environment(),
            terminal_endpoint: Self::terminal_endpoint_from_environment(),
        }
    }

    pub fn with_harness(mut self, harness: HarnessName) -> Self {
        self.harness = harness;
        self
    }

    pub fn with_kind(mut self, kind: HarnessKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn with_socket_mode(mut self, socket_mode: SocketMode) -> Self {
        self.socket_mode = Some(socket_mode);
        self
    }

    pub fn with_terminal_socket(mut self, path: impl Into<PathBuf>) -> Self {
        self.terminal_endpoint = Some(HarnessTerminalEndpoint::pty_socket(path));
        self
    }

    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn harness(&self) -> &HarnessName {
        &self.harness
    }

    pub fn kind(&self) -> &HarnessKind {
        &self.kind
    }

    pub fn run(self) -> Result<()> {
        let bound = self.bind()?;
        let _supervision = SupervisionListener::from_environment(SupervisionProfile::harness())
            .map(SupervisionListener::spawn)
            .transpose()?;
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
            terminal_endpoint: self.terminal_endpoint,
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
            self.kind.clone(),
            std::env::current_dir()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|_| ".".to_string()),
        )
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        harness: &ActorRef<Harness>,
        terminal_endpoint: Option<HarnessTerminalEndpoint>,
        stream: UnixStream,
    ) -> Result<HarnessEvent> {
        let mut connection = HarnessConnection::from_stream(stream);
        let request = connection.read_signal_request()?;
        let event = runtime.block_on(async {
            HarnessRequestHandler::new(harness.clone(), terminal_endpoint)
                .event_for_request(request.request)
                .await
        })?;
        connection.write_signal_event(request.exchange, request.verb, event.clone())?;
        Ok(event)
    }

    fn terminal_endpoint_from_environment() -> Option<HarnessTerminalEndpoint> {
        std::env::var_os("PERSONA_HARNESS_TERMINAL_SOCKET")
            .map(PathBuf::from)
            .map(HarnessTerminalEndpoint::pty_socket)
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
    terminal_endpoint: Option<HarnessTerminalEndpoint>,
}

impl BoundHarnessDaemon {
    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn serve_one(self) -> Result<HarnessEvent> {
        let (stream, _address) = self.listener.accept()?;
        let event = HarnessDaemon::handle_connection(
            &self.runtime,
            &self.harness,
            self.terminal_endpoint.clone(),
            stream,
        )?;
        self.runtime
            .block_on(HarnessDaemon::stop_harness(self.harness))?;
        let _ = std::fs::remove_file(&self.socket);
        Ok(event)
    }

    pub fn serve_forever(self) -> Result<()> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            let _ = HarnessDaemon::handle_connection(
                &self.runtime,
                &self.harness,
                self.terminal_endpoint.clone(),
                stream,
            )?;
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

    pub fn read_signal_request(&mut self) -> Result<ReceivedHarnessRequest> {
        self.signal.read_request(&mut self.stream)
    }

    pub fn write_signal_event(
        &mut self,
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
        event: HarnessEvent,
    ) -> Result<()> {
        let stream = self.stream.get_mut();
        self.signal.write_event(stream, exchange, verb, event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedHarnessRequest {
    exchange: ExchangeIdentifier,
    verb: SignalVerb,
    request: HarnessRequest,
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

    pub fn read_request(&self, reader: &mut impl Read) -> Result<ReceivedHarnessRequest> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Request { exchange, request } => {
                let checked = request
                    .into_checked()
                    .map_err(|(reason, _)| Error::InvalidSignalRequest { reason })?;
                let operation = checked.operations.into_head();
                Ok(ReceivedHarnessRequest {
                    exchange,
                    verb: operation.verb,
                    request: operation.payload,
                })
            }
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_event(
        &self,
        writer: &mut impl Write,
        exchange: ExchangeIdentifier,
        verb: SignalVerb,
        event: HarnessEvent,
    ) -> Result<()> {
        let frame = HarnessFrame::new(FrameBody::Reply {
            exchange,
            reply: Reply::completed(NonEmpty::single(SubReply::Ok {
                verb,
                payload: event,
            })),
        });
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
    terminal_endpoint: Option<HarnessTerminalEndpoint>,
}

impl HarnessRequestHandler {
    pub fn new(
        harness: ActorRef<Harness>,
        terminal_endpoint: Option<HarnessTerminalEndpoint>,
    ) -> Self {
        Self {
            harness,
            terminal_endpoint,
        }
    }

    pub async fn event_for_request(&self, request: HarnessRequest) -> Result<HarnessEvent> {
        match request {
            HarnessRequest::MessageDelivery(delivery) => {
                self.message_delivery_event(delivery).await
            }
            HarnessRequest::HarnessStatusQuery(query) => self.status_event(query).await,
            other => Ok(HarnessRequestUnimplemented {
                harness: Self::request_harness(&other),
                operation: other.operation_kind(),
                reason: HarnessUnimplementedReason::NotBuiltYet,
            }
            .into()),
        }
    }

    async fn message_delivery_event(&self, delivery: MessageDelivery) -> Result<HarnessEvent> {
        let state = self
            .harness
            .ask(ReadState::expecting_at_least(0))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        if !matches!(state.lifecycle, HarnessLifecycle::Running) {
            return Ok(Self::delivery_failed(
                delivery,
                DeliveryFailureReason::HarnessStoppedBeforeDelivery,
            ));
        }

        let Some(endpoint) = self.terminal_endpoint.clone() else {
            return Ok(Self::delivery_failed(
                delivery,
                DeliveryFailureReason::TransportRejected,
            ));
        };

        let binding =
            HarnessTerminalBinding::for_harness(HarnessId::new(delivery.harness.as_str()));
        let mut terminal_delivery = HarnessTerminalDelivery::new(endpoint);
        match terminal_delivery.deliver_text(&binding, delivery.body.as_str()) {
            Ok(receipt) if receipt.delivered() => Ok(DeliveryCompleted {
                harness: delivery.harness,
                message_slot: delivery.message_slot,
            }
            .into()),
            Ok(_) | Err(_) => Ok(Self::delivery_failed(
                delivery,
                DeliveryFailureReason::TransportRejected,
            )),
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

    fn delivery_failed(delivery: MessageDelivery, reason: DeliveryFailureReason) -> HarnessEvent {
        DeliveryFailed {
            harness: delivery.harness,
            message_slot: delivery.message_slot,
            reason,
        }
        .into()
    }

    fn request_harness(request: &HarnessRequest) -> HarnessName {
        match request {
            HarnessRequest::MessageDelivery(payload) => payload.harness.clone(),
            HarnessRequest::InteractionPrompt(payload) => payload.harness.clone(),
            HarnessRequest::DeliveryCancellation(payload) => payload.harness.clone(),
            HarnessRequest::HarnessStatusQuery(payload) => payload.harness.clone(),
            HarnessRequest::SubscribeHarnessTranscript(payload) => payload.harness.clone(),
            HarnessRequest::HarnessTranscriptRetraction(token) => token.harness.clone(),
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
        let mut arguments = HarnessDaemonArguments::new(&self.arguments);
        arguments.parse()
    }

    pub fn run(&self) -> Result<()> {
        self.daemon()?.run()
    }
}

struct HarnessDaemonArguments<'arguments> {
    arguments: &'arguments [OsString],
    index: usize,
    socket: Option<PathBuf>,
    harness: Option<HarnessName>,
    kind: Option<HarnessKind>,
    terminal_socket: Option<PathBuf>,
}

impl<'arguments> HarnessDaemonArguments<'arguments> {
    fn new(arguments: &'arguments [OsString]) -> Self {
        Self {
            arguments,
            index: 0,
            socket: None,
            harness: None,
            kind: None,
            terminal_socket: None,
        }
    }

    fn parse(&mut self) -> Result<HarnessDaemon> {
        while let Some(argument) = self.next() {
            match argument.to_string_lossy().as_ref() {
                "--socket" => self.socket = Some(PathBuf::from(self.required_value("--socket")?)),
                "--harness" => {
                    self.harness = Some(HarnessName::new(
                        self.required_value("--harness")?
                            .to_string_lossy()
                            .to_string(),
                    ))
                }
                "--kind" => {
                    let value = self.required_value("--kind")?;
                    let parsed = value.to_string_lossy().to_string();
                    self.kind = Some(
                        HarnessKind::from_argument_value(parsed.as_str()).ok_or_else(|| {
                            Error::UnexpectedArgument {
                                got: format!("--kind {parsed} (expected codex|claude|pi|fixture)"),
                            }
                        })?,
                    );
                }
                "--terminal-socket" => {
                    self.terminal_socket =
                        Some(PathBuf::from(self.required_value("--terminal-socket")?))
                }
                _ if self.socket.is_none()
                    && !CommandLineArgument::new(argument).starts_option() =>
                {
                    self.socket = Some(PathBuf::from(argument));
                }
                _ if self.harness.is_none()
                    && !CommandLineArgument::new(argument).starts_option() =>
                {
                    self.harness = Some(HarnessName::new(argument.to_string_lossy().to_string()));
                }
                other => {
                    return Err(Error::UnexpectedArgument {
                        got: other.to_string(),
                    });
                }
            }
        }

        let mut daemon =
            HarnessDaemon::from_socket(self.socket.take().ok_or(Error::MissingSocket)?);
        if let Some(harness) = self.harness.take() {
            daemon = daemon.with_harness(harness);
        }
        if let Some(kind) = self.kind.take() {
            daemon = daemon.with_kind(kind);
        }
        if let Some(terminal_socket) = self.terminal_socket.take() {
            daemon = daemon.with_terminal_socket(terminal_socket);
        }
        Ok(daemon)
    }

    fn next(&mut self) -> Option<&'arguments OsString> {
        let argument = self.arguments.get(self.index)?;
        self.index += 1;
        Some(argument)
    }

    fn required_value(&mut self, option: &str) -> Result<&'arguments OsString> {
        self.next().ok_or_else(|| Error::UnexpectedArgument {
            got: format!("{option} without value"),
        })
    }
}

struct CommandLineArgument<'argument> {
    value: &'argument OsString,
}

impl<'argument> CommandLineArgument<'argument> {
    fn new(value: &'argument OsString) -> Self {
        Self { value }
    }

    fn starts_option(&self) -> bool {
        self.value.to_string_lossy().starts_with("--")
    }
}
