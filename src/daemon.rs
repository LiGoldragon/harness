use std::collections::HashMap;
use std::io::{BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use kameo::actor::ActorRef;
use signal_frame::{ExchangeIdentifier, NonEmpty, Reply, SubReply};
use signal_harness::{
    DeliveryCompleted, DeliveryFailed, DeliveryFailureReason, HarnessDaemonConfiguration,
    HarnessEvent, HarnessFrame, HarnessFrameBody as FrameBody, HarnessHealth,
    HarnessInstanceConfiguration, HarnessName, HarnessReadiness, HarnessRequest,
    HarnessRequestUnimplemented, HarnessStatus, HarnessStatusQuery, HarnessUnimplementedReason,
    MessageDelivery,
};

use crate::{
    Error, Harness, HarnessBinding, HarnessDeliveryAdapter, HarnessIdentifier, HarnessKind,
    HarnessLifecycle, HarnessState, HarnessTerminalBinding, HarnessTerminalEndpoint,
    PiRpcProcessConfiguration, PiRpcSession, ReadState, Result, SetHarnessLifecycle,
    supervision::{SupervisionListener, SupervisionProfile, SupervisionSocketMode},
};

#[derive(Debug)]
pub struct HarnessDaemon {
    socket: PathBuf,
    socket_mode: Option<SocketMode>,
    harnesses: Vec<HarnessRuntimeConfiguration>,
    supervision: Option<SupervisionListener>,
}

impl HarnessDaemon {
    /// Canonical constructor — every production launch reads a typed
    /// `HarnessDaemonConfiguration` from the daemon's binary rkyv startup file
    /// and hands the decoded record here.
    pub fn from_configuration(configuration: HarnessDaemonConfiguration) -> Self {
        let supervision = SupervisionListener::new(
            SupervisionProfile::harness(),
            PathBuf::from(configuration.supervision_socket_path.as_str()),
            SupervisionSocketMode::from_octal(configuration.supervision_socket_mode.into_u32()),
        );
        Self {
            socket: PathBuf::from(configuration.harness_socket_path.as_str()),
            socket_mode: Some(SocketMode::from_octal(
                configuration.harness_socket_mode.into_u32(),
            )),
            harnesses: configuration
                .harnesses
                .into_iter()
                .map(HarnessRuntimeConfiguration::from_contract)
                .collect(),
            supervision: Some(supervision),
        }
    }

    pub fn from_socket(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            socket_mode: None,
            harnesses: vec![HarnessRuntimeConfiguration::new(
                HarnessName::new("harness"),
                HarnessKind::Fixture,
            )],
            supervision: None,
        }
    }

    pub fn with_harness(mut self, harness: HarnessName) -> Self {
        self.primary_harness_mut().harness = harness;
        self
    }

    pub fn with_kind(mut self, kind: HarnessKind) -> Self {
        self.primary_harness_mut().kind = kind;
        self
    }

    pub fn with_socket_mode(mut self, socket_mode: SocketMode) -> Self {
        self.socket_mode = Some(socket_mode);
        self
    }

    pub fn with_terminal_socket(mut self, path: impl Into<PathBuf>) -> Self {
        self.primary_harness_mut().terminal_endpoint =
            Some(HarnessTerminalEndpoint::pty_socket(path));
        self
    }

    pub fn with_pi_rpc_process(mut self, configuration: PiRpcProcessConfiguration) -> Self {
        self.primary_harness_mut().pi_rpc_configuration = Some(configuration);
        self
    }

    pub fn with_harnesses(mut self, harnesses: Vec<HarnessRuntimeConfiguration>) -> Self {
        self.harnesses = harnesses;
        self
    }

    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn harness(&self) -> &HarnessName {
        &self
            .harnesses
            .first()
            .expect("harness daemon has at least one harness configuration")
            .harness
    }

    pub fn kind(&self) -> &HarnessKind {
        &self
            .harnesses
            .first()
            .expect("harness daemon has at least one harness configuration")
            .kind
    }

    pub fn run(self) -> Result<()> {
        let supervision = self.supervision.clone();
        let bound = self.bind()?;
        let _supervision = supervision.map(SupervisionListener::spawn).transpose()?;
        eprintln!("harness-daemon socket={}", bound.socket.display());
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
        let instances = runtime.block_on(BoundHarnessInstances::start(self.harnesses))?;
        Ok(BoundHarnessDaemon {
            socket: self.socket,
            runtime,
            listener,
            instances,
        })
    }

    pub fn serve_one(self) -> Result<HarnessEvent> {
        self.bind()?.serve_one()
    }

    async fn stop_harness(reference: ActorRef<Harness>) -> Result<()> {
        reference
            .stop_gracefully()
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        reference.wait_for_shutdown().await;
        Ok(())
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        instances: &mut BoundHarnessInstances,
        stream: UnixStream,
    ) -> Result<HarnessEvent> {
        let mut connection = HarnessConnection::from_stream(stream);
        let request = connection.read_signal_request()?;
        let event = match instances
            .instance_mut(&HarnessRequestHandler::request_harness(&request.request))
        {
            Some(instance) => runtime.block_on(async {
                HarnessRequestHandler::new(instance.harness.clone())
                    .event_for_request(request.request, instance.delivery_adapter.as_mut())
                    .await
            })?,
            None => Self::unavailable_event(request.request),
        };
        connection.write_signal_event(request.exchange, event.clone())?;
        Ok(event)
    }

    fn unavailable_event(request: HarnessRequest) -> HarnessEvent {
        match request {
            HarnessRequest::MessageDelivery(delivery) => DeliveryFailed {
                harness: delivery.harness,
                message_slot: delivery.message_slot,
                reason: DeliveryFailureReason::HarnessUnavailable,
            }
            .into(),
            HarnessRequest::HarnessStatusQuery(query) => HarnessStatus {
                harness: query.harness,
                health: HarnessHealth::Stopped,
                readiness: HarnessReadiness::Unavailable,
            }
            .into(),
            other => HarnessRequestUnimplemented {
                harness: HarnessRequestHandler::request_harness(&other),
                operation: other.operation_kind(),
                reason: HarnessUnimplementedReason::NotBuiltYet,
            }
            .into(),
        }
    }

    fn primary_harness_mut(&mut self) -> &mut HarnessRuntimeConfiguration {
        self.harnesses
            .first_mut()
            .expect("harness daemon has at least one harness configuration")
    }
}

#[derive(Debug, Clone)]
pub struct HarnessRuntimeConfiguration {
    harness: HarnessName,
    kind: HarnessKind,
    terminal_endpoint: Option<HarnessTerminalEndpoint>,
    pi_rpc_configuration: Option<PiRpcProcessConfiguration>,
}

impl HarnessRuntimeConfiguration {
    pub fn new(harness: HarnessName, kind: HarnessKind) -> Self {
        Self {
            harness,
            kind,
            terminal_endpoint: None,
            pi_rpc_configuration: None,
        }
    }

    pub fn from_contract(configuration: HarnessInstanceConfiguration) -> Self {
        let harness_name = configuration.harness_name.clone();
        let terminal_endpoint = configuration
            .terminal_socket_path
            .map(|path| HarnessTerminalEndpoint::pty_socket(path.as_str()));
        let pi_rpc_configuration = configuration.pi_rpc_adapter.map(|adapter| {
            let process_configuration = PiRpcProcessConfiguration::new(
                adapter.command_path.as_str(),
                adapter.session_directory_path.as_str(),
            )
            .with_session_name(harness_name.as_str())
            .with_delivery_command(adapter.delivery_mode.into());
            match adapter.model_pattern {
                Some(model_pattern) => {
                    process_configuration.with_model_pattern(model_pattern.as_str())
                }
                None => process_configuration,
            }
        });
        Self {
            harness: configuration.harness_name,
            kind: HarnessKind::from_contract(configuration.harness_kind),
            terminal_endpoint,
            pi_rpc_configuration,
        }
    }

    pub fn with_terminal_socket(mut self, path: impl Into<PathBuf>) -> Self {
        self.terminal_endpoint = Some(HarnessTerminalEndpoint::pty_socket(path));
        self
    }

    pub fn with_pi_rpc_process(mut self, configuration: PiRpcProcessConfiguration) -> Self {
        self.pi_rpc_configuration = Some(configuration);
        self
    }

    pub fn harness(&self) -> &HarnessName {
        &self.harness
    }

    pub fn kind(&self) -> &HarnessKind {
        &self.kind
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

    fn binding(&self) -> HarnessBinding {
        HarnessBinding::new(
            HarnessIdentifier::new(self.harness.as_str()),
            self.kind.clone(),
            std::env::current_dir()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|_| ".".to_string()),
        )
    }

    fn delivery_adapter(&self) -> Result<Option<HarnessDeliveryAdapter>> {
        if let Some(configuration) = self.pi_rpc_configuration.clone() {
            return Ok(Some(HarnessDeliveryAdapter::pi_rpc(PiRpcSession::spawn(
                configuration,
            )?)));
        }
        Ok(self
            .terminal_endpoint
            .clone()
            .map(HarnessDeliveryAdapter::terminal))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketMode(u32);

impl SocketMode {
    pub const fn from_octal(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_octal(self) -> u32 {
        self.0
    }
}

pub struct BoundHarnessDaemon {
    socket: PathBuf,
    runtime: tokio::runtime::Runtime,
    listener: UnixListener,
    instances: BoundHarnessInstances,
}

impl BoundHarnessDaemon {
    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn serve_one(self) -> Result<HarnessEvent> {
        let mut events = self.serve_requests(1)?;
        Ok(events
            .pop()
            .expect("serve_requests(1) returns one harness event"))
    }

    pub fn serve_requests(mut self, count: usize) -> Result<Vec<HarnessEvent>> {
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let (stream, _address) = self.listener.accept()?;
            events.push(HarnessDaemon::handle_connection(
                &self.runtime,
                &mut self.instances,
                stream,
            )?);
        }
        self.runtime.block_on(self.instances.stop_all())?;
        let _ = std::fs::remove_file(&self.socket);
        Ok(events)
    }

    pub fn serve_forever(mut self) -> Result<()> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            let _ = HarnessDaemon::handle_connection(&self.runtime, &mut self.instances, stream)?;
        }
        Ok(())
    }
}

pub struct BoundHarnessInstances {
    by_name: HashMap<HarnessName, BoundHarnessInstance>,
}

impl BoundHarnessInstances {
    async fn start(configurations: Vec<HarnessRuntimeConfiguration>) -> Result<Self> {
        let mut by_name = HashMap::with_capacity(configurations.len());
        for configuration in configurations {
            let harness = configuration.start_harness().await?;
            let delivery_adapter = configuration.delivery_adapter()?;
            by_name.insert(
                configuration.harness().clone(),
                BoundHarnessInstance {
                    harness,
                    delivery_adapter,
                },
            );
        }
        Ok(Self { by_name })
    }

    fn instance_mut(&mut self, harness: &HarnessName) -> Option<&mut BoundHarnessInstance> {
        self.by_name.get_mut(harness)
    }

    async fn stop_all(&mut self) -> Result<()> {
        let references = self
            .by_name
            .drain()
            .map(|(_harness, instance)| instance.harness)
            .collect::<Vec<_>>();
        for reference in references {
            HarnessDaemon::stop_harness(reference).await?;
        }
        Ok(())
    }
}

pub struct BoundHarnessInstance {
    harness: ActorRef<Harness>,
    delivery_adapter: Option<HarnessDeliveryAdapter>,
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
        event: HarnessEvent,
    ) -> Result<()> {
        let stream = self.stream.get_mut();
        self.signal.write_event(stream, exchange, event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedHarnessRequest {
    exchange: ExchangeIdentifier,
    request: HarnessRequest,
}

impl ReceivedHarnessRequest {
    pub fn exchange(&self) -> &ExchangeIdentifier {
        &self.exchange
    }

    pub fn request(&self) -> &HarnessRequest {
        &self.request
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

    pub fn read_request(&self, reader: &mut impl Read) -> Result<ReceivedHarnessRequest> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Request { exchange, request } => Ok(ReceivedHarnessRequest {
                exchange,
                request: request.payloads().head().clone(),
            }),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_event(
        &self,
        writer: &mut impl Write,
        exchange: ExchangeIdentifier,
        event: HarnessEvent,
    ) -> Result<()> {
        let frame = HarnessFrame::new(FrameBody::Reply {
            exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(event))),
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
}

impl HarnessRequestHandler {
    pub fn new(harness: ActorRef<Harness>) -> Self {
        Self { harness }
    }

    pub async fn event_for_request(
        &self,
        request: HarnessRequest,
        delivery_adapter: Option<&mut HarnessDeliveryAdapter>,
    ) -> Result<HarnessEvent> {
        match request {
            HarnessRequest::MessageDelivery(delivery) => {
                self.message_delivery_event(delivery, delivery_adapter)
                    .await
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

    async fn message_delivery_event(
        &self,
        delivery: MessageDelivery,
        delivery_adapter: Option<&mut HarnessDeliveryAdapter>,
    ) -> Result<HarnessEvent> {
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

        let Some(delivery_adapter) = delivery_adapter else {
            return Ok(Self::delivery_failed(
                delivery,
                DeliveryFailureReason::TransportRejected,
            ));
        };

        let binding =
            HarnessTerminalBinding::for_harness(HarnessIdentifier::new(delivery.harness.as_str()));
        match delivery_adapter.deliver_text(&binding, delivery.body.as_str()) {
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

    pub fn request_harness(request: &HarnessRequest) -> HarnessName {
        match request {
            HarnessRequest::MessageDelivery(payload) => payload.harness.clone(),
            HarnessRequest::InteractionPrompt(payload) => payload.harness.clone(),
            HarnessRequest::DeliveryCancellation(payload) => payload.harness.clone(),
            HarnessRequest::HarnessStatusQuery(payload) => payload.harness.clone(),
            HarnessRequest::WatchHarnessTranscript(payload) => payload.harness.clone(),
            HarnessRequest::UnwatchHarnessTranscript(token) => token.harness.clone(),
        }
    }
}
