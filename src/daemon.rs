//! Harness's daemon hooks — the only daemon code harness hand-writes.
//!
//! The uniform daemon skeleton (argv parsing, async task-backed multi-listener
//! binding, request gating, peer credentials, lifecycle, and the `ExitReport`
//! entry) is emitted into `src/schema/daemon.rs` by schema-rust-next's daemon
//! emitter. Harness adopts the `component_decoded` working tier: the ordinary
//! harness socket keeps speaking the `signal-harness` contract wire (the
//! component owns the per-connection `HarnessFrame` decode), and the existing
//! kameo actors (`Harness`, `TranscriptSubscriptionManager`) stay the engine.
//!
//! The owner-only meta listener carries the engine-management supervision
//! protocol (the second socket the manager binds): `handle_meta_connection`
//! decodes a `signal-persona` `Frame` and drives `SupervisionPhase`.

use std::collections::HashMap;
use std::path::PathBuf;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_frame::{ExchangeIdentifier, NonEmpty, Reply, SubReply};
use signal_harness::{
    DeliveryCompleted, DeliveryFailed, DeliveryFailureReason, HarnessDaemonConfiguration,
    HarnessEvent, HarnessFrame, HarnessFrameBody as FrameBody, HarnessHealth,
    HarnessInstanceConfiguration, HarnessName, HarnessReadiness, HarnessRequest,
    HarnessRequestUnimplemented, HarnessStatus, HarnessStatusQuery, HarnessUnimplementedReason,
    MessageDelivery,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::OnceCell;
use triad_runtime::{
    AcceptedConnection, FrameBody as LengthPrefixedFrameBody, FrameError, LengthPrefixedCodec,
};

use crate::schema::daemon::ComponentDaemon;
use crate::supervision::{
    HandleSupervisionRequest, ReceivedSupervisionRequest, SupervisionPhase, SupervisionProfile,
};
use crate::{
    Configuration, Error, Harness, HarnessBinding, HarnessDeliveryAdapter, HarnessIdentifier,
    HarnessKind, HarnessLifecycle, HarnessState, HarnessTerminalBinding, HarnessTerminalEndpoint,
    PiRpcProcessConfiguration, PiRpcSession, ReadState, Result, SetHarnessLifecycle,
};

/// The type-level selector for harness's emitted daemon. It carries no runtime
/// data — it is the marker the emitted `DaemonCommand<HarnessProcessDaemon>` and
/// the generated runtime dispatch on, selecting harness's `Configuration` /
/// `Engine` / `Error` types through the `ComponentDaemon` associated types.
#[derive(Debug)]
pub struct HarnessProcessDaemon;

/// Harness's daemon-facing engine: the configured harness instances and the
/// supervision profile. The `component_decoded` runtime shares this engine as
/// `&Self::Engine`; each instance and the supervision actor own their mutable
/// state behind a kameo mailbox, so no component-internal lock is required. The
/// actors start on first connection so `build_runtime` stays synchronous and
/// they spawn inside the daemon's tokio runtime.
pub struct HarnessEngine {
    instance_configurations: Vec<HarnessRuntimeConfiguration>,
    profile: SupervisionProfile,
    instances: OnceCell<BoundHarnessInstances>,
    supervision: OnceCell<ActorRef<SupervisionPhase>>,
}

impl HarnessEngine {
    /// Canonical constructor — every production launch reads a typed
    /// `HarnessDaemonConfiguration` from the daemon's binary rkyv startup file
    /// and hands the decoded record here.
    pub fn from_configuration(configuration: HarnessDaemonConfiguration) -> Self {
        Self {
            instance_configurations: configuration
                .harnesses
                .into_iter()
                .map(HarnessRuntimeConfiguration::from_contract)
                .collect(),
            profile: SupervisionProfile::harness(),
            instances: OnceCell::new(),
            supervision: OnceCell::new(),
        }
    }

    async fn instances(&self) -> Result<&BoundHarnessInstances> {
        self.instances
            .get_or_try_init(|| BoundHarnessInstances::start(self.instance_configurations.clone()))
            .await
    }

    async fn supervision(&self) -> &ActorRef<SupervisionPhase> {
        self.supervision
            .get_or_init(|| SupervisionPhase::start(self.profile.clone()))
            .await
    }

    /// Serve one ordinary working connection: decode a `signal-harness`
    /// `HarnessFrame` request off the length-prefixed envelope, route it to the
    /// addressed harness instance, and write the typed event frame back.
    async fn handle_working_connection(&self, connection: &mut AcceptedConnection) -> Result<()> {
        let body = LengthPrefixedCodec::default()
            .read_body_async(connection.stream_mut())
            .await?
            .into_bytes();
        let received = ReceivedHarnessRequest::decode(&body)?;
        let event = self.event_for_request(received.request).await?;
        WorkingHarnessEvent::new(received.exchange, event)
            .write(connection.stream_mut())
            .await
    }

    async fn event_for_request(&self, request: HarnessRequest) -> Result<HarnessEvent> {
        let harness = HarnessRequestHandler::request_harness(&request);
        match self.instances().await?.instance(&harness) {
            Some(instance) => instance
                .ask(HandleHarnessRequest { request })
                .await
                .map_err(|error| Error::ActorCall(error.to_string())),
            None => Ok(Self::unavailable_event(request)),
        }
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

    /// Serve one owner-only supervision (meta) connection: decode an
    /// engine-management `Frame` request and drive the supervision actor. The
    /// manager binds this socket to announce, query readiness/health, and stop
    /// the component.
    async fn handle_meta_connection(&self, connection: &mut AcceptedConnection) -> Result<()> {
        let body = LengthPrefixedCodec::default()
            .read_body_async(connection.stream_mut())
            .await?
            .into_bytes();
        let received = ReceivedSupervisionRequest::decode(&body)?;
        let reply = self
            .supervision()
            .await
            .ask(HandleSupervisionRequest {
                request: received.request,
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        WorkingSupervisionReply::new(received.exchange, reply.reply)
            .write(connection.stream_mut())
            .await
    }
}

impl ComponentDaemon for HarnessProcessDaemon {
    type Configuration = Configuration;
    type ConfigurationError = Error;
    type Engine = HarnessEngine;
    type Error = Error;

    const PROCESS_NAME: &'static str = "harness-daemon";

    fn load_configuration(
        path: &std::path::Path,
    ) -> std::result::Result<Self::Configuration, Self::ConfigurationError> {
        Configuration::from_binary_path(path)
    }

    fn build_runtime(
        configuration: &Self::Configuration,
    ) -> std::result::Result<Self::Engine, Self::Error> {
        Ok(HarnessEngine::from_configuration(
            configuration.raw().clone(),
        ))
    }

    async fn handle_working_connection(
        engine: &Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<()> {
        engine.handle_working_connection(&mut connection).await
    }

    async fn handle_meta_connection(
        engine: &Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<()> {
        engine.handle_meta_connection(&mut connection).await
    }
}

/// One harness instance's binding plus the optional delivery transport it was
/// configured with. The bound instance turns this into a running `Harness`
/// actor and its delivery adapter.
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

/// The set of running harness instance actors keyed by name. Each instance
/// actor owns its `Harness` lifecycle ref and its delivery adapter, so request
/// dispatch is a typed `ask` against the instance mailbox — no shared lock.
pub struct BoundHarnessInstances {
    by_name: HashMap<HarnessName, ActorRef<HarnessInstance>>,
}

impl BoundHarnessInstances {
    async fn start(configurations: Vec<HarnessRuntimeConfiguration>) -> Result<Self> {
        let mut by_name = HashMap::with_capacity(configurations.len());
        for configuration in configurations {
            let harness = configuration.start_harness().await?;
            let delivery_adapter = configuration.delivery_adapter()?;
            let instance = HarnessInstance::start(harness, delivery_adapter).await;
            by_name.insert(configuration.harness().clone(), instance);
        }
        Ok(Self { by_name })
    }

    fn instance(&self, harness: &HarnessName) -> Option<&ActorRef<HarnessInstance>> {
        self.by_name.get(harness)
    }
}

/// One harness instance's actor. It owns the lifecycle `Harness` actor ref and
/// the mutable delivery adapter; its mailbox serialises every request against
/// that delivery state, so the shared engine needs no component-internal lock.
pub struct HarnessInstance {
    harness: ActorRef<Harness>,
    delivery_adapter: Option<HarnessDeliveryAdapter>,
}

impl HarnessInstance {
    fn new(harness: ActorRef<Harness>, delivery_adapter: Option<HarnessDeliveryAdapter>) -> Self {
        Self {
            harness,
            delivery_adapter,
        }
    }

    async fn start(
        harness: ActorRef<Harness>,
        delivery_adapter: Option<HarnessDeliveryAdapter>,
    ) -> ActorRef<Self> {
        let reference = Self::spawn(Self::new(harness, delivery_adapter));
        reference.wait_for_startup().await;
        reference
    }

    async fn event_for_request(&mut self, request: HarnessRequest) -> Result<HarnessEvent> {
        HarnessRequestHandler::new(self.harness.clone())
            .event_for_request(request, self.delivery_adapter.as_mut())
            .await
    }
}

impl Actor for HarnessInstance {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        instance: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(instance)
    }
}

/// Drive one harness request through the addressed instance, returning the
/// typed harness event. The instance mailbox serialises deliveries.
#[derive(Debug)]
pub struct HandleHarnessRequest {
    pub request: HarnessRequest,
}

impl Message<HandleHarnessRequest> for HarnessInstance {
    type Reply = Result<HarnessEvent>;

    async fn handle(
        &mut self,
        message: HandleHarnessRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.event_for_request(message.request).await
    }
}

/// One decoded ordinary harness request plus its exchange identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedHarnessRequest {
    exchange: ExchangeIdentifier,
    request: HarnessRequest,
}

impl ReceivedHarnessRequest {
    pub fn decode(body: &[u8]) -> Result<Self> {
        match HarnessFrame::decode(body)?.into_body() {
            FrameBody::Request { exchange, request } => {
                let (request, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedSignalFrame {
                        got: format!("expected one harness payload, got {}", tail.len() + 1),
                    });
                }
                Ok(Self { exchange, request })
            }
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn exchange(&self) -> ExchangeIdentifier {
        self.exchange
    }

    pub fn request(&self) -> &HarnessRequest {
        &self.request
    }
}

/// One ordinary harness event, framed and written back to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingHarnessEvent {
    exchange: ExchangeIdentifier,
    event: HarnessEvent,
}

impl WorkingHarnessEvent {
    pub fn new(exchange: ExchangeIdentifier, event: HarnessEvent) -> Self {
        Self { exchange, event }
    }

    async fn write(self, stream: &mut tokio::net::UnixStream) -> Result<()> {
        let frame = HarnessFrame::new(FrameBody::Reply {
            exchange: self.exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(self.event))),
        });
        LengthPrefixedCodec::default()
            .write_body_async(stream, &LengthPrefixedFrameBody::new(frame.encode()?))
            .await?;
        stream.flush().await.map_err(FrameError::from)?;
        Ok(())
    }
}

/// One supervision reply, framed and written back to the manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingSupervisionReply {
    exchange: ExchangeIdentifier,
    reply: signal_persona::Reply,
}

impl WorkingSupervisionReply {
    pub fn new(exchange: ExchangeIdentifier, reply: signal_persona::Reply) -> Self {
        Self { exchange, reply }
    }

    async fn write(self, stream: &mut tokio::net::UnixStream) -> Result<()> {
        let frame = signal_persona::Frame::new(signal_persona::FrameBody::Reply {
            exchange: self.exchange,
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(self.reply))),
        });
        LengthPrefixedCodec::default()
            .write_body_async(stream, &LengthPrefixedFrameBody::new(frame.encode()?))
            .await?;
        stream.flush().await.map_err(FrameError::from)?;
        Ok(())
    }
}

/// Turns one decoded harness request into the harness event the daemon replies
/// with, driving the addressed `Harness` lifecycle actor and the configured
/// delivery adapter.
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
