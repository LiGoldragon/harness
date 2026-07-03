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
//! The owner-only meta listener carries the canonical `meta-signal-harness`
//! policy contract and falls back to the engine-management supervision
//! protocol while the component manager still carries both surfaces.

use std::collections::HashMap;
use std::path::PathBuf;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use meta_signal_harness::{
    MetaHarnessFrame, MetaHarnessFrameBody, MetaHarnessReply, MetaHarnessRequest,
    RequestUnimplemented, UnimplementedReason,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, SessionEpoch,
    StreamEventIdentifier, SubReply, SubscriptionTokenInner,
};
use signal_harness::{
    ClaudeSessionObservation, DeliveryCompleted, DeliveryFailed, DeliveryFailureReason,
    HarnessDaemonConfiguration, HarnessEvent, HarnessFrame, HarnessFrameBody as FrameBody,
    HarnessHealth, HarnessInstanceConfiguration, HarnessName, HarnessOperationKind,
    HarnessReadiness, HarnessRequest, HarnessRequestUnimplemented, HarnessStatus,
    HarnessStatusQuery, HarnessStreamEvent, HarnessUnimplementedReason, MessageDelivery,
    TranscriptObservation,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::OnceCell;
use tokio::sync::mpsc;
use triad_runtime::{
    AcceptedConnection, FrameBody as LengthPrefixedFrameBody, FrameError, LengthPrefixedCodec,
};

use crate::schema::daemon::ComponentDaemon;
use crate::supervision::{
    HandleSupervisionRequest, ReceivedSupervisionRequest, SupervisionPhase, SupervisionProfile,
};
use crate::{
    CloseTranscriptSubscription, OpenTranscriptSubscription, OpenedTranscriptSubscription,
    PublishStreamEvent, TranscriptDeliveryEvent, TranscriptDeltaPublisher,
    TranscriptPublicationReceipt, TranscriptSubscriptionManager, TranscriptSubscriptionSink,
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
        self.handle_working_stream(connection.stream_mut()).await
    }

    /// Serve one ordinary working stream. Non-stream requests complete with
    /// one reply frame. `WatchHarnessTranscript` keeps the accepted stream
    /// attached to the subscription sink so transcript deltas and the final
    /// retraction acknowledgement ride the original connection.
    pub async fn handle_working_stream(&self, stream: &mut tokio::net::UnixStream) -> Result<()> {
        let body = LengthPrefixedCodec::default()
            .read_body_async(stream)
            .await?
            .into_bytes();
        let received = ReceivedHarnessRequest::decode(&body)?;
        match received.request {
            HarnessRequest::WatchHarnessTranscript(watch) => {
                self.handle_transcript_stream(received.exchange, watch, stream)
                    .await
            }
            request => {
                let event = self.event_for_request(request).await?;
                WorkingHarnessEvent::new(received.exchange, event)
                    .write(stream)
                    .await
            }
        }
    }

    async fn handle_transcript_stream(
        &self,
        exchange: ExchangeIdentifier,
        watch: signal_harness::WatchHarnessTranscript,
        stream: &mut tokio::net::UnixStream,
    ) -> Result<()> {
        let harness = watch.harness.clone();
        let Some(instance) = self.instances().await?.instance(&harness).cloned() else {
            return WorkingHarnessEvent::new(
                exchange,
                Self::unavailable_event(HarnessRequest::WatchHarnessTranscript(watch)),
            )
            .write(stream)
            .await;
        };
        let mut transcript_stream = HarnessTranscriptWireStream::new(harness);
        transcript_stream
            .open_subscription(exchange, watch, &instance)
            .await?;
        transcript_stream.serve(stream, instance).await
    }

    /// Push one transcript line onto the addressed harness's stream.
    pub async fn publish_transcript_observation(
        &self,
        observation: TranscriptObservation,
    ) -> Result<TranscriptPublicationReceipt> {
        let harness = observation.harness.clone();
        self.publish_stream_event(&harness, observation.into())
            .await
    }

    /// Push one per-turn Claude session observation onto the addressed
    /// harness's stream. It rides the same `HarnessTranscriptStream` as
    /// transcript lines — the Mentci live view renders it, and orchestrate's
    /// session store later consumes the same pushed event.
    pub async fn publish_claude_session_observation(
        &self,
        observation: ClaudeSessionObservation,
    ) -> Result<TranscriptPublicationReceipt> {
        let harness = observation.harness.clone();
        self.publish_stream_event(&harness, observation.into())
            .await
    }

    async fn publish_stream_event(
        &self,
        harness: &HarnessName,
        event: HarnessStreamEvent,
    ) -> Result<TranscriptPublicationReceipt> {
        let Some(instance) = self.instances().await?.instance(harness) else {
            return Ok(TranscriptPublicationReceipt {
                published: false,
                fanned_out: 0,
            });
        };
        instance
            .ask(PublishHarnessStreamEvent { event })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
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

    /// Serve one owner-only meta connection. The canonical meta contract is
    /// `meta-signal-harness`; the older engine-management supervision protocol
    /// still falls through here while the component manager carries both
    /// surfaces during the daemon-shell migration.
    async fn handle_meta_connection(&self, connection: &mut AcceptedConnection) -> Result<()> {
        let body = LengthPrefixedCodec::default()
            .read_body_async(connection.stream_mut())
            .await?
            .into_bytes();
        match ReceivedMetaHarnessRequest::decode(&body) {
            Ok(received) => {
                let reply = MetaHarnessReply::RequestUnimplemented(RequestUnimplemented {
                    operation: received.request.kind(),
                    reason: UnimplementedReason::NotBuiltYet,
                });
                return WorkingMetaHarnessReply::new(received.exchange, reply)
                    .write(connection.stream_mut())
                    .await;
            }
            Err(MetaHarnessDecode::NotMeta) => {}
            Err(MetaHarnessDecode::UnexpectedFrame(got)) => {
                return Err(Error::UnexpectedSignalFrame { got });
            }
        }
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

/// One decoded meta-harness request plus its exchange identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedMetaHarnessRequest {
    exchange: ExchangeIdentifier,
    request: MetaHarnessRequest,
}

impl ReceivedMetaHarnessRequest {
    pub fn decode(body: &[u8]) -> std::result::Result<Self, MetaHarnessDecode> {
        let frame = MetaHarnessFrame::decode(body).map_err(|_| MetaHarnessDecode::NotMeta)?;
        match frame.into_body() {
            MetaHarnessFrameBody::Request { exchange, request } => {
                let (request, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(MetaHarnessDecode::UnexpectedFrame(format!(
                        "expected one meta-harness payload, got {}",
                        tail.len() + 1
                    )));
                }
                Ok(Self { exchange, request })
            }
            other => Err(MetaHarnessDecode::UnexpectedFrame(format!("{other:?}"))),
        }
    }

    pub fn exchange(&self) -> ExchangeIdentifier {
        self.exchange
    }

    pub fn request(&self) -> &MetaHarnessRequest {
        &self.request
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetaHarnessDecode {
    NotMeta,
    UnexpectedFrame(String),
}

/// One meta-harness reply, framed and written back to the owner client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkingMetaHarnessReply {
    exchange: ExchangeIdentifier,
    reply: MetaHarnessReply,
}

impl WorkingMetaHarnessReply {
    pub fn new(exchange: ExchangeIdentifier, reply: MetaHarnessReply) -> Self {
        Self { exchange, reply }
    }

    async fn write(self, stream: &mut tokio::net::UnixStream) -> Result<()> {
        let frame = MetaHarnessFrame::new(MetaHarnessFrameBody::Reply {
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
            let subscription_manager =
                TranscriptSubscriptionManager::spawn(TranscriptSubscriptionManager::new());
            subscription_manager.wait_for_startup().await;
            let transcript_publisher = TranscriptDeltaPublisher::spawn(
                TranscriptDeltaPublisher::new(subscription_manager.clone()),
            );
            transcript_publisher.wait_for_startup().await;
            let instance = HarnessInstance::start(
                harness,
                delivery_adapter,
                subscription_manager,
                transcript_publisher,
            )
            .await;
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
    subscription_manager: ActorRef<TranscriptSubscriptionManager>,
    transcript_publisher: ActorRef<TranscriptDeltaPublisher>,
}

impl HarnessInstance {
    fn new(
        harness: ActorRef<Harness>,
        delivery_adapter: Option<HarnessDeliveryAdapter>,
        subscription_manager: ActorRef<TranscriptSubscriptionManager>,
        transcript_publisher: ActorRef<TranscriptDeltaPublisher>,
    ) -> Self {
        Self {
            harness,
            delivery_adapter,
            subscription_manager,
            transcript_publisher,
        }
    }

    async fn start(
        harness: ActorRef<Harness>,
        delivery_adapter: Option<HarnessDeliveryAdapter>,
        subscription_manager: ActorRef<TranscriptSubscriptionManager>,
        transcript_publisher: ActorRef<TranscriptDeltaPublisher>,
    ) -> ActorRef<Self> {
        let reference = Self::spawn(Self::new(
            harness,
            delivery_adapter,
            subscription_manager,
            transcript_publisher,
        ));
        reference.wait_for_startup().await;
        reference
    }

    async fn event_for_request(&mut self, request: HarnessRequest) -> Result<HarnessEvent> {
        HarnessRequestHandler::new(self.harness.clone(), self.subscription_manager.clone())
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

#[derive(Debug)]
pub struct OpenHarnessTranscriptStream {
    pub watch: signal_harness::WatchHarnessTranscript,
    pub sink: TranscriptSubscriptionSink,
}

impl Message<OpenHarnessTranscriptStream> for HarnessInstance {
    type Reply = Result<OpenedTranscriptSubscription>;

    async fn handle(
        &mut self,
        message: OpenHarnessTranscriptStream,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let subscription_manager = self.subscription_manager.clone();
        subscription_manager
            .ask(OpenTranscriptSubscription {
                harness: message.watch.harness,
                sink: message.sink,
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
    }
}

#[derive(Debug)]
pub struct CloseHarnessTranscriptStream {
    pub token: signal_harness::HarnessTranscriptToken,
}

impl Message<CloseHarnessTranscriptStream> for HarnessInstance {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        message: CloseHarnessTranscriptStream,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let subscription_manager = self.subscription_manager.clone();
        let closed = subscription_manager
            .ask(CloseTranscriptSubscription {
                token: message.token,
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        if closed.closed {
            Ok(())
        } else {
            Err(Error::UnexpectedSignalFrame {
                got: "unwatch did not match an open transcript subscription".to_string(),
            })
        }
    }
}

/// Push one `HarnessStreamEvent` onto this instance's transcript stream. The
/// carried event is either a `TranscriptObservation` (a transcript line) or a
/// `ClaudeSessionObservation` (a per-turn Claude session observation); both
/// ride the one fan-out plane the publisher owns.
#[derive(Debug)]
pub struct PublishHarnessStreamEvent {
    pub event: HarnessStreamEvent,
}

impl Message<PublishHarnessStreamEvent> for HarnessInstance {
    type Reply = Result<TranscriptPublicationReceipt>;

    async fn handle(
        &mut self,
        message: PublishHarnessStreamEvent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let transcript_publisher = self.transcript_publisher.clone();
        transcript_publisher
            .ask(PublishStreamEvent {
                event: message.event,
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))
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

    async fn write<Writer>(self, stream: &mut Writer) -> Result<()>
    where
        Writer: AsyncWrite + Unpin,
    {
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

struct HarnessTranscriptWireStream {
    bound_harness: HarnessName,
    sender: mpsc::UnboundedSender<TranscriptWireDelivery>,
    receiver: mpsc::UnboundedReceiver<TranscriptWireDelivery>,
    subscriptions: Vec<HarnessTranscriptWireSubscription>,
    next_event_sequence: LaneSequence,
}

struct HarnessTranscriptWireSubscription {
    token: signal_harness::HarnessTranscriptToken,
    watch_exchange: ExchangeIdentifier,
    close_exchange: Option<ExchangeIdentifier>,
}

struct TranscriptWireDelivery {
    token: signal_harness::HarnessTranscriptToken,
    event: TranscriptDeliveryEvent,
}

impl TranscriptWireDelivery {
    fn final_ack(&self) -> bool {
        matches!(self.event, TranscriptDeliveryEvent::FinalAcknowledgement(_))
    }
}

impl HarnessTranscriptWireSubscription {
    fn new(
        token: signal_harness::HarnessTranscriptToken,
        watch_exchange: ExchangeIdentifier,
    ) -> Self {
        Self {
            token,
            watch_exchange,
            close_exchange: None,
        }
    }
}

struct TranscriptDeliveryForwarder {
    sender: mpsc::UnboundedSender<TranscriptWireDelivery>,
}

impl TranscriptDeliveryForwarder {
    fn new(sender: mpsc::UnboundedSender<TranscriptWireDelivery>) -> Self {
        Self { sender }
    }

    fn spawn(
        self,
        token: signal_harness::HarnessTranscriptToken,
        mut receiver: mpsc::UnboundedReceiver<TranscriptDeliveryEvent>,
    ) {
        let sender = self.sender;
        let _forwarder = tokio::spawn(async move {
            while let Some(event) = receiver.recv().await {
                if sender
                    .send(TranscriptWireDelivery {
                        token: token.clone(),
                        event,
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
    }
}

impl HarnessTranscriptWireStream {
    fn new(bound_harness: HarnessName) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            bound_harness,
            sender,
            receiver,
            subscriptions: Vec::new(),
            next_event_sequence: LaneSequence::first(),
        }
    }

    async fn open_subscription(
        &mut self,
        exchange: ExchangeIdentifier,
        watch: signal_harness::WatchHarnessTranscript,
        instance: &ActorRef<HarnessInstance>,
    ) -> Result<()> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let sink = TranscriptSubscriptionSink::channel(sender);
        let opened = instance
            .ask(OpenHarnessTranscriptStream { watch, sink })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        let token = opened.token.clone();
        self.subscriptions
            .push(HarnessTranscriptWireSubscription::new(
                token.clone(),
                exchange,
            ));
        TranscriptDeliveryForwarder::new(self.sender.clone()).spawn(token, receiver);
        Ok(())
    }

    fn subscription(
        &self,
        token: &signal_harness::HarnessTranscriptToken,
    ) -> Option<&HarnessTranscriptWireSubscription> {
        self.subscriptions
            .iter()
            .find(|subscription| subscription.token == *token)
    }

    fn subscription_mut(
        &mut self,
        token: &signal_harness::HarnessTranscriptToken,
    ) -> Option<&mut HarnessTranscriptWireSubscription> {
        self.subscriptions
            .iter_mut()
            .find(|subscription| subscription.token == *token)
    }

    fn remove_subscription(&mut self, token: &signal_harness::HarnessTranscriptToken) {
        self.subscriptions
            .retain(|subscription| subscription.token != *token);
    }

    fn tokens(&self) -> Vec<signal_harness::HarnessTranscriptToken> {
        self.subscriptions
            .iter()
            .map(|subscription| subscription.token.clone())
            .collect()
    }

    fn unknown_unwatch_event(token: signal_harness::HarnessTranscriptToken) -> HarnessEvent {
        HarnessRequestUnimplemented {
            harness: token.harness,
            operation: HarnessOperationKind::UnwatchHarnessTranscript,
            reason: HarnessUnimplementedReason::NotBuiltYet,
        }
        .into()
    }

    fn cross_harness_watch_event(watch: signal_harness::WatchHarnessTranscript) -> HarnessEvent {
        HarnessRequestUnimplemented {
            harness: watch.harness,
            operation: HarnessOperationKind::WatchHarnessTranscript,
            reason: HarnessUnimplementedReason::NotBuiltYet,
        }
        .into()
    }

    fn subscription_token_inner(
        token: &signal_harness::HarnessTranscriptToken,
    ) -> SubscriptionTokenInner {
        SubscriptionTokenInner::new(token.subscription.into_u64())
    }

    async fn serve(
        mut self,
        stream: &mut tokio::net::UnixStream,
        instance: ActorRef<HarnessInstance>,
    ) -> Result<()> {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let codec = LengthPrefixedCodec::default();
        loop {
            tokio::select! {
                event = self.receiver.recv() => {
                    let Some(delivery) = event else {
                        return Ok(());
                    };
                    let final_ack = delivery.final_ack();
                    let token = delivery.token.clone();
                    if let Err(error) = self.write_delivery_event(&mut writer, delivery).await {
                        self.close_after_stream_error(&instance).await;
                        return Err(error);
                    }
                    if final_ack {
                        self.remove_subscription(&token);
                        if self.subscriptions.is_empty() {
                            return Ok(());
                        }
                    }
                }
                body = codec.read_body_async(&mut reader) => {
                    let body = match body {
                        Ok(body) => body.into_bytes(),
                        Err(error) => {
                            self.close_after_stream_error(&instance).await;
                            return Err(error.into());
                        }
                    };
                    let received = ReceivedHarnessRequest::decode(&body)?;
                    if let Err(error) = self.handle_request(received, &instance, &mut writer).await {
                        self.close_after_stream_error(&instance).await;
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn handle_request<Writer>(
        &mut self,
        received: ReceivedHarnessRequest,
        instance: &ActorRef<HarnessInstance>,
        writer: &mut Writer,
    ) -> Result<()>
    where
        Writer: AsyncWrite + Unpin,
    {
        match received.request {
            HarnessRequest::UnwatchHarnessTranscript(token)
                if self.subscription(&token).is_some() =>
            {
                if let Some(subscription) = self.subscription_mut(&token) {
                    subscription.close_exchange = Some(received.exchange);
                }
                instance
                    .ask(CloseHarnessTranscriptStream { token })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                Ok(())
            }
            HarnessRequest::UnwatchHarnessTranscript(token) => {
                WorkingHarnessEvent::new(received.exchange, Self::unknown_unwatch_event(token))
                    .write(writer)
                    .await
            }
            HarnessRequest::WatchHarnessTranscript(watch)
                if watch.harness != self.bound_harness =>
            {
                WorkingHarnessEvent::new(received.exchange, Self::cross_harness_watch_event(watch))
                    .write(writer)
                    .await
            }
            HarnessRequest::WatchHarnessTranscript(watch) => {
                self.open_subscription(received.exchange, watch, instance)
                    .await
            }
            request => {
                let event = instance
                    .ask(HandleHarnessRequest { request })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                WorkingHarnessEvent::new(received.exchange, event)
                    .write(writer)
                    .await
            }
        }
    }

    async fn write_delivery_event<Writer>(
        &mut self,
        writer: &mut Writer,
        delivery: TranscriptWireDelivery,
    ) -> Result<()>
    where
        Writer: AsyncWrite + Unpin,
    {
        match delivery.event {
            TranscriptDeliveryEvent::Snapshot(snapshot) => {
                let exchange = self
                    .subscription(&delivery.token)
                    .map(|subscription| subscription.watch_exchange)
                    .ok_or_else(|| Error::UnexpectedSignalFrame {
                        got: "transcript snapshot did not match an open wire subscription"
                            .to_string(),
                    })?;
                WorkingHarnessEvent::new(exchange, snapshot.into())
                    .write(writer)
                    .await
            }
            TranscriptDeliveryEvent::Delta(event) => {
                self.write_stream_event(writer, &delivery.token, event)
                    .await
            }
            TranscriptDeliveryEvent::FinalAcknowledgement(acknowledgement) => {
                let exchange = self
                    .subscription(&delivery.token)
                    .map(|subscription| {
                        subscription
                            .close_exchange
                            .unwrap_or(subscription.watch_exchange)
                    })
                    .ok_or_else(|| Error::UnexpectedSignalFrame {
                        got: "transcript final ack did not match an open wire subscription"
                            .to_string(),
                    })?;
                WorkingHarnessEvent::new(exchange, acknowledgement.into())
                    .write(writer)
                    .await
            }
        }
    }

    async fn write_stream_event<Writer>(
        &mut self,
        writer: &mut Writer,
        token: &signal_harness::HarnessTranscriptToken,
        event: HarnessStreamEvent,
    ) -> Result<()>
    where
        Writer: AsyncWrite + Unpin,
    {
        let frame = HarnessFrame::new(FrameBody::SubscriptionEvent {
            event_identifier: self.next_stream_event_identifier(),
            token: Self::subscription_token_inner(token),
            event,
        });
        LengthPrefixedCodec::default()
            .write_body_async(writer, &LengthPrefixedFrameBody::new(frame.encode()?))
            .await?;
        writer.flush().await.map_err(FrameError::from)?;
        Ok(())
    }

    fn next_stream_event_identifier(&mut self) -> StreamEventIdentifier {
        let session_epoch = self
            .subscriptions
            .first()
            .map(|subscription| subscription.watch_exchange.session_epoch)
            .unwrap_or_else(|| SessionEpoch::new(0));
        let identifier = StreamEventIdentifier::new(
            session_epoch,
            ExchangeLane::Acceptor,
            self.next_event_sequence,
        );
        self.next_event_sequence = self.next_event_sequence.next();
        identifier
    }

    async fn close_after_stream_error(&self, instance: &ActorRef<HarnessInstance>) {
        for token in self.tokens() {
            let _ = instance.ask(CloseHarnessTranscriptStream { token }).await;
        }
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
    subscription_manager: ActorRef<TranscriptSubscriptionManager>,
}

impl HarnessRequestHandler {
    pub fn new(
        harness: ActorRef<Harness>,
        subscription_manager: ActorRef<TranscriptSubscriptionManager>,
    ) -> Self {
        Self {
            harness,
            subscription_manager,
        }
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
            HarnessRequest::WatchHarnessTranscript(watch) => {
                self.watch_transcript_event(watch).await
            }
            HarnessRequest::UnwatchHarnessTranscript(token) => {
                self.unwatch_transcript_event(token).await
            }
            other => Ok(HarnessRequestUnimplemented {
                harness: Self::request_harness(&other),
                operation: other.operation_kind(),
                reason: HarnessUnimplementedReason::NotBuiltYet,
            }
            .into()),
        }
    }

    async fn watch_transcript_event(
        &self,
        watch: signal_harness::WatchHarnessTranscript,
    ) -> Result<HarnessEvent> {
        let opened = self
            .subscription_manager
            .ask(OpenTranscriptSubscription {
                harness: watch.harness,
                sink: TranscriptSubscriptionSink::new(),
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(opened.snapshot.into())
    }

    async fn unwatch_transcript_event(
        &self,
        token: signal_harness::HarnessTranscriptToken,
    ) -> Result<HarnessEvent> {
        let closed = self
            .subscription_manager
            .ask(CloseTranscriptSubscription {
                token: token.clone(),
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        if closed.closed {
            Ok(signal_harness::HarnessSubscriptionRetracted { token }.into())
        } else {
            Ok(HarnessRequestUnimplemented {
                harness: token.harness,
                operation: HarnessOperationKind::UnwatchHarnessTranscript,
                reason: HarnessUnimplementedReason::NotBuiltYet,
            }
            .into())
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
