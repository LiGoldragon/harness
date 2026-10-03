//! Harness's daemon hooks — the only daemon code harness hand-writes.
//!
//! The uniform daemon skeleton (argv parsing, async task-backed multi-listener
//! binding, request gating, peer credentials, lifecycle, and the `ExitReport`
//! entry) is emitted into `src/schema/daemon.rs` by schema-rust's daemon
//! emitter. The ordinary socket speaks `signal-harness`: one `Query` frame in,
//! `Response` frames out, decoded here. The owner-only meta socket speaks
//! `meta-signal-harness`. The supervision socket, bound by the engine itself,
//! speaks the `signal-persona` engine-management lifecycle. Each socket
//! carries exactly one contract, because a Signal frame names none.
//!
//! `UsageSnapshotQuery` is answered at daemon scope, before any configured
//! instance is looked up: it reads the provider sources and live sessions and
//! needs no harness instance.

use std::collections::HashMap;
use std::path::PathBuf;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use meta_signal_harness::{
    MetaOperationKind, Query as MetaHarnessRequest, RequestUnimplemented,
    Response as MetaHarnessReply, UnimplementedReason,
};
use signal_harness::{
    CapabilityProfile, ClaudeSessionObservation, ContinuationHandle, ContinuationRequest,
    DeliveryCompleted, DeliveryFailed, DeliveryFailureReason, EffortRequest,
    HarnessDaemonConfiguration, HarnessHealth, HarnessInstanceConfiguration, HarnessName,
    HarnessOperationKind, HarnessReadiness, HarnessRequestUnimplemented, HarnessStatus,
    HarnessStatusQuery, HarnessStreamEvent, HarnessUnimplementedReason, MessageDelivery,
    ModelResolutionRequest, ModelResolved, ModelSelector, ModelUnavailable, ModelUnavailableReason,
    NamedModel, Query as HarnessRequest, Response as HarnessEvent, TranscriptObservation,
};
use tokio::io::AsyncWrite;
use tokio::sync::OnceCell;
use tokio::sync::mpsc;
use triad_runtime::AcceptedConnection;

use crate::launch::SessionLauncher;
use crate::schema::daemon::ComponentDaemon;
use crate::supervision::{SupervisionProfile, SupervisionSocket};
use crate::usage::{UsageHome, UsageSnapshotReader, UsageSnapshotReading};
use crate::wire::SignalWire;
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

/// Harness's daemon-facing engine: the configured harness instances, the
/// session launcher, and the usage reader. The runtime shares this engine as
/// `&Self::Engine`; each instance owns its mutable state behind a kameo
/// mailbox, so no component-internal lock is required. The instance actors
/// start on first connection so `build_runtime` stays synchronous and they
/// spawn inside the daemon's tokio runtime.
pub struct HarnessEngine {
    instance_configurations: Vec<HarnessRuntimeConfiguration>,
    session_launcher: SessionLauncher,
    usage: Option<UsageSnapshotReader>,
    instances: OnceCell<BoundHarnessInstances>,
    wire: SignalWire,
}

impl HarnessEngine {
    /// Canonical constructor — every production launch reads a typed
    /// `HarnessDaemonConfiguration` from the daemon's binary rkyv startup file
    /// and hands the decoded record here. The usage reader reads the daemon
    /// user's own home.
    pub fn from_configuration(configuration: HarnessDaemonConfiguration) -> Self {
        Self::new(
            configuration,
            UsageHome::from_process().map(UsageSnapshotReader::for_home),
        )
    }

    /// An engine whose usage snapshot reads the given reader, or answers every
    /// provider unavailable when there is none.
    pub fn new(
        configuration: HarnessDaemonConfiguration,
        usage: Option<UsageSnapshotReader>,
    ) -> Self {
        Self {
            instance_configurations: configuration
                .harness_instance_configurations
                .into_iter()
                .map(HarnessRuntimeConfiguration::from_contract)
                .collect(),
            session_launcher: SessionLauncher::from_environment(),
            usage,
            instances: OnceCell::new(),
            wire: SignalWire::default(),
        }
    }

    async fn instances(&self) -> Result<&BoundHarnessInstances> {
        self.instances
            .get_or_try_init(|| BoundHarnessInstances::start(self.instance_configurations.clone()))
            .await
    }

    async fn handle_working_connection(&self, connection: &mut AcceptedConnection) -> Result<()> {
        self.handle_working_stream(connection.stream_mut()).await
    }

    /// Serve one ordinary working stream. A request completes with one
    /// `Response` frame. `WatchHarnessTranscript` keeps the stream attached to
    /// the subscription sink, so the snapshot, every stream event and the
    /// final retraction ride the original connection as `Response` frames.
    pub async fn handle_working_stream(&self, stream: &mut tokio::net::UnixStream) -> Result<()> {
        let request: HarnessRequest = self.wire.read_async(stream).await?;
        match request {
            HarnessRequest::UsageSnapshotQuery => {
                let snapshot = self.usage_snapshot().await;
                self.wire
                    .write_async(stream, &HarnessEvent::UsageSnapshot(snapshot))
                    .await
            }
            HarnessRequest::WatchHarnessTranscript(watch) => {
                self.handle_transcript_stream(watch, stream).await
            }
            request => {
                let event = self.event_for_request(request).await?;
                self.wire.write_async(stream, &event).await
            }
        }
    }

    /// One fresh usage snapshot, read off the async runtime because the
    /// provider reads block.
    async fn usage_snapshot(&self) -> signal_harness::UsageSnapshot {
        let reader = self.usage.clone();
        let read = tokio::task::spawn_blocking(move || match reader {
            Some(reader) => reader.read_snapshot(),
            None => UsageSnapshotReader::failed_snapshot(),
        })
        .await;
        read.unwrap_or_else(|_| UsageSnapshotReader::failed_snapshot())
    }

    async fn handle_transcript_stream(
        &self,
        watch: signal_harness::WatchHarnessTranscript,
        stream: &mut tokio::net::UnixStream,
    ) -> Result<()> {
        let harness = watch.harness_name.clone();
        let Some(instance) = self.instances().await?.instance(&harness).cloned() else {
            let event = Self::unavailable_event(HarnessRequest::WatchHarnessTranscript(watch));
            return self.wire.write_async(stream, &event).await;
        };
        let mut transcript_stream = HarnessTranscriptWireStream::new(harness);
        transcript_stream
            .open_subscription(watch, &instance)
            .await?;
        transcript_stream.serve(stream, instance).await
    }

    /// Push one transcript line onto the addressed harness's stream.
    pub async fn publish_transcript_observation(
        &self,
        observation: TranscriptObservation,
    ) -> Result<TranscriptPublicationReceipt> {
        let harness = observation.harness_name.clone();
        self.publish_stream_event(
            &harness,
            HarnessStreamEvent::TranscriptObservation(observation),
        )
        .await
    }

    /// Push one per-turn Claude session observation onto the addressed
    /// harness's stream. It rides the same transcript stream as transcript
    /// lines — the Mentci live view renders it, and orchestrate's session
    /// store later consumes the same pushed event.
    pub async fn publish_claude_session_observation(
        &self,
        observation: ClaudeSessionObservation,
    ) -> Result<TranscriptPublicationReceipt> {
        let harness = observation.harness_name.clone();
        self.publish_stream_event(
            &harness,
            HarnessStreamEvent::ClaudeSessionObservation(observation),
        )
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
        let Some(harness) = request.addressed_harness() else {
            return Ok(Self::unavailable_event(request));
        };
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
            HarnessRequest::MessageDelivery(delivery) => {
                HarnessEvent::DeliveryFailed(DeliveryFailed {
                    harness_name: delivery.harness_name,
                    message_slot: delivery.message_slot,
                    delivery_failure_reason: DeliveryFailureReason::HarnessUnavailable,
                })
            }
            HarnessRequest::HarnessStatusQuery(query) => {
                HarnessEvent::HarnessStatus(HarnessStatus {
                    harness_name: query.harness_name,
                    harness_health: HarnessHealth::Stopped,
                    harness_readiness: HarnessReadiness::Unavailable,
                })
            }
            other => {
                let harness = other.addressed_harness().unwrap_or_default();
                HarnessRequestHandler::unimplemented(&other, harness)
            }
        }
    }

    /// Serve one owner-only meta connection: one `meta-signal-harness` `Query`
    /// frame in, one `Response` frame out.
    async fn handle_meta_connection(&self, connection: &mut AcceptedConnection) -> Result<()> {
        let request: MetaHarnessRequest = self.wire.read_async(connection.stream_mut()).await?;
        let reply = self.reply_for_meta_request(request);
        self.wire.write_async(connection.stream_mut(), &reply).await
    }

    fn reply_for_meta_request(&self, request: MetaHarnessRequest) -> MetaHarnessReply {
        match request {
            MetaHarnessRequest::Configure(_) => {
                MetaHarnessReply::RequestUnimplemented(RequestUnimplemented {
                    meta_operation_kind: MetaOperationKind::ConfigureDaemon,
                    unimplemented_reason: UnimplementedReason::NotBuiltYet,
                })
            }
            MetaHarnessRequest::ResolveModel(request) => {
                ModelResolutionCatalog::new(&self.instance_configurations)
                    .reply_for_request(request)
            }
            MetaHarnessRequest::LaunchSession(request) => self.session_launcher.launch(request),
        }
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

    /// Build the engine and bind the supervision socket beside the ordinary
    /// and meta listeners the shell binds. The shell calls this inside its
    /// tokio runtime, so the supervision listener runs as a task of it.
    fn build_runtime(
        configuration: &Self::Configuration,
    ) -> std::result::Result<Self::Engine, Self::Error> {
        let supervision = SupervisionSocket::new(
            configuration.supervision_socket_path(),
            configuration.supervision_socket_mode(),
        )
        .bind(SupervisionProfile::harness())?;
        let _supervision = tokio::spawn(supervision.serve());
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

/// The harness instance a request addresses; a daemon-scope request
/// addresses none.
pub trait AddressedHarness {
    fn addressed_harness(&self) -> Option<HarnessName>;
    fn operation_kind(&self) -> HarnessOperationKind;
}

impl AddressedHarness for HarnessRequest {
    fn addressed_harness(&self) -> Option<HarnessName> {
        match self {
            HarnessRequest::MessageDelivery(payload) => Some(payload.harness_name.clone()),
            HarnessRequest::InteractionPrompt(payload) => Some(payload.harness_name.clone()),
            HarnessRequest::DeliveryCancellation(payload) => Some(payload.harness_name.clone()),
            HarnessRequest::HarnessStatusQuery(payload) => Some(payload.harness_name.clone()),
            HarnessRequest::WatchHarnessTranscript(payload) => Some(payload.harness_name.clone()),
            HarnessRequest::UnwatchHarnessTranscript(token) => Some(token.harness_name.clone()),
            HarnessRequest::UsageSnapshotQuery => None,
        }
    }

    fn operation_kind(&self) -> HarnessOperationKind {
        match self {
            HarnessRequest::MessageDelivery(_) => HarnessOperationKind::DeliverMessage,
            HarnessRequest::InteractionPrompt(_) => HarnessOperationKind::PromptInteraction,
            HarnessRequest::DeliveryCancellation(_) => HarnessOperationKind::CancelDelivery,
            HarnessRequest::HarnessStatusQuery(_) => HarnessOperationKind::QueryHarnessStatus,
            HarnessRequest::WatchHarnessTranscript(_) => HarnessOperationKind::WatchTranscript,
            HarnessRequest::UnwatchHarnessTranscript(_) => HarnessOperationKind::UnwatchTranscript,
            HarnessRequest::UsageSnapshotQuery => HarnessOperationKind::ReadUsageSnapshot,
        }
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
            .terminal_socket_path_option
            .map(|path| HarnessTerminalEndpoint::pty_socket(path.as_str()));
        let pi_rpc_configuration =
            configuration
                .pi_rpc_jsonl_adapter_configuration_option
                .map(|adapter| {
                    let process_configuration = PiRpcProcessConfiguration::new(
                        adapter.pi_rpc_command_path.as_str(),
                        adapter.pi_rpc_session_directory_path.as_str(),
                    )
                    .with_session_name(harness_name.as_str())
                    .with_delivery_command(adapter.pi_rpc_delivery_mode.into());
                    match adapter.pi_rpc_model_pattern_option {
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

    fn contract_kind(&self) -> signal_harness::HarnessKind {
        match &self.kind {
            HarnessKind::Codex => signal_harness::HarnessKind::Codex,
            HarnessKind::Claude => signal_harness::HarnessKind::Claude,
            HarnessKind::Pi => signal_harness::HarnessKind::Pi,
            HarnessKind::Fixture => signal_harness::HarnessKind::Fixture,
        }
    }

    fn resolver_adapter(&self) -> ConfiguredResolverAdapter<'_> {
        ConfiguredResolverAdapter::new(self)
    }
}

struct ModelResolutionCatalog<'a> {
    configurations: &'a [HarnessRuntimeConfiguration],
}

impl<'a> ModelResolutionCatalog<'a> {
    fn new(configurations: &'a [HarnessRuntimeConfiguration]) -> Self {
        Self { configurations }
    }

    fn reply_for_request(&self, request: ModelResolutionRequest) -> MetaHarnessReply {
        match self.resolved_model(&request) {
            Ok(resolved) => MetaHarnessReply::ModelResolved(resolved),
            Err(reason) => MetaHarnessReply::ModelUnavailable(ModelUnavailable {
                model_resolution_request: request,
                model_unavailable_reason: reason,
            }),
        }
    }

    fn resolved_model(
        &self,
        request: &ModelResolutionRequest,
    ) -> std::result::Result<ModelResolved, ModelUnavailableReason> {
        if self.configurations.is_empty() {
            return Err(ModelUnavailableReason::NoConfiguredHarness);
        }
        let mut failures = ExhaustedModelResolution::new(&request.model_request.model_selector);
        for selection in self
            .configurations
            .iter()
            .map(HarnessRuntimeConfiguration::resolver_adapter)
            .filter_map(|adapter| adapter.selection_for_model_request(&request.model_request))
        {
            match selection.resolved_model(request) {
                Ok(resolved) => return Ok(resolved),
                Err(reason) => failures.record(reason),
            }
        }
        Err(failures.into_reason())
    }
}

struct ExhaustedModelResolution<'a> {
    selector: &'a ModelSelector,
    strongest_reason: Option<ModelUnavailableReason>,
}

impl<'a> ExhaustedModelResolution<'a> {
    fn new(selector: &'a ModelSelector) -> Self {
        Self {
            selector,
            strongest_reason: None,
        }
    }

    fn record(&mut self, reason: ModelUnavailableReason) {
        let new_priority = self.priority(&reason);
        let current_priority = self
            .strongest_reason
            .as_ref()
            .map(|current| self.priority(current))
            .unwrap_or(0);
        if self.strongest_reason.is_none() || new_priority > current_priority {
            self.strongest_reason = Some(reason);
        }
    }

    fn into_reason(self) -> ModelUnavailableReason {
        self.strongest_reason
            .unwrap_or_else(|| self.selector.unmatched_reason())
    }

    fn priority(&self, reason: &ModelUnavailableReason) -> u8 {
        match reason {
            ModelUnavailableReason::EffortUnsupported => 1,
            ModelUnavailableReason::AdapterConfigurationMissing => 2,
            ModelUnavailableReason::ContinuationUnavailable => 3,
            ModelUnavailableReason::NoConfiguredHarness
            | ModelUnavailableReason::ModelNotKnown
            | ModelUnavailableReason::CapabilityUnsupported
            | ModelUnavailableReason::ProviderUnavailable => 0,
        }
    }
}

trait ModelSelectorUnmatchedReason {
    fn unmatched_reason(&self) -> ModelUnavailableReason;
}

impl ModelSelectorUnmatchedReason for ModelSelector {
    fn unmatched_reason(&self) -> ModelUnavailableReason {
        match self {
            ModelSelector::Exact(_) => ModelUnavailableReason::ModelNotKnown,
            ModelSelector::CapabilityProfile(_) => ModelUnavailableReason::CapabilityUnsupported,
        }
    }
}

struct ModelResolutionSelection<'a> {
    adapter: ConfiguredResolverAdapter<'a>,
    model: NamedModel,
}

impl<'a> ModelResolutionSelection<'a> {
    fn resolved_model(
        self,
        request: &ModelResolutionRequest,
    ) -> std::result::Result<ModelResolved, ModelUnavailableReason> {
        if !self
            .adapter
            .supports_effort(&request.model_request.effort_request)
        {
            return Err(ModelUnavailableReason::EffortUnsupported);
        }
        if !self.adapter.has_required_runtime_adapter() {
            return Err(ModelUnavailableReason::AdapterConfigurationMissing);
        }
        let Some(continuation) = self
            .adapter
            .continuation_for_request(&request.continuation_request)
        else {
            return Err(ModelUnavailableReason::ContinuationUnavailable);
        };
        Ok(ModelResolved {
            harness_name: self.adapter.harness().clone(),
            harness_kind: self.adapter.contract_kind(),
            named_model: self.model,
            effort_request: request.model_request.effort_request.clone(),
            continuation_handle: continuation,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct ConfiguredResolverAdapter<'a> {
    configuration: &'a HarnessRuntimeConfiguration,
}

impl<'a> ConfiguredResolverAdapter<'a> {
    fn new(configuration: &'a HarnessRuntimeConfiguration) -> Self {
        Self { configuration }
    }

    fn harness(&self) -> &HarnessName {
        &self.configuration.harness
    }

    fn contract_kind(&self) -> signal_harness::HarnessKind {
        self.configuration.contract_kind()
    }

    fn selection_for_model_request(
        self,
        request: &signal_harness::ModelRequest,
    ) -> Option<ModelResolutionSelection<'a>> {
        let model = match &request.model_selector {
            ModelSelector::Exact(model) => self.exact_model(model)?,
            ModelSelector::CapabilityProfile(profile) => self.model_for_profile(profile)?,
        };
        Some(ModelResolutionSelection {
            adapter: self,
            model,
        })
    }

    fn exact_model(&self, requested: &NamedModel) -> Option<NamedModel> {
        match &self.configuration.kind {
            HarnessKind::Codex => ProviderModelNamespace::codex().exact_model(requested),
            HarnessKind::Claude => ProviderModelNamespace::claude().exact_model(requested),
            HarnessKind::Pi => self
                .configuration
                .pi_rpc_configuration
                .as_ref()?
                .model_pattern()
                .filter(|model| *model == requested.as_str())
                .map(NamedModel::from),
            HarnessKind::Fixture => None,
        }
    }

    fn model_for_profile(&self, requested: &CapabilityProfile) -> Option<NamedModel> {
        match &self.configuration.kind {
            HarnessKind::Codex => ProviderModelNamespace::codex().model_for_profile(requested),
            HarnessKind::Claude => ProviderModelNamespace::claude().model_for_profile(requested),
            HarnessKind::Pi => {
                if !ProviderModelNamespace::pi().profile_matches(requested) {
                    return None;
                }
                Some(
                    self.configuration
                        .pi_rpc_configuration
                        .as_ref()
                        .and_then(PiRpcProcessConfiguration::model_pattern)
                        .map(NamedModel::from)
                        .unwrap_or_else(|| requested.clone()),
                )
            }
            HarnessKind::Fixture => None,
        }
    }

    fn supports_effort(&self, effort: &EffortRequest) -> bool {
        match &self.configuration.kind {
            HarnessKind::Codex | HarnessKind::Claude => true,
            HarnessKind::Pi => matches!(
                effort,
                EffortRequest::Minimal | EffortRequest::Low | EffortRequest::Medium
            ),
            HarnessKind::Fixture => false,
        }
    }

    fn has_required_runtime_adapter(&self) -> bool {
        match &self.configuration.kind {
            HarnessKind::Codex | HarnessKind::Claude => {
                self.configuration.terminal_endpoint.is_some()
            }
            HarnessKind::Pi => self
                .configuration
                .pi_rpc_configuration
                .as_ref()
                .and_then(PiRpcProcessConfiguration::model_pattern)
                .is_some(),
            HarnessKind::Fixture => false,
        }
    }

    fn continuation_for_request(
        &self,
        request: &ContinuationRequest,
    ) -> Option<ContinuationHandle> {
        match request {
            ContinuationRequest::Fresh => self.fresh_continuation(),
            ContinuationRequest::Prefer(handle) | ContinuationRequest::Require(handle) => {
                self.validated_continuation(handle)
            }
        }
    }

    fn fresh_continuation(&self) -> Option<ContinuationHandle> {
        match &self.configuration.kind {
            HarnessKind::Codex => Some(ContinuationHandle::Codex(self.harness().clone())),
            HarnessKind::Claude => Some(ContinuationHandle::Claude(self.harness().clone())),
            HarnessKind::Pi => {
                self.configuration
                    .pi_rpc_configuration
                    .as_ref()
                    .map(|configuration| {
                        ContinuationHandle::Pi(configuration.session_name().to_owned())
                    })
            }
            HarnessKind::Fixture => None,
        }
    }

    fn validated_continuation(&self, handle: &ContinuationHandle) -> Option<ContinuationHandle> {
        match (&self.configuration.kind, handle) {
            (HarnessKind::Codex, ContinuationHandle::Codex(identifier)) => {
                (!identifier.as_str().is_empty()).then(|| handle.clone())
            }
            (HarnessKind::Claude, ContinuationHandle::Claude(identifier)) => {
                (!identifier.as_str().is_empty()).then(|| handle.clone())
            }
            (HarnessKind::Pi, ContinuationHandle::Pi(identifier)) => self
                .configuration
                .pi_rpc_configuration
                .as_ref()
                .filter(|configuration| configuration.session_name() == identifier.as_str())
                .map(|_| handle.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ProviderModelNamespace {
    exact_models: &'static [&'static str],
    capability_profiles: &'static [&'static str],
    default_model: &'static str,
}

impl ProviderModelNamespace {
    fn codex() -> Self {
        Self {
            exact_models: &["gpt-5-codex", "codex"],
            capability_profiles: &["codex", "coding"],
            default_model: "gpt-5-codex",
        }
    }

    fn claude() -> Self {
        Self {
            exact_models: &[
                "claude-sonnet-4-20250514",
                "claude-opus-4-20250514",
                "claude-3-5-haiku-latest",
                "sonnet",
                "opus",
                "haiku",
            ],
            capability_profiles: &["claude", "anthropic"],
            default_model: "claude-sonnet-4-20250514",
        }
    }

    fn pi() -> Self {
        Self {
            exact_models: &[],
            capability_profiles: &["pi", "local"],
            default_model: "",
        }
    }

    fn exact_model(&self, requested: &NamedModel) -> Option<NamedModel> {
        self.exact_models
            .contains(&requested.as_str())
            .then(|| requested.clone())
    }

    fn model_for_profile(&self, requested: &CapabilityProfile) -> Option<NamedModel> {
        self.profile_matches(requested)
            .then(|| NamedModel::from(self.default_model))
    }

    fn profile_matches(&self, requested: &CapabilityProfile) -> bool {
        self.capability_profiles.contains(&requested.as_str())
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
                harness: message.watch.harness_name,
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

struct HarnessTranscriptWireStream {
    bound_harness: HarnessName,
    sender: mpsc::UnboundedSender<TranscriptWireDelivery>,
    receiver: mpsc::UnboundedReceiver<TranscriptWireDelivery>,
    subscriptions: Vec<signal_harness::HarnessTranscriptToken>,
    wire: SignalWire,
}

struct TranscriptWireDelivery {
    token: signal_harness::HarnessTranscriptToken,
    event: TranscriptDeliveryEvent,
}

impl TranscriptWireDelivery {
    fn final_ack(&self) -> bool {
        matches!(self.event, TranscriptDeliveryEvent::FinalAcknowledgement(_))
    }

    /// The `Response` frame this delivery rides the wire as; a stream event
    /// carries the token of the subscription it belongs to.
    fn into_response(self) -> HarnessEvent {
        match self.event {
            TranscriptDeliveryEvent::Snapshot(snapshot) => {
                HarnessEvent::HarnessTranscriptSnapshot(snapshot)
            }
            TranscriptDeliveryEvent::Delta(event) => {
                HarnessEvent::HarnessTranscriptEvent(signal_harness::HarnessTranscriptEvent {
                    harness_transcript_token: self.token,
                    harness_stream_event: event,
                })
            }
            TranscriptDeliveryEvent::FinalAcknowledgement(acknowledgement) => {
                HarnessEvent::HarnessSubscriptionRetracted(acknowledgement)
            }
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
            wire: SignalWire::default(),
        }
    }

    async fn open_subscription(
        &mut self,
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
        self.subscriptions.push(token.clone());
        TranscriptDeliveryForwarder::new(self.sender.clone()).spawn(token, receiver);
        Ok(())
    }

    fn holds(&self, token: &signal_harness::HarnessTranscriptToken) -> bool {
        self.subscriptions.contains(token)
    }

    fn remove_subscription(&mut self, token: &signal_harness::HarnessTranscriptToken) {
        self.subscriptions.retain(|held| held != token);
    }

    fn unknown_unwatch_event(token: signal_harness::HarnessTranscriptToken) -> HarnessEvent {
        HarnessEvent::HarnessRequestUnimplemented(HarnessRequestUnimplemented {
            harness_name: token.harness_name,
            harness_operation_kind: HarnessOperationKind::UnwatchTranscript,
            harness_unimplemented_reason: HarnessUnimplementedReason::NotBuiltYet,
        })
    }

    fn cross_harness_watch_event(watch: signal_harness::WatchHarnessTranscript) -> HarnessEvent {
        HarnessEvent::HarnessRequestUnimplemented(HarnessRequestUnimplemented {
            harness_name: watch.harness_name,
            harness_operation_kind: HarnessOperationKind::WatchTranscript,
            harness_unimplemented_reason: HarnessUnimplementedReason::NotBuiltYet,
        })
    }

    async fn serve(
        mut self,
        stream: &mut tokio::net::UnixStream,
        instance: ActorRef<HarnessInstance>,
    ) -> Result<()> {
        let (mut reader, mut writer) = tokio::io::split(stream);
        loop {
            tokio::select! {
                event = self.receiver.recv() => {
                    let Some(delivery) = event else {
                        return Ok(());
                    };
                    let final_ack = delivery.final_ack();
                    let token = delivery.token.clone();
                    if let Err(error) = self.wire.write_async(&mut writer, &delivery.into_response()).await {
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
                request = self.wire.read_async::<HarnessRequest>(&mut reader) => {
                    let request = match request {
                        Ok(request) => request,
                        Err(error) => {
                            self.close_after_stream_error(&instance).await;
                            return Err(error);
                        }
                    };
                    if let Err(error) = self.handle_request(request, &instance, &mut writer).await {
                        self.close_after_stream_error(&instance).await;
                        return Err(error);
                    }
                }
            }
        }
    }

    async fn handle_request<Writer>(
        &mut self,
        request: HarnessRequest,
        instance: &ActorRef<HarnessInstance>,
        writer: &mut Writer,
    ) -> Result<()>
    where
        Writer: AsyncWrite + Unpin + Send,
    {
        match request {
            HarnessRequest::UnwatchHarnessTranscript(token) if self.holds(&token) => {
                instance
                    .ask(CloseHarnessTranscriptStream { token })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                Ok(())
            }
            HarnessRequest::UnwatchHarnessTranscript(token) => {
                self.wire
                    .write_async(writer, &Self::unknown_unwatch_event(token))
                    .await
            }
            HarnessRequest::WatchHarnessTranscript(watch)
                if watch.harness_name != self.bound_harness =>
            {
                self.wire
                    .write_async(writer, &Self::cross_harness_watch_event(watch))
                    .await
            }
            HarnessRequest::WatchHarnessTranscript(watch) => {
                self.open_subscription(watch, instance).await
            }
            HarnessRequest::UsageSnapshotQuery => {
                let event = HarnessRequestHandler::unimplemented(
                    &HarnessRequest::UsageSnapshotQuery,
                    self.bound_harness.clone(),
                );
                self.wire.write_async(writer, &event).await
            }
            request => {
                let event = instance
                    .ask(HandleHarnessRequest { request })
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                self.wire.write_async(writer, &event).await
            }
        }
    }

    async fn close_after_stream_error(&self, instance: &ActorRef<HarnessInstance>) {
        for token in self.subscriptions.clone() {
            let _ = instance.ask(CloseHarnessTranscriptStream { token }).await;
        }
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
            other => {
                let harness = other.addressed_harness().unwrap_or_default();
                Ok(Self::unimplemented(&other, harness))
            }
        }
    }

    /// The typed reply for a request whose runtime path is not built.
    pub fn unimplemented(request: &HarnessRequest, harness: HarnessName) -> HarnessEvent {
        HarnessEvent::HarnessRequestUnimplemented(HarnessRequestUnimplemented {
            harness_name: harness,
            harness_operation_kind: request.operation_kind(),
            harness_unimplemented_reason: HarnessUnimplementedReason::NotBuiltYet,
        })
    }

    async fn watch_transcript_event(
        &self,
        watch: signal_harness::WatchHarnessTranscript,
    ) -> Result<HarnessEvent> {
        let opened = self
            .subscription_manager
            .ask(OpenTranscriptSubscription {
                harness: watch.harness_name,
                sink: TranscriptSubscriptionSink::new(),
            })
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(HarnessEvent::HarnessTranscriptSnapshot(opened.snapshot))
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
            Ok(HarnessEvent::HarnessSubscriptionRetracted(
                signal_harness::HarnessSubscriptionRetracted {
                    harness_transcript_token: token,
                },
            ))
        } else {
            Ok(HarnessEvent::HarnessRequestUnimplemented(
                HarnessRequestUnimplemented {
                    harness_name: token.harness_name,
                    harness_operation_kind: HarnessOperationKind::UnwatchTranscript,
                    harness_unimplemented_reason: HarnessUnimplementedReason::NotBuiltYet,
                },
            ))
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

        let binding = HarnessTerminalBinding::for_harness(HarnessIdentifier::new(
            delivery.harness_name.as_str(),
        ));
        match delivery_adapter.deliver_text(&binding, delivery.message_body.as_str()) {
            Ok(receipt) if receipt.delivered() => {
                Ok(HarnessEvent::DeliveryCompleted(DeliveryCompleted {
                    harness_name: delivery.harness_name,
                    message_slot: delivery.message_slot,
                }))
            }
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
        Ok(HarnessEvent::HarnessStatus(HarnessStatus {
            harness_name: query.harness_name,
            harness_health: Self::health(&state),
            harness_readiness: Self::readiness(&state),
        }))
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
        HarnessEvent::DeliveryFailed(DeliveryFailed {
            harness_name: delivery.harness_name,
            message_slot: delivery.message_slot,
            delivery_failure_reason: reason,
        })
    }
}
