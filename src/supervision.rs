use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_frame::ExchangeIdentifier;
use signal_persona::{
    ComponentHealth, ComponentHealthReport, ComponentIdentity, ComponentKind, ComponentName,
    ComponentReady, EngineManagementProtocolVersion, Frame as SupervisionFrame, FrameBody,
    Operation as SupervisionRequest, Query as SupervisionQuery, Reply as SupervisionReply,
    StopAcknowledgement,
};

use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupervisionProfile {
    name: ComponentName,
    kind: ComponentKind,
    health: ComponentHealth,
}

impl SupervisionProfile {
    pub fn harness() -> Self {
        Self {
            name: ComponentName::new("harness"),
            kind: ComponentKind::Harness,
            health: ComponentHealth::Running,
        }
    }
}

/// The engine-management lifecycle actor — announce, readiness, health, and
/// graceful stop. The schema-emitted daemon shell accepts the owner-only
/// supervision (meta) connection and the component drives this actor; the
/// mailbox serialises every supervision exchange.
#[derive(Debug)]
pub struct SupervisionPhase {
    profile: SupervisionProfile,
    request_count: u64,
}

impl SupervisionPhase {
    fn new(profile: SupervisionProfile) -> Self {
        Self {
            profile,
            request_count: 0,
        }
    }

    pub async fn start(profile: SupervisionProfile) -> ActorRef<Self> {
        let reference = Self::spawn(Self::new(profile));
        reference.wait_for_startup().await;
        reference
    }

    pub async fn stop(reference: ActorRef<Self>) -> Result<()> {
        reference
            .stop_gracefully()
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        reference.wait_for_shutdown().await;
        Ok(())
    }

    fn reply(&mut self, request: SupervisionRequest) -> SupervisionReply {
        self.request_count = self.request_count.saturating_add(1);
        match request {
            SupervisionRequest::Announce(_) => SupervisionReply::Identified(
                ComponentIdentity::new(
                    self.profile.name.clone(),
                    self.profile.kind,
                    EngineManagementProtocolVersion::new(1),
                    None,
                )
                .into(),
            ),
            SupervisionRequest::Query(query) => match query.into_payload() {
                SupervisionQuery::ReadinessStatus(_) => {
                    SupervisionReply::Ready(ComponentReady::from_started_at(None).into())
                }
                SupervisionQuery::HealthStatus(_) => SupervisionReply::HealthReport(
                    ComponentHealthReport::new(self.profile.health).into(),
                ),
            },
            SupervisionRequest::Stop(_) => SupervisionReply::StopAcknowledged(
                StopAcknowledgement::from_drain_completed_at(None).into(),
            ),
        }
    }
}

#[derive(Debug, kameo::Reply)]
pub struct SupervisionPhaseReply {
    pub reply: SupervisionReply,
}

impl Actor for SupervisionPhase {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        phase: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(phase)
    }
}

#[derive(Debug)]
pub struct HandleSupervisionRequest {
    pub request: SupervisionRequest,
}

impl Message<HandleSupervisionRequest> for SupervisionPhase {
    type Reply = SupervisionPhaseReply;

    async fn handle(
        &mut self,
        message: HandleSupervisionRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SupervisionPhaseReply {
            reply: self.reply(message.request),
        }
    }
}

/// One decoded engine-management request plus its exchange identifier, as the
/// daemon shell's owner-only meta (supervision) connection hook delivers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedSupervisionRequest {
    pub exchange: ExchangeIdentifier,
    pub request: SupervisionRequest,
}

impl ReceivedSupervisionRequest {
    /// Decode one engine-management request off the bare (non-length-prefixed)
    /// supervision `Frame` body the daemon shell delivers after stripping its
    /// outer length-prefixed envelope.
    pub fn decode(body: &[u8]) -> Result<Self> {
        match SupervisionFrame::decode(body)?.into_body() {
            FrameBody::Request { exchange, request } => {
                let (request, tail) = request.payloads.into_head_and_tail();
                if !tail.is_empty() {
                    return Err(Error::UnexpectedSignalFrame {
                        got: format!("expected one supervision operation, got {}", tail.len() + 1),
                    });
                }
                Ok(Self { exchange, request })
            }
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }
}
