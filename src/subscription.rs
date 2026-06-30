//! Transcript-observation subscription producer plane.
//!
//! Three named Kameo actors carry the producer side of the
//! `HarnessTranscriptStream` subscription per the canonical
//! five-state lifecycle named in
//! `~/primary/skills/subscription-lifecycle.md`:
//!
//! - `TranscriptSubscriptionManager` — owns the set of open
//!   subscriptions keyed by `HarnessTranscriptToken`.
//! - `TranscriptStreamingReplyHandler` — one per open
//!   subscription; holds the per-stream cursor, the bounded
//!   outbound buffer, the close-ack flag, and the consumer
//!   sink.
//! - `TranscriptDeltaPublisher` — fans `TranscriptObservation`
//!   records out to every registered handler.
//!
//! The publisher fans out by per-handler mailbox sends; one
//! slow handler stalls only its own mailbox. No shared
//! `Arc<Mutex<_>>` carries the subscription set; the manager
//! IS the single owner.

use std::collections::VecDeque;
use std::sync::Arc;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_harness::{
    HarnessName, HarnessSubscriptionRetracted, HarnessTranscriptSequence,
    HarnessTranscriptSnapshot, HarnessTranscriptSubscriptionIdentifier, HarnessTranscriptToken,
    TranscriptObservation,
};
use std::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

/// One delta event emitted on a `HarnessTranscriptStream`.
/// The publisher hands these to every handler; each handler
/// forwards them to its consumer sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverTranscriptDelta {
    pub observation: TranscriptObservation,
}

/// Open a subscription. The handler that responds carries the
/// per-stream token and the snapshot that opens the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenTranscriptSubscription {
    pub harness: HarnessName,
    pub sink: TranscriptSubscriptionSink,
}

/// Close a subscription. The manager hands the token to the
/// matching handler, which drains in-flight deltas and emits
/// the final `HarnessSubscriptionRetracted` ack onto the sink.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseTranscriptSubscription {
    pub token: HarnessTranscriptToken,
}

/// One observation arrived from the `Harness` actor. The
/// publisher fans it out to every open handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishTranscriptObservation {
    pub observation: TranscriptObservation,
}

/// The consumer-facing sink. Each open subscription has its
/// own sink; the handler pushes typed delivery events onto it.
///
/// The prototype uses a `Vec` shared through a `Mutex` for the
/// sink rather than a Tokio channel because tests need
/// deterministic, mailbox-paced reads. Production daemons
/// replace this with a real socket-writer actor.
#[derive(Debug, Clone)]
pub struct TranscriptSubscriptionSink {
    target: TranscriptSubscriptionSinkTarget,
}

#[derive(Debug, Clone)]
enum TranscriptSubscriptionSinkTarget {
    Memory(Arc<Mutex<TranscriptSubscriptionSinkInner>>),
    Channel(UnboundedSender<TranscriptDeliveryEvent>),
}

#[derive(Debug)]
struct TranscriptSubscriptionSinkInner {
    delivered: VecDeque<TranscriptDeliveryEvent>,
    delivered_count: u64,
    closed_with_ack: bool,
    pending_acceptance: usize,
}

/// One event delivered to the consumer-facing sink. The
/// variants mirror the subscription's lifecycle: snapshot
/// (open), delta (event), final ack (close).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscriptDeliveryEvent {
    Snapshot(HarnessTranscriptSnapshot),
    Delta(TranscriptObservation),
    FinalAcknowledgement(HarnessSubscriptionRetracted),
}

impl TranscriptSubscriptionSink {
    /// Build a fresh sink. The consumer drains the sink by
    /// repeatedly polling `next_delivered`; the producer
    /// pushes through `push` (which `StreamingReplyHandler`
    /// owns).
    pub fn new() -> Self {
        Self {
            target: TranscriptSubscriptionSinkTarget::Memory(Self::memory_inner_with_acceptance(
                usize::MAX,
            )),
        }
    }

    /// Build a sink that forwards delivery events to a daemon-owned
    /// writer task. The subscription actors still own event ordering and
    /// final-ack emission; the daemon writes each event onto the accepted
    /// stream.
    pub fn channel(sender: UnboundedSender<TranscriptDeliveryEvent>) -> Self {
        Self {
            target: TranscriptSubscriptionSinkTarget::Channel(sender),
        }
    }

    fn memory_inner_with_acceptance(
        pending_acceptance: usize,
    ) -> Arc<Mutex<TranscriptSubscriptionSinkInner>> {
        Arc::new(Mutex::new(TranscriptSubscriptionSinkInner {
            delivered: VecDeque::new(),
            delivered_count: 0,
            closed_with_ack: false,
            pending_acceptance,
        }))
    }

    fn memory_inner(&self) -> Option<&Arc<Mutex<TranscriptSubscriptionSinkInner>>> {
        match &self.target {
            TranscriptSubscriptionSinkTarget::Memory(inner) => Some(inner),
            TranscriptSubscriptionSinkTarget::Channel(_) => None,
        }
    }

    fn read_memory_inner<Output>(
        &self,
        default: Output,
        operation: impl FnOnce(&TranscriptSubscriptionSinkInner) -> Output,
    ) -> Output {
        let Some(inner) = self.memory_inner() else {
            return default;
        };
        let inner = inner.lock().expect("transcript subscription sink lock");
        operation(&inner)
    }

    fn with_memory_inner<Output>(
        &self,
        default: Output,
        operation: impl FnOnce(&mut TranscriptSubscriptionSinkInner) -> Output,
    ) -> Output {
        let Some(inner) = self.memory_inner() else {
            return default;
        };
        let mut inner = inner.lock().expect("transcript subscription sink lock");
        operation(&mut inner)
    }

    /// Build a sink with a bounded acceptance capacity. The
    /// handler refuses further pushes once the consumer's
    /// pending-acceptance count drops to zero; the consumer
    /// raises it again by acknowledging delivered events.
    pub fn with_acceptance(initial_capacity: usize) -> Self {
        Self {
            target: TranscriptSubscriptionSinkTarget::Memory(Self::memory_inner_with_acceptance(
                initial_capacity,
            )),
        }
    }

    /// Drain the next delivered event. Returns `None` when no
    /// event has arrived yet; tests poll until an event lands.
    pub fn next_delivered(&self) -> Option<TranscriptDeliveryEvent> {
        self.with_memory_inner(None, |inner| inner.delivered.pop_front())
    }

    /// Number of events the sink has accepted from its handler.
    pub fn delivered_count(&self) -> u64 {
        self.read_memory_inner(0, |inner| inner.delivered_count)
    }

    /// Whether the sink has observed the final retraction ack.
    pub fn closed_with_ack(&self) -> bool {
        self.read_memory_inner(false, |inner| inner.closed_with_ack)
    }

    /// Number of additional events the sink is willing to
    /// accept. The handler reads this between pushes.
    pub fn pending_acceptance(&self) -> usize {
        self.read_memory_inner(usize::MAX, |inner| inner.pending_acceptance)
    }

    /// Consumer acknowledges that it has processed
    /// `additional` events; the handler may push that many
    /// more.
    pub fn accept_additional(&self, additional: usize) {
        self.with_memory_inner((), |inner| {
            inner.pending_acceptance = inner.pending_acceptance.saturating_add(additional);
        });
    }

    fn try_push(&self, event: TranscriptDeliveryEvent) -> Result<(), TranscriptDeliveryEvent> {
        match &self.target {
            TranscriptSubscriptionSinkTarget::Memory(inner) => {
                let mut inner = inner.lock().expect("transcript subscription sink lock");
                if inner.pending_acceptance == 0 {
                    return Err(event);
                }
                inner.pending_acceptance = inner.pending_acceptance.saturating_sub(1);
                if matches!(event, TranscriptDeliveryEvent::FinalAcknowledgement(_)) {
                    inner.closed_with_ack = true;
                }
                inner.delivered.push_back(event);
                inner.delivered_count = inner.delivered_count.saturating_add(1);
                Ok(())
            }
            TranscriptSubscriptionSinkTarget::Channel(sender) => {
                sender.send(event).map_err(|error| error.0)
            }
        }
    }
}

impl Default for TranscriptSubscriptionSink {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for TranscriptSubscriptionSink {
    fn eq(&self, other: &Self) -> bool {
        match (&self.target, &other.target) {
            (
                TranscriptSubscriptionSinkTarget::Memory(left),
                TranscriptSubscriptionSinkTarget::Memory(right),
            ) => Arc::ptr_eq(left, right),
            (
                TranscriptSubscriptionSinkTarget::Channel(left),
                TranscriptSubscriptionSinkTarget::Channel(right),
            ) => left.same_channel(right),
            _ => false,
        }
    }
}

impl Eq for TranscriptSubscriptionSink {}

/// One open subscription's actor. Owns the connection's
/// outbound queue, the close-ack flag, the per-stream cursor,
/// and the consumer sink reference.
#[derive(Debug)]
pub struct TranscriptStreamingReplyHandler {
    token: HarnessTranscriptToken,
    sink: TranscriptSubscriptionSink,
    delivered_deltas: u64,
    buffered_overruns: u64,
    closed: bool,
}

impl TranscriptStreamingReplyHandler {
    pub fn new(token: HarnessTranscriptToken, sink: TranscriptSubscriptionSink) -> Self {
        Self {
            token,
            sink,
            delivered_deltas: 0,
            buffered_overruns: 0,
            closed: false,
        }
    }

    pub fn token(&self) -> &HarnessTranscriptToken {
        &self.token
    }

    pub fn delivered_deltas(&self) -> u64 {
        self.delivered_deltas
    }

    pub fn buffered_overruns(&self) -> u64 {
        self.buffered_overruns
    }

    pub fn closed(&self) -> bool {
        self.closed
    }
}

impl Actor for TranscriptStreamingReplyHandler {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(actor)
    }
}

/// Initial open snapshot to push onto the sink. The manager
/// sends this to the new handler right after spawning it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverSnapshot {
    pub snapshot: HarnessTranscriptSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct TranscriptDeliveryReceipt {
    pub delivered: bool,
    pub overrun: bool,
}

impl Message<DeliverSnapshot> for TranscriptStreamingReplyHandler {
    type Reply = TranscriptDeliveryReceipt;

    async fn handle(
        &mut self,
        message: DeliverSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.closed {
            return TranscriptDeliveryReceipt {
                delivered: false,
                overrun: false,
            };
        }
        match self
            .sink
            .try_push(TranscriptDeliveryEvent::Snapshot(message.snapshot))
        {
            Ok(()) => TranscriptDeliveryReceipt {
                delivered: true,
                overrun: false,
            },
            Err(_) => {
                self.buffered_overruns = self.buffered_overruns.saturating_add(1);
                TranscriptDeliveryReceipt {
                    delivered: false,
                    overrun: true,
                }
            }
        }
    }
}

impl Message<DeliverTranscriptDelta> for TranscriptStreamingReplyHandler {
    type Reply = TranscriptDeliveryReceipt;

    async fn handle(
        &mut self,
        message: DeliverTranscriptDelta,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.closed {
            return TranscriptDeliveryReceipt {
                delivered: false,
                overrun: false,
            };
        }
        match self
            .sink
            .try_push(TranscriptDeliveryEvent::Delta(message.observation))
        {
            Ok(()) => {
                self.delivered_deltas = self.delivered_deltas.saturating_add(1);
                TranscriptDeliveryReceipt {
                    delivered: true,
                    overrun: false,
                }
            }
            Err(_) => {
                self.buffered_overruns = self.buffered_overruns.saturating_add(1);
                TranscriptDeliveryReceipt {
                    delivered: false,
                    overrun: true,
                }
            }
        }
    }
}

/// Close the subscription. The handler drains any in-flight
/// deltas already queued (none, in this in-process model —
/// the sink already received them through the prior pushes),
/// then emits the final `HarnessSubscriptionRetracted`
/// acknowledgement onto the sink and marks itself closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmitFinalRetractionAck;

impl Message<EmitFinalRetractionAck> for TranscriptStreamingReplyHandler {
    type Reply = TranscriptDeliveryReceipt;

    async fn handle(
        &mut self,
        _message: EmitFinalRetractionAck,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.closed {
            return TranscriptDeliveryReceipt {
                delivered: false,
                overrun: false,
            };
        }
        let ack = HarnessSubscriptionRetracted {
            token: self.token.clone(),
        };
        match self
            .sink
            .try_push(TranscriptDeliveryEvent::FinalAcknowledgement(ack))
        {
            Ok(()) => {
                self.closed = true;
                TranscriptDeliveryReceipt {
                    delivered: true,
                    overrun: false,
                }
            }
            Err(_) => {
                self.buffered_overruns = self.buffered_overruns.saturating_add(1);
                TranscriptDeliveryReceipt {
                    delivered: false,
                    overrun: true,
                }
            }
        }
    }
}

/// Read the handler's witness counters. Tests use this to
/// assert per-handler delivery progress without depending on
/// the consumer sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadHandlerStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct TranscriptStreamingReplyHandlerStatus {
    pub delivered_deltas: u64,
    pub buffered_overruns: u64,
    pub closed: bool,
}

impl Message<ReadHandlerStatus> for TranscriptStreamingReplyHandler {
    type Reply = TranscriptStreamingReplyHandlerStatus;

    async fn handle(
        &mut self,
        _message: ReadHandlerStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        TranscriptStreamingReplyHandlerStatus {
            delivered_deltas: self.delivered_deltas,
            buffered_overruns: self.buffered_overruns,
            closed: self.closed,
        }
    }
}

/// The subscription manager — single owner of the open
/// subscription set. Each entry pairs the per-stream token
/// with the handler that serves it.
#[derive(Debug)]
pub struct TranscriptSubscriptionManager {
    open: Vec<TranscriptSubscriptionEntry>,
    next_subscription_identifier: u64,
    opened_count: u64,
    closed_count: u64,
}

#[derive(Debug, Clone)]
struct TranscriptSubscriptionEntry {
    token: HarnessTranscriptToken,
    handler: ActorRef<TranscriptStreamingReplyHandler>,
}

impl TranscriptSubscriptionManager {
    pub fn new() -> Self {
        Self {
            open: Vec::new(),
            next_subscription_identifier: 1,
            opened_count: 0,
            closed_count: 0,
        }
    }

    pub fn open_count(&self) -> usize {
        self.open.len()
    }

    pub fn opened_count(&self) -> u64 {
        self.opened_count
    }

    pub fn closed_count(&self) -> u64 {
        self.closed_count
    }
}

impl Default for TranscriptSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for TranscriptSubscriptionManager {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        manager: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(manager)
    }
}

/// Manager's reply to `OpenTranscriptSubscription`. Carries
/// the per-stream token (so the consumer can later close the
/// subscription by name) and the handler reference (so the
/// publisher can fan deltas out).
#[derive(Debug, Clone, kameo::Reply)]
pub struct OpenedTranscriptSubscription {
    pub token: HarnessTranscriptToken,
    pub handler: ActorRef<TranscriptStreamingReplyHandler>,
    pub snapshot: HarnessTranscriptSnapshot,
}

impl Message<OpenTranscriptSubscription> for TranscriptSubscriptionManager {
    type Reply = OpenedTranscriptSubscription;

    async fn handle(
        &mut self,
        message: OpenTranscriptSubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let subscription =
            HarnessTranscriptSubscriptionIdentifier::new(self.next_subscription_identifier);
        self.next_subscription_identifier = self.next_subscription_identifier.saturating_add(1);
        let token = HarnessTranscriptToken {
            harness: message.harness.clone(),
            subscription,
        };
        let snapshot = HarnessTranscriptSnapshot {
            token: token.clone(),
            current_sequence: HarnessTranscriptSequence::new(0),
        };
        let handler = TranscriptStreamingReplyHandler::spawn(TranscriptStreamingReplyHandler::new(
            token.clone(),
            message.sink,
        ));
        handler.wait_for_startup().await;
        let _ = handler
            .ask(DeliverSnapshot {
                snapshot: snapshot.clone(),
            })
            .await;
        self.open.push(TranscriptSubscriptionEntry {
            token: token.clone(),
            handler: handler.clone(),
        });
        self.opened_count = self.opened_count.saturating_add(1);
        OpenedTranscriptSubscription {
            token,
            handler,
            snapshot,
        }
    }
}

/// Manager's reply when a close request resolves. Carries
/// whether a matching subscription was found and closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct ClosedTranscriptSubscription {
    pub closed: bool,
}

impl Message<CloseTranscriptSubscription> for TranscriptSubscriptionManager {
    type Reply = ClosedTranscriptSubscription;

    async fn handle(
        &mut self,
        message: CloseTranscriptSubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let position = self
            .open
            .iter()
            .position(|entry| entry.token == message.token);
        let Some(position) = position else {
            return ClosedTranscriptSubscription { closed: false };
        };
        let entry = self.open.remove(position);
        let _ = entry.handler.ask(EmitFinalRetractionAck).await;
        let _ = entry.handler.stop_gracefully().await;
        entry.handler.wait_for_shutdown().await;
        self.closed_count = self.closed_count.saturating_add(1);
        ClosedTranscriptSubscription { closed: true }
    }
}

/// Read the manager's witness counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadManagerStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct TranscriptSubscriptionManagerStatus {
    pub open_count: usize,
    pub opened_count: u64,
    pub closed_count: u64,
}

impl Message<ReadManagerStatus> for TranscriptSubscriptionManager {
    type Reply = TranscriptSubscriptionManagerStatus;

    async fn handle(
        &mut self,
        _message: ReadManagerStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        TranscriptSubscriptionManagerStatus {
            open_count: self.open.len(),
            opened_count: self.opened_count,
            closed_count: self.closed_count,
        }
    }
}

/// Snapshot of registered handlers, for the publisher to fan
/// deltas out to. Returned by the manager when the publisher
/// asks for the current routing table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadSubscriptionHandlers;

#[derive(Debug, Clone, kameo::Reply)]
pub struct SubscriptionHandlers {
    pub handlers: Vec<ActorRef<TranscriptStreamingReplyHandler>>,
}

impl Message<ReadSubscriptionHandlers> for TranscriptSubscriptionManager {
    type Reply = SubscriptionHandlers;

    async fn handle(
        &mut self,
        _message: ReadSubscriptionHandlers,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        SubscriptionHandlers {
            handlers: self
                .open
                .iter()
                .map(|entry| entry.handler.clone())
                .collect(),
        }
    }
}

/// The delta publisher. Receives `PublishTranscriptObservation`
/// from the `Harness` actor and fans the observation out to
/// every handler the manager has registered.
#[derive(Debug)]
pub struct TranscriptDeltaPublisher {
    manager: ActorRef<TranscriptSubscriptionManager>,
    published_count: u64,
    fanned_out_count: u64,
}

impl TranscriptDeltaPublisher {
    pub fn new(manager: ActorRef<TranscriptSubscriptionManager>) -> Self {
        Self {
            manager,
            published_count: 0,
            fanned_out_count: 0,
        }
    }

    pub fn published_count(&self) -> u64 {
        self.published_count
    }

    pub fn fanned_out_count(&self) -> u64 {
        self.fanned_out_count
    }
}

impl Actor for TranscriptDeltaPublisher {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        publisher: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(publisher)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct TranscriptPublicationReceipt {
    pub published: bool,
    pub fanned_out: usize,
}

impl Message<PublishTranscriptObservation> for TranscriptDeltaPublisher {
    type Reply = TranscriptPublicationReceipt;

    async fn handle(
        &mut self,
        message: PublishTranscriptObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.published_count = self.published_count.saturating_add(1);
        let handlers = match self.manager.ask(ReadSubscriptionHandlers).await {
            Ok(reply) => reply.handlers,
            Err(_) => Vec::new(),
        };
        let mut fanned_out = 0;
        for handler in handlers {
            let receipt = handler
                .ask(DeliverTranscriptDelta {
                    observation: message.observation.clone(),
                })
                .await;
            if let Ok(receipt) = receipt
                && receipt.delivered
            {
                fanned_out += 1;
            }
        }
        self.fanned_out_count = self.fanned_out_count.saturating_add(fanned_out as u64);
        TranscriptPublicationReceipt {
            published: true,
            fanned_out,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPublisherStatus;

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct TranscriptDeltaPublisherStatus {
    pub published_count: u64,
    pub fanned_out_count: u64,
}

impl Message<ReadPublisherStatus> for TranscriptDeltaPublisher {
    type Reply = TranscriptDeltaPublisherStatus;

    async fn handle(
        &mut self,
        _message: ReadPublisherStatus,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        TranscriptDeltaPublisherStatus {
            published_count: self.published_count,
            fanned_out_count: self.fanned_out_count,
        }
    }
}
