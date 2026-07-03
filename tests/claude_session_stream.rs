//! Witnesses that a `ClaudeSessionObservation` is PUSHED to subscribers on the
//! same multi-watch `HarnessTranscriptStream` as transcript lines, with no
//! polling on the consumer path.
//!
//! Each test spawns the real producer-plane actors (`TranscriptSubscriptionManager`
//! + `TranscriptDeltaPublisher`) and subscribes through a channel-backed sink.
//! The subscriber's only wait is `UnboundedReceiver::recv().await` — the task
//! parks until the producer pushes and is woken by the push, never a `sleep` or
//! interval tick. The push-quiescence assertion (`try_recv` is `Empty` before
//! the push) is the no-poll witness: the stream produces an event only when the
//! producer publishes one.

use harness::{
    OpenTranscriptSubscription, PublishStreamEvent, TranscriptDeliveryEvent,
    TranscriptDeltaPublisher, TranscriptSubscriptionManager, TranscriptSubscriptionSink,
};
use kameo::actor::{ActorRef, Spawn};
use signal_harness::{
    AssistantResponseText, ClaudeModel, ClaudeSessionIdentifier, ClaudeSessionLifecycle,
    ClaudeSessionObservation, HarnessName, HarnessStreamEvent, StatusTransitionCount,
    StreamedEventCount, ToolCallCount, TranscriptPath, TurnLaunch,
};
use signal_persona::TimestampNanos;
use tokio::sync::mpsc::error::TryRecvError;

fn session_observation(harness: &str) -> ClaudeSessionObservation {
    ClaudeSessionObservation {
        harness: HarnessName::new(harness),
        session_identifier: Some(ClaudeSessionIdentifier::new("session-alpha")),
        model: Some(ClaudeModel::new("claude-3-5-haiku-latest")),
        launch: TurnLaunch::Fresh,
        reached_end_of_turn: true,
        streamed_event_count: StreamedEventCount::new(9),
        tool_call_count: ToolCallCount::new(2),
        status_transition_count: StatusTransitionCount::new(4),
        transcript_path: Some(TranscriptPath::new("/tmp/session-alpha.jsonl")),
        response: Some(AssistantResponseText::new("FINAL_MARKER done")),
        accumulated_context: None,
        last_activity: TimestampNanos::new(1),
        lifecycle: ClaudeSessionLifecycle::Completed,
    }
}

struct StreamFixture {
    manager: ActorRef<TranscriptSubscriptionManager>,
    publisher: ActorRef<TranscriptDeltaPublisher>,
}

impl StreamFixture {
    async fn start() -> Self {
        let manager = TranscriptSubscriptionManager::spawn(TranscriptSubscriptionManager::new());
        manager.wait_for_startup().await;
        let publisher =
            TranscriptDeltaPublisher::spawn(TranscriptDeltaPublisher::new(manager.clone()));
        publisher.wait_for_startup().await;
        Self { manager, publisher }
    }

    async fn stop(self) {
        let _ = self.publisher.stop_gracefully().await;
        self.publisher.wait_for_shutdown().await;
        let _ = self.manager.stop_gracefully().await;
        self.manager.wait_for_shutdown().await;
    }
}

#[tokio::test]
async fn claude_session_observation_is_pushed_to_subscriber_without_polling() {
    let fixture = StreamFixture::start().await;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let sink = TranscriptSubscriptionSink::channel(sender);

    // The subscriber opens once and receives the current-state snapshot on
    // connect — pushed onto its channel, not polled for.
    let opened = fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("designer-session-1"),
            sink,
        })
        .await
        .expect("manager accepts open");
    match receiver.recv().await.expect("snapshot pushed on connect") {
        TranscriptDeliveryEvent::Snapshot(snapshot) => assert_eq!(snapshot.token, opened.token),
        other => panic!("expected snapshot, got {other:?}"),
    }

    // NO-POLL WITNESS: with nothing published, the stream is quiescent. A poll
    // finds no event; only a producer push produces one.
    assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

    // The producer pushes one Claude session observation onto the stream.
    let observation = session_observation("designer-session-1");
    let receipt = fixture
        .publisher
        .ask(PublishStreamEvent {
            event: observation.clone().into(),
        })
        .await
        .expect("publish claude session observation");
    assert!(receipt.published);
    assert_eq!(receipt.fanned_out, 1);

    // The subscriber's park on `recv().await` is woken by the push and yields
    // the ClaudeSessionObservation delta on the SAME stream as transcript lines.
    match receiver.recv().await.expect("observation pushed as delta") {
        TranscriptDeliveryEvent::Delta(HarnessStreamEvent::ClaudeSessionObservation(pushed)) => {
            assert_eq!(pushed, observation);
            assert_eq!(
                pushed.response.as_ref().map(|text| text.as_str()),
                Some("FINAL_MARKER done")
            );
            // The deferred field crosses the wire as absent, never synthesized.
            assert_eq!(pushed.accumulated_context, None);
        }
        other => panic!("expected ClaudeSessionObservation delta, got {other:?}"),
    }

    // Still quiescent after delivery: no ticker, no self-generated traffic.
    assert_eq!(receiver.try_recv().unwrap_err(), TryRecvError::Empty);

    fixture.stop().await;
}

#[tokio::test]
async fn claude_session_observation_fans_out_to_every_open_subscriber() {
    let fixture = StreamFixture::start().await;

    let (first_sender, mut first_receiver) = tokio::sync::mpsc::unbounded_channel();
    let (second_sender, mut second_receiver) = tokio::sync::mpsc::unbounded_channel();

    fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("view-a"),
            sink: TranscriptSubscriptionSink::channel(first_sender),
        })
        .await
        .expect("open first");
    fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("view-b"),
            sink: TranscriptSubscriptionSink::channel(second_sender),
        })
        .await
        .expect("open second");

    // Drain the per-open snapshots.
    first_receiver.recv().await.expect("first snapshot");
    second_receiver.recv().await.expect("second snapshot");

    let observation = session_observation("hosting-harness");
    let receipt = fixture
        .publisher
        .ask(PublishStreamEvent {
            event: observation.clone().into(),
        })
        .await
        .expect("publish");
    assert!(receipt.published);
    assert_eq!(
        receipt.fanned_out, 2,
        "both open subscribers receive the push"
    );

    for receiver in [&mut first_receiver, &mut second_receiver] {
        match receiver.recv().await.expect("observation pushed") {
            TranscriptDeliveryEvent::Delta(HarnessStreamEvent::ClaudeSessionObservation(
                pushed,
            )) => {
                assert_eq!(pushed, observation);
            }
            other => panic!("expected ClaudeSessionObservation delta, got {other:?}"),
        }
    }

    fixture.stop().await;
}
