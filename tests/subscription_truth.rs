//! Witnesses for the transcript-observation subscription
//! producer plane. Each test spawns real Kameo actors and
//! drives real mailbox round-trips — no mock fanout — so the
//! tests prove the three-actor pattern (manager, handler,
//! publisher) is the path the producer took.

use harness::{
    CloseTranscriptSubscription, OpenTranscriptSubscription, PublishStreamEvent, ReadHandlerStatus,
    ReadManagerStatus, ReadPublisherStatus, TranscriptDeliveryEvent, TranscriptDeltaPublisher,
    TranscriptSubscriptionManager, TranscriptSubscriptionSink,
};
use kameo::actor::{ActorRef, Spawn};
use signal_harness::{
    HarnessName, HarnessStreamEvent, HarnessTranscriptSequence,
    HarnessTranscriptSubscriptionIdentifier, HarnessTranscriptToken, TranscriptObservation,
};

struct SubscriptionFixture {
    manager: ActorRef<TranscriptSubscriptionManager>,
    publisher: ActorRef<TranscriptDeltaPublisher>,
}

impl SubscriptionFixture {
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

fn observation(harness: &str, sequence: u64, line: &str) -> TranscriptObservation {
    TranscriptObservation {
        harness: HarnessName::new(harness),
        sequence: HarnessTranscriptSequence::new(sequence),
        line: line.to_string(),
    }
}

#[tokio::test]
async fn subscription_open_returns_typed_snapshot_with_per_stream_token() {
    let fixture = SubscriptionFixture::start().await;
    let sink = TranscriptSubscriptionSink::new();

    let opened = fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("designer"),
            sink: sink.clone(),
        })
        .await
        .expect("manager accepts open");

    // The token names the harness and the open subscription; the snapshot
    // carries that token plus the sequence cursor at 0.
    assert_eq!(opened.token.harness.as_str(), "designer");
    assert_eq!(opened.token.subscription.into_u64(), 1);
    assert_eq!(opened.snapshot.token, opened.token);
    assert_eq!(opened.snapshot.current_sequence.into_u64(), 0);

    // The first event the sink received is the snapshot.
    let first = sink.next_delivered().expect("snapshot was delivered");
    match first {
        TranscriptDeliveryEvent::Snapshot(snapshot) => {
            assert_eq!(snapshot.token, opened.token);
            assert_eq!(snapshot.current_sequence.into_u64(), 0);
        }
        other => panic!("expected snapshot, got {other:?}"),
    }

    // Manager state: one subscription open, one opened total.
    let status = fixture
        .manager
        .ask(ReadManagerStatus)
        .await
        .expect("manager status");
    assert_eq!(status.open_count, 1);
    assert_eq!(status.opened_count, 1);
    assert_eq!(status.closed_count, 0);

    fixture.stop().await;
}

#[tokio::test]
async fn publisher_fans_typed_deltas_to_open_subscription() {
    let fixture = SubscriptionFixture::start().await;
    let sink = TranscriptSubscriptionSink::new();

    let _opened = fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("designer"),
            sink: sink.clone(),
        })
        .await
        .expect("open");

    // Drain the snapshot first.
    sink.next_delivered();

    // Publish three deltas; the publisher fans them through
    // the manager's handler.
    for sequence in 1..=3 {
        let receipt = fixture
            .publisher
            .ask(PublishStreamEvent {
                event: observation("designer", sequence, &format!("line {sequence}")).into(),
            })
            .await
            .expect("publish");
        assert!(receipt.published);
        assert_eq!(receipt.fanned_out, 1);
    }

    // The sink received exactly three deltas, in order.
    for sequence in 1..=3 {
        match sink.next_delivered() {
            Some(TranscriptDeliveryEvent::Delta(HarnessStreamEvent::TranscriptObservation(
                observation,
            ))) => {
                assert_eq!(observation.sequence.into_u64(), sequence);
                assert_eq!(observation.line, format!("line {sequence}"));
            }
            other => panic!("expected delta at sequence {sequence}, got {other:?}"),
        }
    }
    assert!(sink.next_delivered().is_none());

    // Publisher counters reflect three publications, three
    // fanned-out events.
    let status = fixture
        .publisher
        .ask(ReadPublisherStatus)
        .await
        .expect("publisher status");
    assert_eq!(status.published_count, 3);
    assert_eq!(status.fanned_out_count, 3);

    fixture.stop().await;
}

#[tokio::test]
async fn subscription_close_emits_final_acknowledgement_before_end() {
    let fixture = SubscriptionFixture::start().await;
    let sink = TranscriptSubscriptionSink::new();

    let opened = fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("operator"),
            sink: sink.clone(),
        })
        .await
        .expect("open");

    // Drain snapshot.
    sink.next_delivered();

    // Publish one delta.
    fixture
        .publisher
        .ask(PublishStreamEvent {
            event: observation("operator", 1, "first").into(),
        })
        .await
        .expect("publish");
    sink.next_delivered();

    // Close.
    let closed = fixture
        .manager
        .ask(CloseTranscriptSubscription {
            token: opened.token.clone(),
        })
        .await
        .expect("close");
    assert!(closed.closed);

    // The next (and last) event on the sink is the final
    // acknowledgement carrying the same token.
    match sink.next_delivered() {
        Some(TranscriptDeliveryEvent::FinalAcknowledgement(ack)) => {
            assert_eq!(ack.token, opened.token);
        }
        other => panic!("expected final ack, got {other:?}"),
    }
    assert!(sink.next_delivered().is_none());
    assert!(sink.closed_with_ack());

    // Manager state: zero open, one closed.
    let status = fixture
        .manager
        .ask(ReadManagerStatus)
        .await
        .expect("manager status");
    assert_eq!(status.open_count, 0);
    assert_eq!(status.closed_count, 1);

    fixture.stop().await;
}

#[tokio::test]
async fn close_after_publish_drops_further_deltas_to_closed_subscription() {
    let fixture = SubscriptionFixture::start().await;
    let sink = TranscriptSubscriptionSink::new();

    let opened = fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("designer"),
            sink: sink.clone(),
        })
        .await
        .expect("open");

    // Drain snapshot.
    sink.next_delivered();

    // Publish, close, then try to publish again.
    fixture
        .publisher
        .ask(PublishStreamEvent {
            event: observation("designer", 1, "before-close").into(),
        })
        .await
        .expect("publish");
    fixture
        .manager
        .ask(CloseTranscriptSubscription {
            token: opened.token.clone(),
        })
        .await
        .expect("close");

    // After close, the subscription is unregistered; the
    // publisher fanout finds zero handlers.
    let receipt = fixture
        .publisher
        .ask(PublishStreamEvent {
            event: observation("designer", 2, "after-close").into(),
        })
        .await
        .expect("publish");
    assert!(receipt.published);
    assert_eq!(receipt.fanned_out, 0);

    // The sink has delta+ack and nothing more.
    let mut events = Vec::new();
    while let Some(event) = sink.next_delivered() {
        events.push(event);
    }
    assert_eq!(events.len(), 2, "expected delta + ack, got {events:?}");
    assert!(matches!(events[0], TranscriptDeliveryEvent::Delta(_)));
    assert!(matches!(
        events[1],
        TranscriptDeliveryEvent::FinalAcknowledgement(_)
    ));

    fixture.stop().await;
}

#[tokio::test]
async fn slow_subscriber_does_not_block_sibling_subscription() {
    let fixture = SubscriptionFixture::start().await;

    // Two subscribers. The "slow" one's sink has acceptance
    // capacity of just 1 (the snapshot itself fills it); it
    // cannot accept any delta until the consumer acknowledges.
    let slow_sink = TranscriptSubscriptionSink::with_acceptance(1);
    let fast_sink = TranscriptSubscriptionSink::new();

    let slow = fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("slow"),
            sink: slow_sink.clone(),
        })
        .await
        .expect("open slow");

    let _fast = fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("fast"),
            sink: fast_sink.clone(),
        })
        .await
        .expect("open fast");

    // Drain snapshots.
    slow_sink.next_delivered();
    fast_sink.next_delivered();

    // Publish three deltas. The slow sink has no acceptance
    // capacity left after the snapshot, so all three deltas
    // overrun its handler. The fast sink takes all three.
    for sequence in 1..=3 {
        let receipt = fixture
            .publisher
            .ask(PublishStreamEvent {
                event: observation("multi", sequence, &format!("line {sequence}")).into(),
            })
            .await
            .expect("publish");
        assert!(receipt.published);
        // Both handlers were asked; the slow one overran each
        // time so only one delivered ack came back.
        assert_eq!(receipt.fanned_out, 1, "fast subscriber should deliver");
    }

    // Slow handler's status: zero deltas delivered, three
    // buffered overruns; not closed.
    let slow_status = fixture
        .manager
        .ask(harness::ReadSubscriptionHandlers)
        .await
        .expect("handlers")
        .handlers;
    // Find the slow handler by looking up its delivery count.
    let mut slow_delivered = u64::MAX;
    let mut slow_overruns = u64::MAX;
    for handler in &slow_status {
        let status = handler
            .ask(ReadHandlerStatus)
            .await
            .expect("handler status");
        if status.buffered_overruns > 0 {
            slow_delivered = status.delivered_deltas;
            slow_overruns = status.buffered_overruns;
        }
    }
    assert_eq!(slow_delivered, 0);
    assert_eq!(slow_overruns, 3);

    // Fast subscriber's sink received all three deltas.
    let mut fast_deltas = 0_u64;
    while let Some(event) = fast_sink.next_delivered() {
        if matches!(event, TranscriptDeliveryEvent::Delta(_)) {
            fast_deltas += 1;
        }
    }
    assert_eq!(fast_deltas, 3);

    // Clean up.
    fixture
        .manager
        .ask(CloseTranscriptSubscription {
            token: slow.token.clone(),
        })
        .await
        .expect("close slow");

    fixture.stop().await;
}

#[tokio::test]
async fn second_close_for_same_token_is_idempotent_returns_false() {
    let fixture = SubscriptionFixture::start().await;
    let sink = TranscriptSubscriptionSink::new();
    let opened = fixture
        .manager
        .ask(OpenTranscriptSubscription {
            harness: HarnessName::new("designer"),
            sink: sink.clone(),
        })
        .await
        .expect("open");

    let first = fixture
        .manager
        .ask(CloseTranscriptSubscription {
            token: opened.token.clone(),
        })
        .await
        .expect("first close");
    assert!(first.closed);

    let second = fixture
        .manager
        .ask(CloseTranscriptSubscription {
            token: opened.token.clone(),
        })
        .await
        .expect("second close");
    assert!(
        !second.closed,
        "second close should report no subscription matched"
    );

    fixture.stop().await;
}

#[tokio::test]
async fn unknown_token_close_reports_not_found() {
    let fixture = SubscriptionFixture::start().await;
    let phantom = HarnessTranscriptToken {
        harness: HarnessName::new("phantom"),
        subscription: HarnessTranscriptSubscriptionIdentifier::new(9000),
    };
    let receipt = fixture
        .manager
        .ask(CloseTranscriptSubscription { token: phantom })
        .await
        .expect("close phantom");
    assert!(!receipt.closed);
    fixture.stop().await;
}
