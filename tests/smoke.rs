use harness::{
    HarnessBinding, HarnessIdentifier, HarnessIdentityView, HarnessKind, HarnessTerminalBinding,
    HarnessTerminalDelivery, HarnessTerminalEndpoint, TerminalDeliveryPath, TranscriptEvent,
    TranscriptLine,
};
use signal_terminal::{Input as TerminalInputRoot, TerminalInput, TerminalInputBytes};

#[test]
fn harness_binding_keeps_identity() {
    let binding = HarnessBinding::new(
        HarnessIdentifier::new("operator"),
        HarnessKind::Codex,
        "/tmp/op",
    );

    assert_eq!(binding.id().as_str(), "operator");
    assert_eq!(binding.working_directory(), "/tmp/op");
}

#[test]
fn harness_identity_projection_keeps_full_owner_view() {
    let binding = HarnessBinding::new(
        HarnessIdentifier::new("operator"),
        HarnessKind::Codex,
        "/tmp/op",
    );
    let projection = binding.identity_projection(HarnessIdentityView::Full);

    assert_eq!(
        projection.id().expect("full view keeps id").as_str(),
        "operator"
    );
    assert_eq!(projection.kind(), Some(&HarnessKind::Codex));
    assert_eq!(projection.working_directory(), Some("/tmp/op"));
}

#[test]
fn harness_identity_projection_redacts_non_owner_view() {
    let binding = HarnessBinding::new(
        HarnessIdentifier::new("operator"),
        HarnessKind::Codex,
        "/tmp/op",
    );
    let projection = binding.identity_projection(HarnessIdentityView::Redacted);

    assert_eq!(
        projection.id().expect("redacted view keeps id").as_str(),
        "operator"
    );
    assert_eq!(projection.kind(), None);
    assert_eq!(projection.working_directory(), None);
}

#[test]
fn harness_identity_projection_hides_unapproved_external_view() {
    let binding = HarnessBinding::new(
        HarnessIdentifier::new("operator"),
        HarnessKind::Codex,
        "/tmp/op",
    );
    let projection = binding.identity_projection(HarnessIdentityView::Hidden);

    assert_eq!(projection.id(), None);
    assert_eq!(projection.kind(), None);
    assert_eq!(projection.working_directory(), None);
}

#[test]
fn transcript_event_keeps_line() {
    let event = TranscriptEvent::new(HarnessIdentifier::new("pi"), TranscriptLine::new("ready"));

    assert_eq!(event.line().as_str(), "ready");
}

#[test]
fn terminal_binding_defaults_terminal_name_to_harness_id() {
    let binding = HarnessTerminalBinding::for_harness(HarnessIdentifier::new("operator"));

    assert_eq!(binding.harness().as_str(), "operator");
    assert_eq!(binding.terminal().as_str(), "operator");
}

#[test]
fn terminal_binding_builds_typed_input_request() {
    let binding = HarnessTerminalBinding::for_harness(HarnessIdentifier::new("operator"));
    let request = binding.input_request(b"hello\r".to_vec());

    assert_eq!(
        request,
        TerminalInputRoot::TerminalInput(TerminalInput {
            terminal: binding.terminal().clone(),
            bytes: TerminalInputBytes::new(
                b"hello\r".iter().map(|byte| u64::from(*byte)).collect()
            ),
        })
    );
}

#[test]
fn fixture_only_human_terminal_endpoint_cannot_claim_transport_delivery() {
    let binding = HarnessTerminalBinding::for_harness(HarnessIdentifier::new("operator"));
    let mut delivery = HarnessTerminalDelivery::new(HarnessTerminalEndpoint::fixture_only_human());
    let receipt = delivery
        .deliver_text(&binding, "local")
        .expect("fixture-only endpoint has no transport failure");

    assert!(!receipt.delivered());
    assert_eq!(receipt.path(), TerminalDeliveryPath::FixtureOnly);
    assert_eq!(receipt.accepted_event(), None);
    assert_eq!(delivery.delivered_input_count(), 0);
}
