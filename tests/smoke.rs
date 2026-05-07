use persona_harness::{HarnessBinding, HarnessId, HarnessKind, TranscriptEvent, TranscriptLine};

#[test]
fn harness_binding_keeps_identity() {
    let binding = HarnessBinding::new(HarnessId::new("operator"), HarnessKind::Codex, "/tmp/op");

    assert_eq!(binding.id().as_str(), "operator");
    assert_eq!(binding.working_directory(), "/tmp/op");
}

#[test]
fn transcript_event_keeps_line() {
    let event = TranscriptEvent::new(HarnessId::new("pi"), TranscriptLine::new("ready"));

    assert_eq!(event.line().as_str(), "ready");
}
