use std::fs;
use std::path::{Path, PathBuf};

use harness::{
    Harness, HarnessBinding, HarnessIdentifier, HarnessKind, HarnessLifecycle, ReadState,
    RecordTranscriptLine, SetHarnessLifecycle, TranscriptLine,
};

struct SourceFile {
    path: PathBuf,
    content: String,
}

impl SourceFile {
    fn read(path: PathBuf) -> Self {
        let content = fs::read_to_string(&path).expect("source file is readable");
        Self { path, content }
    }

    fn is_guard_source(&self) -> bool {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "actor_runtime_truth.rs")
    }

    fn contains(&self, fragment: &str) -> bool {
        self.content.contains(fragment)
    }
}

struct SourceTree {
    root: PathBuf,
}

impl SourceTree {
    fn new() -> Self {
        Self {
            root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        }
    }

    fn guarded_files(&self) -> Vec<SourceFile> {
        let mut files = vec![self.root.join("Cargo.toml"), self.root.join("Cargo.lock")];
        files.extend(self.source_files());
        files.extend(self.test_files());
        files.into_iter().map(SourceFile::read).collect()
    }

    fn source_files(&self) -> Vec<PathBuf> {
        let src = self.root.join("src");
        fs::read_dir(src)
            .expect("source directory is readable")
            .map(|entry| entry.expect("source entry is readable").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
            .collect()
    }

    fn test_files(&self) -> Vec<PathBuf> {
        let tests = self.root.join("tests");
        fs::read_dir(tests)
            .expect("tests directory is readable")
            .map(|entry| entry.expect("test entry is readable").path())
            .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
            .collect()
    }
}

#[test]
fn harness_actor_cannot_use_non_kameo_runtime() {
    let forbidden_fragments = [
        "ractor =",
        "name = \"ractor\"",
        "use ractor",
        "ractor::",
        "RpcReplyPort",
        "ActorProcessingErr",
    ];

    let mut violations = Vec::new();
    for file in SourceTree::new().guarded_files() {
        if file.is_guard_source() {
            continue;
        }
        for fragment in forbidden_fragments {
            if file.contains(fragment) {
                violations.push(format!("{} contains {fragment}", file.path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "non-kameo harness actor runtime violations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn harness_runtime_cannot_be_empty_marker() {
    let source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("runtime.rs"),
    );

    assert!(source.contains("pub struct Harness {"));
    assert!(source.contains("binding: HarnessBinding,"));
    assert!(source.contains("lifecycle: HarnessLifecycle,"));
    assert!(source.contains("transcript_event_count: u64,"));
    assert!(source.contains("pub struct ReadState {"));
    assert!(source.contains("minimum_transcript_events: u64,"));
}

#[test]
fn harness_identity_projection_cannot_leak_everything_by_default() {
    let source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("harness.rs"),
    );

    assert!(source.contains("pub enum HarnessIdentityView {"));
    assert!(source.contains("Full,"));
    assert!(source.contains("Redacted,"));
    assert!(source.contains("Hidden,"));
    assert!(!source.contains("Access"));
    assert!(source.contains("pub struct HarnessIdentityProjection {"));
    assert!(source.contains("id: Option<HarnessIdentifier>,"));
    assert!(source.contains("kind: Option<HarnessKind>,"));
    assert!(source.contains("working_directory: Option<String>,"));
}

#[test]
fn harness_kind_is_closed_schema_enum() {
    let source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("harness.rs"),
    );

    assert!(source.contains("pub enum HarnessKind {"));
    assert!(source.contains("Codex,"));
    assert!(source.contains("Claude,"));
    assert!(source.contains("Pi,"));
    assert!(source.contains("Fixture,"));
    assert!(!source.contains("Other {"));
    assert!(!source.contains("name: String"));
}

/// Witnesses that `HarnessKind` has exactly the four variants the
/// architecture commits to — `Codex`, `Claude`, `Pi`, `Fixture` — and
/// no fifth variant. The exhaustive `match` body forces every variant
/// to be observed; the assertion that each variant is hit at most once
/// confirms the closed set.
#[test]
fn harness_kind_includes_all_four_variants() {
    use std::collections::HashSet;

    let kinds = [
        HarnessKind::Codex,
        HarnessKind::Claude,
        HarnessKind::Pi,
        HarnessKind::Fixture,
    ];

    let mut observed: HashSet<&'static str> = HashSet::new();
    for kind in kinds {
        let label = match kind {
            HarnessKind::Codex => "Codex",
            HarnessKind::Claude => "Claude",
            HarnessKind::Pi => "Pi",
            HarnessKind::Fixture => "Fixture",
        };
        assert!(observed.insert(label), "variant `{label}` covered twice");
    }

    assert_eq!(observed.len(), 4);
}

/// Witnesses that the daemon no longer exposes a kind-specific argv
/// spelling table. The only production input is the single typed
/// `HarnessDaemonConfiguration` argument, where `HarnessKind` is a
/// closed enum field.
#[test]
fn harness_kind_has_no_command_line_argument_projection() {
    let source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("harness.rs"),
    );

    assert!(!source.contains("from_argument_value"));
    assert!(!source.contains("as_argument_value"));
}

#[test]
fn terminal_delivery_cannot_use_retired_transport_or_sleep_verification() {
    let source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("terminal.rs"),
    );

    let retired_transport_fragments = [
        ["persona", "_", "wez", "term"].concat(),
        ["Wez", "Term"].concat(),
        ["Wez", "Term", "Mux"].concat(),
        "thread::sleep".to_string(),
        "Duration::from_millis".to_string(),
    ];

    for fragment in retired_transport_fragments {
        assert!(
            !source.contains(fragment.as_str()),
            "terminal delivery still carries retired transport fragment {fragment}"
        );
    }

    assert!(!source.contains("persona_terminal::contract::TerminalTransportBinding"));
    assert!(source.contains("TerminalFrame::new"));
    assert!(source.contains("TerminalFrameBody::Request"));
    assert!(source.contains("TerminalReply::TerminalInputAccepted"));
}

#[test]
fn fixture_human_endpoint_cannot_be_production_delivery() {
    let source = SourceFile::read(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("terminal.rs"),
    );

    assert!(!source.contains("\n    Human,"));
    assert!(!source.contains("HarnessTerminalEndpoint::Human"));
    assert!(source.contains("FixtureOnlyHuman"));
    assert!(source.contains("TerminalDeliveryPath::FixtureOnly"));
    assert!(source.contains("TerminalDeliveryPath::TerminalTransport"));
    assert!(source.contains("delivered: false"));
}

#[tokio::test]
async fn harness_runtime_cannot_forget_lifecycle_between_messages() {
    let harness = Harness::start(binding()).await;

    harness
        .ask(SetHarnessLifecycle {
            lifecycle: HarnessLifecycle::Running,
        })
        .await
        .expect("harness mailbox accepts lifecycle writes");
    let state = harness
        .ask(ReadState::expecting_at_least(0))
        .await
        .expect("harness mailbox accepts state reads");

    assert_eq!(state.lifecycle, HarnessLifecycle::Running);
    assert_eq!(state.binding.id().as_str(), "operator");
    harness.stop_gracefully().await.expect("harness stops");
    harness.wait_for_shutdown().await;
}

#[tokio::test]
async fn harness_runtime_cannot_emit_transcript_for_another_harness() {
    let harness = Harness::start(binding()).await;

    let event = harness
        .ask(RecordTranscriptLine {
            line: TranscriptLine::new("ready"),
        })
        .await
        .expect("harness mailbox accepts transcript writes");
    let state = harness
        .ask(ReadState::expecting_at_least(1))
        .await
        .expect("harness mailbox accepts state reads");

    assert_eq!(event.harness().as_str(), "operator");
    assert_eq!(event.line().as_str(), "ready");
    assert_eq!(state.transcript_event_count, 1);
    harness.stop_gracefully().await.expect("harness stops");
    harness.wait_for_shutdown().await;
}

fn binding() -> HarnessBinding {
    HarnessBinding::new(
        HarnessIdentifier::new("operator"),
        HarnessKind::Codex,
        "/tmp/operator",
    )
}
