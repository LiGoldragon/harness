//! Witnesses for the recovered-turn → `ClaudeSessionObservation` projection.
//!
//! Each test recovers a real `ClaudeArtifactSnapshot` from an on-disk `.claude`
//! fixture (the same artifact layout the headless observer reads), folds it into
//! an `ObservedClaudeTurn`, and asserts the projected `ClaudeSessionObservation`
//! sources every field from that observation — the response from the recovered
//! turn's own `assistant_text()` getter, the counts from the observer's
//! structured metadata — and leaves `accumulated_context` unset pending the
//! og38.1 sourcing ruling.

use std::fs;

use harness::{ClaudeArtifactObserver, ObservedClaudeTurn};
use signal_harness::{
    AdapterExitStatus, ClaudeSessionLifecycle, HarnessName, StreamedEventCount, TurnLaunch,
};
use tempfile::TempDir;

const WORKING_DIRECTORY: &str = "/tmp/claude-session-observation-proof";

#[test]
fn observed_turn_projects_assistant_text_and_defers_accumulated_context() {
    let fixture = ClaudeFixture::new();
    fixture.write_project_jsonl(
        "session-alpha.jsonl",
        &[
            r#"{"type":"user","cwd":"/tmp/claude-session-observation-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER say hello"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-session-observation-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:06Z","message":{"role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"text","text":"FINAL_MARKER hello there"}],"stop_reason":"end_turn"}}"#,
        ],
    );

    let snapshot = fixture.snapshot();
    let recovered = snapshot.recovered_turn().clone();
    let observed = ObservedClaudeTurn::new(
        HarnessName::from("designer-session-1"),
        TurnLaunch::Fresh,
        ClaudeSessionLifecycle::Completed,
        StreamedEventCount::from(7_i64),
        snapshot,
    );

    let observation = observed.into_session_observation();

    // The per-session harness key and launch/lifecycle records the runtime
    // supplied ride through unchanged.
    assert_eq!(observation.harness_name.as_str(), "designer-session-1");
    assert_eq!(observation.turn_launch, TurnLaunch::Fresh);
    assert_eq!(
        observation.claude_session_lifecycle,
        ClaudeSessionLifecycle::Completed
    );
    assert_eq!(observation.streamed_event_count, 7);

    // Session facts recovered from the JSONL transcript.
    assert_eq!(
        observation.claude_session_identifier_option.as_deref(),
        Some("session-alpha")
    );
    assert_eq!(
        observation.claude_model_option.as_deref(),
        Some("claude-3-5-haiku-latest")
    );
    assert!(observation.reached_end_of_turn);

    // The response is sourced from the recovered turn's assistant_text getter,
    // NOT the Claude CLI `result` line.
    assert_eq!(
        observation.assistant_response_text_option.as_deref(),
        Some("FINAL_MARKER hello there")
    );
    assert_eq!(
        observation.assistant_response_text_option.as_deref(),
        recovered.assistant_text().as_deref()
    );

    // The transcript path is the JSONL file the observer discovered.
    let transcript_path = observation
        .transcript_path_option
        .as_ref()
        .expect("transcript path discovered");
    assert!(transcript_path.as_str().ends_with("session-alpha.jsonl"));

    // accumulated_context is DEFERRED (og38.1): the projection synthesizes no
    // figure until the A/B sourcing ruling lands.
    assert_eq!(observation.context_tokens_option, None);

    // Display/ordering timestamp is infrastructure-minted and non-zero.
    assert!(observation.timestamp_nanoseconds > 0);
}

#[test]
fn observed_turn_counts_tool_calls_and_status_transitions_from_metadata() {
    let fixture = ClaudeFixture::new();
    fixture.write_project_jsonl(
        "session-beta.jsonl",
        &[
            r#"{"type":"user","cwd":"/tmp/claude-session-observation-proof","sessionId":"session-beta","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER create README"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-session-observation-proof","sessionId":"session-beta","timestamp":"2026-06-28T10:00:02Z","message":{"role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"tool_use","id":"tool-1","name":"Write","input":{"file_path":"README.md","content":"hello"}}]}}"#,
            r#"{"type":"user","cwd":"/tmp/claude-session-observation-proof","sessionId":"session-beta","timestamp":"2026-06-28T10:00:04Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"wrote README.md"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-session-observation-proof","sessionId":"session-beta","timestamp":"2026-06-28T10:00:06Z","message":{"role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"text","text":"FINAL_MARKER done"}],"stop_reason":"end_turn"}}"#,
        ],
    );

    let snapshot = fixture.snapshot_for("session-beta");
    let recovered = snapshot.recovered_turn().clone();
    let expected_tool_calls = recovered.tool_calls().len() as i64;
    let expected_status_transitions = recovered.status_transitions().len() as i64;
    assert_eq!(expected_tool_calls, 1);
    assert!(expected_status_transitions > 0);

    let observed = ObservedClaudeTurn::new(
        HarnessName::from("designer-session-2"),
        TurnLaunch::Resumed,
        ClaudeSessionLifecycle::Exited(AdapterExitStatus::Success),
        StreamedEventCount::from(12_i64),
        snapshot,
    );
    let observation = observed.into_session_observation();

    assert_eq!(observation.turn_launch, TurnLaunch::Resumed);
    assert_eq!(
        observation.claude_session_lifecycle,
        ClaudeSessionLifecycle::Exited(AdapterExitStatus::Success)
    );
    assert_eq!(observation.tool_call_count, expected_tool_calls);
    assert_eq!(
        observation.status_transition_count,
        expected_status_transitions
    );
    assert_eq!(
        observation.assistant_response_text_option.as_deref(),
        Some("FINAL_MARKER done")
    );
    assert_eq!(observation.context_tokens_option, None);
}

#[test]
fn observed_turn_with_no_assistant_text_leaves_response_absent() {
    let fixture = ClaudeFixture::new();
    fixture.write_project_jsonl(
        "session-gamma.jsonl",
        &[
            r#"{"type":"user","cwd":"/tmp/claude-session-observation-proof","sessionId":"session-gamma","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER work silently"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-session-observation-proof","sessionId":"session-gamma","timestamp":"2026-06-28T10:00:02Z","message":{"role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"tool_use","id":"tool-1","name":"Write","input":{"file_path":"NOTES.md","content":"x"}}]}}"#,
        ],
    );

    let snapshot = fixture.snapshot_for("session-gamma");
    let observed = ObservedClaudeTurn::new(
        HarnessName::from("designer-session-3"),
        TurnLaunch::SelfHealed,
        ClaudeSessionLifecycle::Active,
        StreamedEventCount::from(3_i64),
        snapshot,
    );
    let observation = observed.into_session_observation();

    assert_eq!(observation.turn_launch, TurnLaunch::SelfHealed);
    assert_eq!(
        observation.claude_session_lifecycle,
        ClaudeSessionLifecycle::Active
    );
    assert!(!observation.reached_end_of_turn);
    // A turn still in flight observed no assistant text: the response is
    // genuinely absent, not an empty string.
    assert_eq!(observation.assistant_response_text_option, None);
    assert_eq!(observation.context_tokens_option, None);
}

struct ClaudeFixture {
    home: TempDir,
}

impl ClaudeFixture {
    fn new() -> Self {
        Self {
            home: TempDir::new().expect("create fixture home"),
        }
    }

    fn project_directory(&self) -> std::path::PathBuf {
        self.home
            .path()
            .join(".claude")
            .join("projects")
            .join(WORKING_DIRECTORY.replace('/', "-"))
    }

    fn write_project_jsonl(&self, file_name: &str, lines: &[&str]) {
        let directory = self.project_directory();
        fs::create_dir_all(&directory).expect("create project directory");
        fs::write(directory.join(file_name), lines.join("\n")).expect("write project jsonl");
    }

    fn snapshot(&self) -> harness::ClaudeArtifactSnapshot {
        ClaudeArtifactObserver::with_home(self.home.path(), WORKING_DIRECTORY)
            .snapshot()
            .expect("snapshot recovers fixture")
    }

    fn snapshot_for(&self, session_identifier: &str) -> harness::ClaudeArtifactSnapshot {
        ClaudeArtifactObserver::with_home(self.home.path(), WORKING_DIRECTORY)
            .with_session_identifier(session_identifier)
            .snapshot()
            .expect("snapshot recovers requested session")
    }
}
