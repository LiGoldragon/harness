use std::fs;

use harness::ClaudeArtifactObserver;
use tempfile::TempDir;

#[test]
fn claude_artifact_observer_recovers_marked_turn_from_project_jsonl() {
    let fixture = ClaudeFixture::new("/tmp/claude-proof");
    fixture.write_project_jsonl(
        "session-alpha.jsonl",
        &[
            r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:00Z","permissionMode":"bypassPermissions","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER create README"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:02Z","message":{"role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"tool_use","id":"tool-1","name":"Write","input":{"file_path":"README.md","content":"hello"}}]}}"#,
            r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:04Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"wrote README.md\nTOOL_MARKER"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:06Z","message":{"role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"text","text":"FINAL_MARKER done"}],"stop_reason":"end_turn"}}"#,
        ],
    );
    fixture.write_session_file(
        "session-alpha.json",
        r#"{"sessionId":"session-alpha","cwd":"/tmp/claude-proof","permissionMode":"bypassPermissions"}"#,
    );

    let snapshot = ClaudeArtifactObserver::with_home(fixture.home_directory(), "/tmp/claude-proof")
        .snapshot()
        .expect("snapshot recovers fixture");
    let turn = snapshot.recovered_turn();

    assert_eq!(snapshot.session_identifier(), Some("session-alpha"));
    assert_eq!(turn.model(), Some("claude-3-5-haiku-latest"));
    assert!(turn.contains_text("PROMPT_MARKER"));
    assert!(turn.contains_text("FINAL_MARKER"));
    assert!(turn.contains_text("TOOL_MARKER"));
    assert!(turn.has_stop_reason_end_turn());
    assert_eq!(turn.permission_modes(), &["bypassPermissions".to_string()]);
    assert_eq!(turn.tool_calls().len(), 1);
    assert_eq!(turn.tool_calls()[0].name(), "Write");
    assert_eq!(turn.tool_results().len(), 1);
    assert_eq!(turn.file_edits().len(), 1);
    assert_eq!(turn.file_edits()[0].path(), "README.md");
    assert!(
        turn.status_transitions()
            .iter()
            .any(|transition| transition.kind() == "end_turn_observed")
    );
}

#[test]
fn claude_artifact_observer_can_filter_by_session_identifier() {
    let fixture = ClaudeFixture::new("/tmp/claude-proof");
    fixture.write_project_jsonl(
        "session-alpha.jsonl",
        &[r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"wrong session"}]}}"#],
    );
    fixture.write_project_jsonl(
        "session-beta.jsonl",
        &[
            r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-beta","timestamp":"2026-06-28T10:01:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER beta"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-proof","sessionId":"session-beta","timestamp":"2026-06-28T10:01:06Z","message":{"role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"text","text":"FINAL_MARKER beta"}],"stop_reason":"end_turn"}}"#,
        ],
    );

    let snapshot = ClaudeArtifactObserver::with_home(fixture.home_directory(), "/tmp/claude-proof")
        .with_session_identifier("session-beta")
        .snapshot()
        .expect("snapshot recovers requested session");
    let turn = snapshot.recovered_turn();

    assert_eq!(snapshot.session_identifier(), Some("session-beta"));
    assert!(turn.contains_text("PROMPT_MARKER beta"));
    assert!(!turn.contains_text("wrong session"));
    assert!(turn.has_completed_marked_turn("PROMPT_MARKER", "FINAL_MARKER"));
}

#[test]
fn claude_artifact_observer_ignores_transient_partial_jsonl_line() {
    let fixture = ClaudeFixture::new("/tmp/claude-proof");
    fixture.write_project_jsonl(
        "session-alpha.jsonl",
        &[
            r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:06Z","message":{"role":"assistant","content":[{"type":"text","text":"FINAL_MARKER"}],"stop_reason":"end_turn"}}"#,
            r#"{"type":"assistant","cwd":"#,
        ],
    );

    let snapshot = ClaudeArtifactObserver::with_home(fixture.home_directory(), "/tmp/claude-proof")
        .snapshot()
        .expect("partial trailing line is ignored");

    assert!(
        snapshot
            .recovered_turn()
            .has_completed_marked_turn("PROMPT_MARKER", "FINAL_MARKER")
    );
}

#[derive(Debug)]
struct ClaudeFixture {
    home: TempDir,
    current_working_directory: String,
}

impl ClaudeFixture {
    fn new(current_working_directory: &str) -> Self {
        Self {
            home: TempDir::new().expect("create fixture home"),
            current_working_directory: current_working_directory.to_string(),
        }
    }

    fn home_directory(&self) -> &std::path::Path {
        self.home.path()
    }

    fn write_project_jsonl(&self, file_name: &str, lines: &[&str]) {
        let directory = self
            .home
            .path()
            .join(".claude")
            .join("projects")
            .join(self.current_working_directory.replace('/', "-"));
        fs::create_dir_all(&directory).expect("create project directory");
        fs::write(directory.join(file_name), lines.join("\n")).expect("write project jsonl");
    }

    fn write_session_file(&self, file_name: &str, text: &str) {
        let directory = self.home.path().join(".claude").join("sessions");
        fs::create_dir_all(&directory).expect("create session directory");
        fs::write(directory.join(file_name), text).expect("write session json");
    }
}
