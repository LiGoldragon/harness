use std::fs::{self, OpenOptions};
use std::io::Write;
use std::thread;
use std::time::Duration;

use harness::{ClaudeArtifactObserver, ClaudeObservationStrategy};
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
fn claude_artifact_observer_recovered_turn_exposes_assistant_text() {
    let fixture = ClaudeFixture::new("/tmp/claude-proof");
    fixture.write_project_jsonl(
        "session-alpha.jsonl",
        &[
            r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER say hello"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:06Z","message":{"role":"assistant","model":"claude-3-5-haiku-latest","content":[{"type":"text","text":"FINAL_MARKER hello there"}],"stop_reason":"end_turn"}}"#,
        ],
    );

    let snapshot = ClaudeArtifactObserver::with_home(fixture.home_directory(), "/tmp/claude-proof")
        .snapshot()
        .expect("snapshot recovers fixture");
    let turn = snapshot.recovered_turn();

    assert_eq!(
        turn.assistant_text().as_deref(),
        Some("FINAL_MARKER hello there")
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
fn claude_artifact_observer_requires_final_marker_from_assistant_message() {
    let fixture = ClaudeFixture::new("/tmp/claude-proof");
    fixture.write_project_jsonl(
        "session-alpha.jsonl",
        &[
            r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER reply with FINAL_MARKER"}]}}"#,
            r#"{"type":"assistant","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:06Z","message":{"role":"assistant","content":[{"type":"text","text":"working"}],"stop_reason":"end_turn"}}"#,
        ],
    );

    let snapshot = ClaudeArtifactObserver::with_home(fixture.home_directory(), "/tmp/claude-proof")
        .snapshot()
        .expect("snapshot recovers fixture");
    let turn = snapshot.recovered_turn();

    assert!(turn.contains_text("FINAL_MARKER"));
    assert!(!turn.has_completed_marked_turn("PROMPT_MARKER", "FINAL_MARKER"));
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

#[test]
fn claude_artifact_event_watcher_wakes_on_project_jsonl_create_and_append() {
    let fixture = ClaudeFixture::new("/tmp/claude-proof");
    fixture.create_artifact_directories();
    let mut watcher =
        ClaudeArtifactObserver::with_home(fixture.home_directory(), "/tmp/claude-proof")
            .event_watcher()
            .expect("event watcher starts on existing artifact directories");
    let project_jsonl = fixture.project_jsonl_path("session-alpha.jsonl");

    fixture.append_project_jsonl(
        &project_jsonl,
        r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER"}]}}"#,
    );
    let prompt_wake = watcher
        .wait_for_next_snapshot(Duration::from_secs(5))
        .expect("project jsonl create event wakes observer");

    assert!(prompt_wake.used_file_event());
    assert!(
        prompt_wake
            .snapshot()
            .recovered_turn()
            .contains_text("PROMPT_MARKER")
    );

    fixture.append_project_jsonl(
        &project_jsonl,
        r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:02Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"TOOL_MARKER"}]}}"#,
    );
    fixture.append_project_jsonl(
        &project_jsonl,
        r#"{"type":"assistant","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:04Z","message":{"role":"assistant","model":"claude-haiku","content":[{"type":"text","text":"FINAL_MARKER"}],"stop_reason":"end_turn"}}"#,
    );
    let final_wake = watcher
        .wait_for_next_snapshot(Duration::from_secs(5))
        .expect("project jsonl append event wakes observer");
    let turn = final_wake.snapshot().recovered_turn();

    assert!(final_wake.used_file_event());
    assert!(turn.contains_text("TOOL_MARKER"));
    assert!(turn.has_completed_marked_turn("PROMPT_MARKER", "FINAL_MARKER"));
}

#[test]
fn claude_artifact_event_watcher_wakes_on_session_file_create() {
    let fixture = ClaudeFixture::new("/tmp/claude-proof");
    fixture.create_artifact_directories();
    let mut watcher =
        ClaudeArtifactObserver::with_home(fixture.home_directory(), "/tmp/claude-proof")
            .event_watcher()
            .expect("event watcher starts on existing session directory");

    fixture.write_session_file(
        "session-alpha.json",
        r#"{"sessionId":"session-alpha","cwd":"/tmp/claude-proof","permissionMode":"bypassPermissions"}"#,
    );
    let wake = watcher
        .wait_for_next_snapshot(Duration::from_secs(5))
        .expect("session file create event wakes observer");
    let turn = wake.snapshot().recovered_turn();

    assert!(wake.used_file_event());
    assert_eq!(wake.snapshot().session_identifier(), Some("session-alpha"));
    assert_eq!(turn.permission_modes(), &["bypassPermissions".to_string()]);
}

#[test]
fn claude_artifact_wait_report_marks_file_event_strategy() {
    let fixture = ClaudeFixture::new("/tmp/claude-proof");
    fixture.create_artifact_directories();
    let mut watcher =
        ClaudeArtifactObserver::with_home(fixture.home_directory(), "/tmp/claude-proof")
            .event_watcher()
            .expect("event watcher starts");
    let project_jsonl = fixture.project_jsonl_path("session-alpha.jsonl");

    fixture.append_project_jsonl(
        &project_jsonl,
        r#"{"type":"user","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:00Z","message":{"role":"user","content":[{"type":"text","text":"PROMPT_MARKER"}]}}"#,
    );
    let writer_path = project_jsonl.clone();
    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        let mut file = OpenOptions::new()
            .append(true)
            .open(writer_path)
            .expect("open project jsonl");
        writeln!(
            file,
            r#"{{"type":"assistant","cwd":"/tmp/claude-proof","sessionId":"session-alpha","timestamp":"2026-06-28T10:00:04Z","message":{{"role":"assistant","model":"claude-haiku","content":[{{"type":"text","text":"FINAL_MARKER"}}],"stop_reason":"end_turn"}}}}"#
        )
        .expect("append final marker");
        file.flush().expect("flush final marker");
    });
    let report = watcher
        .wait_for_markers("PROMPT_MARKER", "FINAL_MARKER", Duration::from_secs(5))
        .expect("event watcher recovers completed markers");
    writer.join().expect("writer thread finishes");

    assert_eq!(report.strategy(), ClaudeObservationStrategy::FileEvents);
    assert!(report.file_event_count() > 0);
    assert_eq!(report.polling_fallback_count(), 0);
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

    fn create_artifact_directories(&self) {
        fs::create_dir_all(self.project_directory()).expect("create project directory");
        fs::create_dir_all(self.session_directory()).expect("create session directory");
    }

    fn project_directory(&self) -> std::path::PathBuf {
        self.home
            .path()
            .join(".claude")
            .join("projects")
            .join(self.current_working_directory.replace('/', "-"))
    }

    fn session_directory(&self) -> std::path::PathBuf {
        self.home.path().join(".claude").join("sessions")
    }

    fn project_jsonl_path(&self, file_name: &str) -> std::path::PathBuf {
        self.project_directory().join(file_name)
    }

    fn write_project_jsonl(&self, file_name: &str, lines: &[&str]) {
        let directory = self.project_directory();
        fs::create_dir_all(&directory).expect("create project directory");
        fs::write(directory.join(file_name), lines.join("\n")).expect("write project jsonl");
    }

    fn append_project_jsonl(&self, path: &std::path::Path, line: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open project jsonl");
        writeln!(file, "{line}").expect("append project jsonl");
        file.flush().expect("flush project jsonl");
    }

    fn write_session_file(&self, file_name: &str, text: &str) {
        let directory = self.session_directory();
        fs::create_dir_all(&directory).expect("create session directory");
        fs::write(directory.join(file_name), text).expect("write session json");
    }
}
