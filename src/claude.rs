use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::thread;
use std::time::{Duration, Instant};

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde_json::{Value, json};

use crate::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeArtifactObserver {
    artifact_root: PathBuf,
    current_working_directory: PathBuf,
    session_identifier: Option<String>,
    poll_interval: Duration,
    event_reconciliation_interval: Duration,
}

impl ClaudeArtifactObserver {
    pub fn new(current_working_directory: impl Into<PathBuf>) -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        Self::with_home(home, current_working_directory)
    }

    pub fn with_home(
        home_directory: impl Into<PathBuf>,
        current_working_directory: impl Into<PathBuf>,
    ) -> Self {
        Self {
            artifact_root: home_directory.into().join(".claude"),
            current_working_directory: current_working_directory.into(),
            session_identifier: None,
            poll_interval: Duration::from_millis(250),
            event_reconciliation_interval: Duration::from_secs(5),
        }
    }

    pub fn with_session_identifier(mut self, session_identifier: impl Into<String>) -> Self {
        self.session_identifier = Some(session_identifier.into());
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_event_reconciliation_interval(
        mut self,
        event_reconciliation_interval: Duration,
    ) -> Self {
        self.event_reconciliation_interval = event_reconciliation_interval;
        self
    }

    pub fn artifact_root(&self) -> &Path {
        &self.artifact_root
    }

    pub fn current_working_directory(&self) -> &Path {
        &self.current_working_directory
    }

    pub fn session_identifier(&self) -> Option<&str> {
        self.session_identifier.as_deref()
    }

    pub fn snapshot(&self) -> Result<ClaudeArtifactSnapshot> {
        ClaudeArtifactSnapshot::from_observer(self)
    }

    pub fn event_watcher(&self) -> Result<ClaudeArtifactEventWatcher> {
        ClaudeArtifactEventWatcher::from_observer(self.clone())
    }

    pub fn wait_for_markers(
        &self,
        prompt_marker: &str,
        final_marker: &str,
        timeout: Duration,
    ) -> Result<ClaudeArtifactSnapshot> {
        Ok(self
            .wait_for_markers_with_report(prompt_marker, final_marker, timeout)?
            .into_snapshot())
    }

    pub fn wait_for_markers_with_report(
        &self,
        prompt_marker: &str,
        final_marker: &str,
        timeout: Duration,
    ) -> Result<ClaudeArtifactWaitReport> {
        match self.event_watcher() {
            Ok(mut watcher) => watcher.wait_for_markers(prompt_marker, final_marker, timeout),
            Err(_) => self.wait_for_markers_by_polling(prompt_marker, final_marker, timeout),
        }
    }

    fn wait_for_markers_by_polling(
        &self,
        prompt_marker: &str,
        final_marker: &str,
        timeout: Duration,
    ) -> Result<ClaudeArtifactWaitReport> {
        let deadline = Instant::now() + timeout;
        let mut fallback_count = 0_u64;
        loop {
            let snapshot = self.snapshot()?;
            if snapshot
                .recovered_turn()
                .has_completed_marked_turn(prompt_marker, final_marker)
            {
                return Ok(ClaudeArtifactWaitReport::new(
                    snapshot,
                    ClaudeObservationStrategy::PollingFallback,
                    0,
                    fallback_count,
                ));
            }
            if Instant::now() >= deadline {
                return Err(Error::ClaudeObservationTimeout {
                    current_working_directory: self.current_working_directory.clone(),
                });
            }
            fallback_count = fallback_count.saturating_add(1);
            thread::sleep(self.poll_interval);
        }
    }

    fn project_jsonl_paths(&self) -> Result<Vec<PathBuf>> {
        let projects_root = self.artifact_root.join("projects");
        let encoded_directory = projects_root.join(
            ClaudeProjectDirectoryName::from_path(&self.current_working_directory).into_string(),
        );
        let mut paths = Vec::new();
        if encoded_directory.is_dir() {
            ClaudeDirectory::new(encoded_directory).jsonl_paths(&mut paths)?;
        } else if projects_root.is_dir() {
            ClaudeDirectory::new(projects_root).descendant_jsonl_paths(&mut paths)?;
        }
        paths.sort();
        paths.dedup();
        Ok(paths)
    }

    fn session_file_paths(&self) -> Result<Vec<PathBuf>> {
        let sessions_root = self.artifact_root.join("sessions");
        let mut paths = Vec::new();
        if sessions_root.is_dir() {
            ClaudeDirectory::new(sessions_root).json_paths(&mut paths)?;
        }
        paths.sort();
        Ok(paths)
    }

    fn watch_roots(&self) -> Vec<PathBuf> {
        let projects_root = self.artifact_root.join("projects");
        let encoded_directory = projects_root.join(
            ClaudeProjectDirectoryName::from_path(&self.current_working_directory).into_string(),
        );
        let sessions_root = self.artifact_root.join("sessions");
        let mut roots = Vec::new();
        if encoded_directory.is_dir() {
            roots.push(encoded_directory);
        } else if projects_root.is_dir() {
            roots.push(projects_root);
        }
        if sessions_root.is_dir() {
            roots.push(sessions_root);
        }
        roots.sort();
        roots.dedup();
        roots
    }

    fn record_matches(&self, record: &ClaudeJsonLine) -> bool {
        let requested_directory = self.current_working_directory.to_string_lossy();
        let directory_matches = record
            .current_working_directory()
            .is_some_and(|directory| directory == requested_directory);
        let session_matches =
            self.session_identifier
                .as_deref()
                .is_some_and(|session_identifier| {
                    record
                        .session_identifier()
                        .is_some_and(|record_identifier| record_identifier == session_identifier)
                });
        if self.session_identifier.is_some() {
            session_matches || (record.session_identifier().is_none() && directory_matches)
        } else {
            directory_matches || record.current_working_directory().is_none()
        }
    }

    fn session_artifact_matches(&self, artifact: &ClaudeSessionArtifact) -> bool {
        let requested_directory = self.current_working_directory.to_string_lossy();
        let directory_matches = artifact
            .current_working_directory()
            .is_some_and(|directory| directory == requested_directory);
        let session_matches =
            self.session_identifier
                .as_deref()
                .is_some_and(|session_identifier| {
                    artifact
                        .session_identifier()
                        .is_some_and(|record_identifier| record_identifier == session_identifier)
                });
        directory_matches || session_matches
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClaudeObservationStrategy {
    FileEvents,
    PollingFallback,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClaudeArtifactWaitReport {
    snapshot: ClaudeArtifactSnapshot,
    strategy: ClaudeObservationStrategy,
    file_event_count: u64,
    polling_fallback_count: u64,
}

impl ClaudeArtifactWaitReport {
    fn new(
        snapshot: ClaudeArtifactSnapshot,
        strategy: ClaudeObservationStrategy,
        file_event_count: u64,
        polling_fallback_count: u64,
    ) -> Self {
        Self {
            snapshot,
            strategy,
            file_event_count,
            polling_fallback_count,
        }
    }

    pub fn snapshot(&self) -> &ClaudeArtifactSnapshot {
        &self.snapshot
    }

    pub fn into_snapshot(self) -> ClaudeArtifactSnapshot {
        self.snapshot
    }

    pub fn strategy(&self) -> ClaudeObservationStrategy {
        self.strategy
    }

    pub fn file_event_count(&self) -> u64 {
        self.file_event_count
    }

    pub fn polling_fallback_count(&self) -> u64 {
        self.polling_fallback_count
    }

    pub fn summary_json(
        &self,
        prompt_marker: &str,
        final_marker: &str,
        tool_marker: &str,
    ) -> Value {
        let mut summary = self
            .snapshot
            .summary_json(prompt_marker, final_marker, tool_marker);
        if let Value::Object(map) = &mut summary {
            map.insert(
                "observation_strategy".to_string(),
                json!(match self.strategy {
                    ClaudeObservationStrategy::FileEvents => "file_events",
                    ClaudeObservationStrategy::PollingFallback => "polling_fallback",
                }),
            );
            map.insert("file_event_count".to_string(), json!(self.file_event_count));
            map.insert(
                "polling_fallback_count".to_string(),
                json!(self.polling_fallback_count),
            );
        }
        summary
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClaudeArtifactWake {
    FileEvent {
        snapshot: ClaudeArtifactSnapshot,
        event_paths: Vec<PathBuf>,
    },
    Reconciliation {
        snapshot: ClaudeArtifactSnapshot,
    },
}

impl ClaudeArtifactWake {
    pub fn snapshot(&self) -> &ClaudeArtifactSnapshot {
        match self {
            Self::FileEvent { snapshot, .. } | Self::Reconciliation { snapshot } => snapshot,
        }
    }

    pub fn event_paths(&self) -> &[PathBuf] {
        match self {
            Self::FileEvent { event_paths, .. } => event_paths,
            Self::Reconciliation { .. } => &[],
        }
    }

    pub fn used_file_event(&self) -> bool {
        matches!(self, Self::FileEvent { .. })
    }
}

#[derive(Debug)]
pub struct ClaudeArtifactEventWatcher {
    observer: ClaudeArtifactObserver,
    event_receiver: Receiver<notify::Result<Event>>,
    _event_sender: Sender<notify::Result<Event>>,
    watcher: RecommendedWatcher,
    watched_roots: BTreeSet<PathBuf>,
}

impl ClaudeArtifactEventWatcher {
    fn from_observer(observer: ClaudeArtifactObserver) -> Result<Self> {
        let (event_sender, event_receiver) = channel();
        let callback_sender = event_sender.clone();
        let watcher = notify::recommended_watcher(move |event| {
            let _ = callback_sender.send(event);
        })
        .map_err(|error| ClaudeNotifyError::from(error).into_error())?;
        let mut watcher = Self {
            observer,
            event_receiver,
            _event_sender: event_sender,
            watcher,
            watched_roots: BTreeSet::new(),
        };
        watcher.refresh_watched_roots()?;
        if watcher.watched_roots.is_empty() {
            return Err(Error::ClaudeArtifactWatcher {
                message: "no existing Claude artifact directories to watch".to_string(),
            });
        }
        Ok(watcher)
    }

    pub fn watched_roots(&self) -> Vec<PathBuf> {
        self.watched_roots.iter().cloned().collect()
    }

    pub fn snapshot(&self) -> Result<ClaudeArtifactSnapshot> {
        self.observer.snapshot()
    }

    pub fn wait_for_next_snapshot(&mut self, timeout: Duration) -> Result<ClaudeArtifactWake> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::ClaudeObservationTimeout {
                    current_working_directory: self.observer.current_working_directory.clone(),
                });
            }
            match self.event_receiver.recv_timeout(remaining) {
                Ok(Ok(event)) => {
                    self.refresh_watched_roots()?;
                    return Ok(ClaudeArtifactWake::FileEvent {
                        snapshot: self.observer.snapshot()?,
                        event_paths: event.paths,
                    });
                }
                Ok(Err(error)) => return Err(ClaudeNotifyError::from(error).into_error()),
                Err(RecvTimeoutError::Timeout) => {
                    return Err(Error::ClaudeObservationTimeout {
                        current_working_directory: self.observer.current_working_directory.clone(),
                    });
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(Error::ClaudeArtifactWatcher {
                        message: "file event channel disconnected".to_string(),
                    });
                }
            }
        }
    }

    pub fn wait_for_markers(
        &mut self,
        prompt_marker: &str,
        final_marker: &str,
        timeout: Duration,
    ) -> Result<ClaudeArtifactWaitReport> {
        let deadline = Instant::now() + timeout;
        let mut file_event_count = 0_u64;
        let mut polling_fallback_count = 0_u64;
        loop {
            let snapshot = self.observer.snapshot()?;
            if snapshot
                .recovered_turn()
                .has_completed_marked_turn(prompt_marker, final_marker)
            {
                return Ok(ClaudeArtifactWaitReport::new(
                    snapshot,
                    ClaudeObservationStrategy::FileEvents,
                    file_event_count,
                    polling_fallback_count,
                ));
            }
            if Instant::now() >= deadline {
                return Err(Error::ClaudeObservationTimeout {
                    current_working_directory: self.observer.current_working_directory.clone(),
                });
            }
            match self.wait_for_change_before(deadline)? {
                ClaudeArtifactChange::FileEvent => {
                    file_event_count = file_event_count.saturating_add(1);
                }
                ClaudeArtifactChange::Reconciliation => {
                    polling_fallback_count = polling_fallback_count.saturating_add(1);
                }
            }
        }
    }

    fn wait_for_change_before(&mut self, deadline: Instant) -> Result<ClaudeArtifactChange> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(Error::ClaudeObservationTimeout {
                current_working_directory: self.observer.current_working_directory.clone(),
            });
        }
        let wait_duration = remaining.min(self.observer.event_reconciliation_interval);
        match self.event_receiver.recv_timeout(wait_duration) {
            Ok(Ok(_event)) => {
                self.refresh_watched_roots()?;
                Ok(ClaudeArtifactChange::FileEvent)
            }
            Ok(Err(error)) => Err(ClaudeNotifyError::from(error).into_error()),
            Err(RecvTimeoutError::Timeout) => Ok(ClaudeArtifactChange::Reconciliation),
            Err(RecvTimeoutError::Disconnected) => Err(Error::ClaudeArtifactWatcher {
                message: "file event channel disconnected".to_string(),
            }),
        }
    }

    fn refresh_watched_roots(&mut self) -> Result<()> {
        for root in self.observer.watch_roots() {
            if self.watched_roots.contains(&root) {
                continue;
            }
            self.watcher
                .watch(&root, RecursiveMode::Recursive)
                .map_err(|error| ClaudeNotifyError::from(error).into_error())?;
            self.watched_roots.insert(root);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClaudeArtifactChange {
    FileEvent,
    Reconciliation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClaudeNotifyError {
    message: String,
}

impl ClaudeNotifyError {
    fn into_error(self) -> Error {
        Error::ClaudeArtifactWatcher {
            message: self.message,
        }
    }
}

impl From<notify::Error> for ClaudeNotifyError {
    fn from(value: notify::Error) -> Self {
        Self {
            message: value.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClaudeProjectDirectoryName {
    value: String,
}

impl ClaudeProjectDirectoryName {
    fn from_path(path: &Path) -> Self {
        Self {
            value: path.to_string_lossy().replace('/', "-"),
        }
    }

    fn into_string(self) -> String {
        self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ClaudeDirectory {
    path: PathBuf,
}

impl ClaudeDirectory {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    fn jsonl_paths(&self, paths: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension() == Some(OsStr::new("jsonl")) {
                paths.push(path);
            }
        }
        Ok(())
    }

    fn json_paths(&self, paths: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension() == Some(OsStr::new("json")) {
                paths.push(path);
            }
        }
        Ok(())
    }

    fn descendant_jsonl_paths(&self, paths: &mut Vec<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                ClaudeDirectory::new(path).descendant_jsonl_paths(paths)?;
            } else if path.extension() == Some(OsStr::new("jsonl")) {
                paths.push(path);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ClaudeJsonLine {
    source_path: PathBuf,
    line_number: usize,
    value: Value,
}

impl ClaudeJsonLine {
    fn from_text(source_path: PathBuf, line_number: usize, text: &str) -> Result<Self> {
        Ok(Self {
            source_path,
            line_number,
            value: serde_json::from_str(text)?,
        })
    }

    fn source_path(&self) -> &Path {
        &self.source_path
    }

    fn line_number(&self) -> usize {
        self.line_number
    }

    fn record_type(&self) -> Option<&str> {
        self.value.get("type").and_then(Value::as_str)
    }

    fn timestamp(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["timestamp", "createdAt"])
    }

    fn session_identifier(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["sessionId", "session_id"])
    }

    fn current_working_directory(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["cwd", "currentWorkingDirectory"])
    }

    fn model(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["model"])
    }

    fn stop_reason(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["stop_reason", "stopReason"])
    }

    fn permission_mode(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["permissionMode", "permission_mode"])
    }

    fn text_fragments(&self) -> Vec<String> {
        JsonLookup::new(&self.value).strings_for_keys(&["text", "content", "stdout"])
    }

    fn tool_calls(&self) -> Vec<ClaudeToolCall> {
        let mut calls = Vec::new();
        JsonLookup::new(&self.value).collect_tool_calls(self, &mut calls);
        calls
    }

    fn tool_results(&self) -> Vec<ClaudeToolResult> {
        let mut results = Vec::new();
        JsonLookup::new(&self.value).collect_tool_results(self, &mut results);
        results
    }

    fn file_edits(&self) -> Vec<ClaudeFileEdit> {
        self.tool_calls()
            .into_iter()
            .filter_map(|call| call.file_edit())
            .collect()
    }

    fn status_transitions(&self) -> Vec<ClaudeStatusTransition> {
        let mut transitions = Vec::new();
        if self.record_type() == Some("user") {
            transitions.push(ClaudeStatusTransition::new(
                "user_prompt_observed",
                self.timestamp().map(ToOwned::to_owned),
                self.source_path.clone(),
                self.line_number,
            ));
        }
        if self.record_type() == Some("assistant") {
            transitions.push(ClaudeStatusTransition::new(
                "assistant_message_observed",
                self.timestamp().map(ToOwned::to_owned),
                self.source_path.clone(),
                self.line_number,
            ));
        }
        if !self.tool_calls().is_empty() {
            transitions.push(ClaudeStatusTransition::new(
                "tool_call_observed",
                self.timestamp().map(ToOwned::to_owned),
                self.source_path.clone(),
                self.line_number,
            ));
        }
        if !self.tool_results().is_empty() {
            transitions.push(ClaudeStatusTransition::new(
                "tool_result_observed",
                self.timestamp().map(ToOwned::to_owned),
                self.source_path.clone(),
                self.line_number,
            ));
        }
        if self.permission_mode().is_some()
            || JsonLookup::new(&self.value).contains_key_part("permission")
        {
            transitions.push(ClaudeStatusTransition::new(
                "permission_observed",
                self.timestamp().map(ToOwned::to_owned),
                self.source_path.clone(),
                self.line_number,
            ));
        }
        if self.stop_reason() == Some("end_turn") {
            transitions.push(ClaudeStatusTransition::new(
                "end_turn_observed",
                self.timestamp().map(ToOwned::to_owned),
                self.source_path.clone(),
                self.line_number,
            ));
        }
        transitions
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClaudeSessionArtifact {
    source_path: PathBuf,
    value: Value,
}

impl ClaudeSessionArtifact {
    fn from_path(path: PathBuf) -> Result<Self> {
        let value = serde_json::from_slice(&fs::read(&path)?)?;
        Ok(Self {
            source_path: path,
            value,
        })
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub fn session_identifier(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["sessionId", "session_id"])
    }

    pub fn current_working_directory(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["cwd", "currentWorkingDirectory"])
    }

    pub fn permission_mode(&self) -> Option<&str> {
        JsonLookup::new(&self.value).first_string_for_keys(&["permissionMode", "permission_mode"])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeToolCall {
    identifier: Option<String>,
    name: String,
    input: Value,
    timestamp: Option<String>,
    source_path: PathBuf,
    line_number: usize,
}

impl ClaudeToolCall {
    fn new(
        record: &ClaudeJsonLine,
        identifier: Option<String>,
        name: String,
        input: Value,
    ) -> Self {
        Self {
            identifier,
            name,
            input,
            timestamp: record.timestamp().map(ToOwned::to_owned),
            source_path: record.source_path().to_path_buf(),
            line_number: record.line_number(),
        }
    }

    pub fn identifier(&self) -> Option<&str> {
        self.identifier.as_deref()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn input(&self) -> &Value {
        &self.input
    }

    pub fn timestamp(&self) -> Option<&str> {
        self.timestamp.as_deref()
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub fn line_number(&self) -> usize {
        self.line_number
    }

    fn file_edit(&self) -> Option<ClaudeFileEdit> {
        let operation = match self.name.as_str() {
            "Edit" | "MultiEdit" | "Write" | "NotebookEdit" => Some(self.name.clone()),
            _ => None,
        }?;
        let path = JsonLookup::new(&self.input)
            .first_string_for_keys(&["file_path", "path"])
            .map(ToOwned::to_owned)?;
        Some(ClaudeFileEdit {
            operation,
            path,
            tool_identifier: self.identifier.clone(),
            timestamp: self.timestamp.clone(),
            source_path: self.source_path.clone(),
            line_number: self.line_number,
        })
    }

    fn summary_json(&self) -> Value {
        json!({
            "identifier": self.identifier,
            "name": self.name,
            "timestamp": self.timestamp,
            "source_path": self.source_path,
            "line_number": self.line_number,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeToolResult {
    tool_use_identifier: Option<String>,
    content: Vec<String>,
    stdout: Vec<String>,
    is_error: Option<bool>,
    timestamp: Option<String>,
    source_path: PathBuf,
    line_number: usize,
}

impl ClaudeToolResult {
    fn new(
        record: &ClaudeJsonLine,
        tool_use_identifier: Option<String>,
        content: Vec<String>,
        stdout: Vec<String>,
        is_error: Option<bool>,
    ) -> Self {
        Self {
            tool_use_identifier,
            content,
            stdout,
            is_error,
            timestamp: record.timestamp().map(ToOwned::to_owned),
            source_path: record.source_path().to_path_buf(),
            line_number: record.line_number(),
        }
    }

    pub fn tool_use_identifier(&self) -> Option<&str> {
        self.tool_use_identifier.as_deref()
    }

    pub fn content(&self) -> &[String] {
        &self.content
    }

    pub fn stdout(&self) -> &[String] {
        &self.stdout
    }

    pub fn is_error(&self) -> Option<bool> {
        self.is_error
    }

    pub fn timestamp(&self) -> Option<&str> {
        self.timestamp.as_deref()
    }

    pub fn contains_text(&self, needle: &str) -> bool {
        self.content
            .iter()
            .chain(self.stdout.iter())
            .any(|fragment| fragment.contains(needle))
    }

    fn summary_json(&self) -> Value {
        json!({
            "tool_use_identifier": self.tool_use_identifier,
            "content_count": self.content.len(),
            "stdout_count": self.stdout.len(),
            "is_error": self.is_error,
            "timestamp": self.timestamp,
            "source_path": self.source_path,
            "line_number": self.line_number,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeFileEdit {
    operation: String,
    path: String,
    tool_identifier: Option<String>,
    timestamp: Option<String>,
    source_path: PathBuf,
    line_number: usize,
}

impl ClaudeFileEdit {
    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn tool_identifier(&self) -> Option<&str> {
        self.tool_identifier.as_deref()
    }

    pub fn timestamp(&self) -> Option<&str> {
        self.timestamp.as_deref()
    }

    fn summary_json(&self) -> Value {
        json!({
            "operation": self.operation,
            "path": self.path,
            "tool_identifier": self.tool_identifier,
            "timestamp": self.timestamp,
            "source_path": self.source_path,
            "line_number": self.line_number,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeStatusTransition {
    kind: String,
    timestamp: Option<String>,
    source_path: PathBuf,
    line_number: usize,
}

impl ClaudeStatusTransition {
    fn new(
        kind: impl Into<String>,
        timestamp: Option<String>,
        source_path: PathBuf,
        line_number: usize,
    ) -> Self {
        Self {
            kind: kind.into(),
            timestamp,
            source_path,
            line_number,
        }
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn timestamp(&self) -> Option<&str> {
        self.timestamp.as_deref()
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub fn line_number(&self) -> usize {
        self.line_number
    }

    fn summary_json(&self) -> Value {
        json!({
            "kind": self.kind,
            "timestamp": self.timestamp,
            "source_path": self.source_path,
            "line_number": self.line_number,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClaudeArtifactSnapshot {
    current_working_directory: PathBuf,
    session_identifier: Option<String>,
    project_jsonl_paths: Vec<PathBuf>,
    session_artifacts: Vec<ClaudeSessionArtifact>,
    recovered_turn: ClaudeRecoveredTurn,
}

impl ClaudeArtifactSnapshot {
    fn from_observer(observer: &ClaudeArtifactObserver) -> Result<Self> {
        let mut records = Vec::new();
        for path in observer.project_jsonl_paths()? {
            let file = File::open(&path)?;
            for (line_index, line) in BufReader::new(file).lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let record = match ClaudeJsonLine::from_text(path.clone(), line_index + 1, &line) {
                    Ok(record) => record,
                    Err(Error::Json(_)) => continue,
                    Err(error) => return Err(error),
                };
                if observer.record_matches(&record) {
                    records.push(record);
                }
            }
        }
        let mut session_artifacts = Vec::new();
        for path in observer.session_file_paths()? {
            let artifact = ClaudeSessionArtifact::from_path(path)?;
            if observer.session_artifact_matches(&artifact) {
                session_artifacts.push(artifact);
            }
        }
        let project_jsonl_paths = records
            .iter()
            .map(|record| record.source_path().to_path_buf())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let recovered_turn = ClaudeRecoveredTurn::from_records(&records, &session_artifacts);
        Ok(Self {
            current_working_directory: observer.current_working_directory.clone(),
            session_identifier: observer
                .session_identifier
                .clone()
                .or_else(|| recovered_turn.session_identifier.clone()),
            project_jsonl_paths,
            session_artifacts,
            recovered_turn,
        })
    }

    pub fn current_working_directory(&self) -> &Path {
        &self.current_working_directory
    }

    pub fn session_identifier(&self) -> Option<&str> {
        self.session_identifier.as_deref()
    }

    pub fn project_jsonl_paths(&self) -> &[PathBuf] {
        &self.project_jsonl_paths
    }

    pub fn session_artifacts(&self) -> &[ClaudeSessionArtifact] {
        &self.session_artifacts
    }

    pub fn recovered_turn(&self) -> &ClaudeRecoveredTurn {
        &self.recovered_turn
    }

    pub fn summary_json(
        &self,
        prompt_marker: &str,
        final_marker: &str,
        tool_marker: &str,
    ) -> Value {
        self.recovered_turn
            .summary_json(self, prompt_marker, final_marker, tool_marker)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeRecoveredTurn {
    session_identifier: Option<String>,
    model: Option<String>,
    current_working_directory: Option<String>,
    permission_modes: Vec<String>,
    stop_reasons: Vec<String>,
    timestamps: Vec<String>,
    text_fragments: Vec<String>,
    tool_calls: Vec<ClaudeToolCall>,
    tool_results: Vec<ClaudeToolResult>,
    file_edits: Vec<ClaudeFileEdit>,
    status_transitions: Vec<ClaudeStatusTransition>,
}

impl ClaudeRecoveredTurn {
    fn from_records(
        records: &[ClaudeJsonLine],
        session_artifacts: &[ClaudeSessionArtifact],
    ) -> Self {
        let mut turn = Self {
            session_identifier: None,
            model: None,
            current_working_directory: None,
            permission_modes: Vec::new(),
            stop_reasons: Vec::new(),
            timestamps: Vec::new(),
            text_fragments: Vec::new(),
            tool_calls: Vec::new(),
            tool_results: Vec::new(),
            file_edits: Vec::new(),
            status_transitions: Vec::new(),
        };
        for record in records {
            turn.observe_record(record);
        }
        for artifact in session_artifacts {
            turn.observe_session_artifact(artifact);
        }
        turn.permission_modes.sort();
        turn.permission_modes.dedup();
        turn.stop_reasons.sort();
        turn.stop_reasons.dedup();
        turn.timestamps.sort();
        turn.timestamps.dedup();
        turn
    }

    pub fn session_identifier(&self) -> Option<&str> {
        self.session_identifier.as_deref()
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    pub fn current_working_directory(&self) -> Option<&str> {
        self.current_working_directory.as_deref()
    }

    pub fn permission_modes(&self) -> &[String] {
        &self.permission_modes
    }

    pub fn stop_reasons(&self) -> &[String] {
        &self.stop_reasons
    }

    pub fn timestamps(&self) -> &[String] {
        &self.timestamps
    }

    pub fn tool_calls(&self) -> &[ClaudeToolCall] {
        &self.tool_calls
    }

    pub fn tool_results(&self) -> &[ClaudeToolResult] {
        &self.tool_results
    }

    pub fn file_edits(&self) -> &[ClaudeFileEdit] {
        &self.file_edits
    }

    pub fn status_transitions(&self) -> &[ClaudeStatusTransition] {
        &self.status_transitions
    }

    pub fn contains_text(&self, needle: &str) -> bool {
        self.text_fragments
            .iter()
            .any(|fragment| fragment.contains(needle))
            || self
                .tool_results
                .iter()
                .any(|result| result.contains_text(needle))
    }

    pub fn has_stop_reason_end_turn(&self) -> bool {
        self.stop_reasons.iter().any(|reason| reason == "end_turn")
    }

    pub fn has_completed_marked_turn(&self, prompt_marker: &str, final_marker: &str) -> bool {
        self.contains_text(prompt_marker)
            && self.contains_text(final_marker)
            && self.has_stop_reason_end_turn()
    }

    fn observe_record(&mut self, record: &ClaudeJsonLine) {
        if self.session_identifier.is_none() {
            self.session_identifier = record.session_identifier().map(ToOwned::to_owned);
        }
        if self.model.is_none() {
            self.model = record.model().map(ToOwned::to_owned);
        }
        if self.current_working_directory.is_none() {
            self.current_working_directory =
                record.current_working_directory().map(ToOwned::to_owned);
        }
        if let Some(permission_mode) = record.permission_mode() {
            self.permission_modes.push(permission_mode.to_string());
        }
        if let Some(stop_reason) = record.stop_reason() {
            self.stop_reasons.push(stop_reason.to_string());
        }
        if let Some(timestamp) = record.timestamp() {
            self.timestamps.push(timestamp.to_string());
        }
        self.text_fragments.extend(record.text_fragments());
        self.tool_calls.extend(record.tool_calls());
        self.tool_results.extend(record.tool_results());
        self.file_edits.extend(record.file_edits());
        self.status_transitions.extend(record.status_transitions());
    }

    fn observe_session_artifact(&mut self, artifact: &ClaudeSessionArtifact) {
        if self.session_identifier.is_none() {
            self.session_identifier = artifact.session_identifier().map(ToOwned::to_owned);
        }
        if self.current_working_directory.is_none() {
            self.current_working_directory =
                artifact.current_working_directory().map(ToOwned::to_owned);
        }
        if let Some(permission_mode) = artifact.permission_mode() {
            self.permission_modes.push(permission_mode.to_string());
        }
        self.status_transitions.push(ClaudeStatusTransition::new(
            "session_file_observed",
            None,
            artifact.source_path().to_path_buf(),
            0,
        ));
    }

    fn summary_json(
        &self,
        snapshot: &ClaudeArtifactSnapshot,
        prompt_marker: &str,
        final_marker: &str,
        tool_marker: &str,
    ) -> Value {
        json!({
            "current_working_directory": snapshot.current_working_directory(),
            "session_identifier": snapshot.session_identifier(),
            "project_jsonl_paths": snapshot.project_jsonl_paths(),
            "session_file_paths": snapshot.session_artifacts().iter().map(|artifact| artifact.source_path()).collect::<Vec<_>>(),
            "model": self.model(),
            "permission_modes": self.permission_modes(),
            "stop_reason_end_turn": self.has_stop_reason_end_turn(),
            "user_prompt_marker_seen": self.contains_text(prompt_marker),
            "assistant_final_marker_seen": self.contains_text(final_marker),
            "tool_marker_seen": self.contains_text(tool_marker),
            "tool_calls": self.tool_calls().iter().map(ClaudeToolCall::summary_json).collect::<Vec<_>>(),
            "tool_results": self.tool_results().iter().map(ClaudeToolResult::summary_json).collect::<Vec<_>>(),
            "file_edits": self.file_edits().iter().map(ClaudeFileEdit::summary_json).collect::<Vec<_>>(),
            "status_transitions": self.status_transitions().iter().map(ClaudeStatusTransition::summary_json).collect::<Vec<_>>(),
            "timestamp_count": self.timestamps().len(),
        })
    }
}

#[derive(Clone, Debug)]
struct JsonLookup<'a> {
    value: &'a Value,
}

impl<'a> JsonLookup<'a> {
    fn new(value: &'a Value) -> Self {
        Self { value }
    }

    fn first_string_for_keys(&self, keys: &[&str]) -> Option<&'a str> {
        self.first_string_in_value(self.value, keys)
    }

    fn strings_for_keys(&self, keys: &[&str]) -> Vec<String> {
        let mut strings = Vec::new();
        self.collect_strings_for_keys(self.value, keys, &mut strings);
        strings
    }

    fn contains_key_part(&self, needle: &str) -> bool {
        self.value_contains_key_part(self.value, needle)
    }

    fn collect_tool_calls(&self, record: &ClaudeJsonLine, calls: &mut Vec<ClaudeToolCall>) {
        self.collect_tool_calls_from_value(record, self.value, calls);
    }

    fn collect_tool_results(&self, record: &ClaudeJsonLine, results: &mut Vec<ClaudeToolResult>) {
        self.collect_tool_results_from_value(record, self.value, results);
    }

    fn first_string_in_value(&self, value: &'a Value, keys: &[&str]) -> Option<&'a str> {
        match value {
            Value::Object(map) => {
                for key in keys {
                    if let Some(string) = map.get(*key).and_then(Value::as_str) {
                        return Some(string);
                    }
                }
                for child in map.values() {
                    if let Some(string) = self.first_string_in_value(child, keys) {
                        return Some(string);
                    }
                }
                None
            }
            Value::Array(array) => array
                .iter()
                .find_map(|child| self.first_string_in_value(child, keys)),
            _ => None,
        }
    }

    fn collect_strings_for_keys(&self, value: &Value, keys: &[&str], strings: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                for (key, child) in map {
                    if keys.iter().any(|candidate| candidate == key) {
                        self.collect_string_values(child, strings);
                    } else {
                        self.collect_strings_for_keys(child, keys, strings);
                    }
                }
            }
            Value::Array(array) => {
                for child in array {
                    self.collect_strings_for_keys(child, keys, strings);
                }
            }
            _ => {}
        }
    }

    fn collect_string_values(&self, value: &Value, strings: &mut Vec<String>) {
        match value {
            Value::String(string) => strings.push(string.clone()),
            Value::Array(array) => {
                for child in array {
                    self.collect_string_values(child, strings);
                }
            }
            Value::Object(map) => {
                for child in map.values() {
                    self.collect_string_values(child, strings);
                }
            }
            _ => {}
        }
    }

    fn value_contains_key_part(&self, value: &Value, needle: &str) -> bool {
        match value {
            Value::Object(map) => {
                map.keys().any(|key| key.contains(needle))
                    || map
                        .values()
                        .any(|child| self.value_contains_key_part(child, needle))
            }
            Value::Array(array) => array
                .iter()
                .any(|child| self.value_contains_key_part(child, needle)),
            _ => false,
        }
    }

    fn collect_tool_calls_from_value(
        &self,
        record: &ClaudeJsonLine,
        value: &Value,
        calls: &mut Vec<ClaudeToolCall>,
    ) {
        match value {
            Value::Object(map) => {
                if map.get("type").and_then(Value::as_str) == Some("tool_use") {
                    if let Some(name) = map.get("name").and_then(Value::as_str) {
                        let identifier =
                            map.get("id").and_then(Value::as_str).map(ToOwned::to_owned);
                        let input = map.get("input").cloned().unwrap_or(Value::Null);
                        calls.push(ClaudeToolCall::new(
                            record,
                            identifier,
                            name.to_string(),
                            input,
                        ));
                    }
                }
                for child in map.values() {
                    self.collect_tool_calls_from_value(record, child, calls);
                }
            }
            Value::Array(array) => {
                for child in array {
                    self.collect_tool_calls_from_value(record, child, calls);
                }
            }
            _ => {}
        }
    }

    fn collect_tool_results_from_value(
        &self,
        record: &ClaudeJsonLine,
        value: &Value,
        results: &mut Vec<ClaudeToolResult>,
    ) {
        match value {
            Value::Object(map) => {
                if map.get("type").and_then(Value::as_str) == Some("tool_result") {
                    let tool_use_identifier = map
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                    let content = map
                        .get("content")
                        .map(|content| {
                            let mut strings = Vec::new();
                            self.collect_string_values(content, &mut strings);
                            strings
                        })
                        .unwrap_or_default();
                    let stdout = map
                        .get("stdout")
                        .map(|content| {
                            let mut strings = Vec::new();
                            self.collect_string_values(content, &mut strings);
                            strings
                        })
                        .unwrap_or_default();
                    let is_error = map.get("is_error").and_then(Value::as_bool);
                    results.push(ClaudeToolResult::new(
                        record,
                        tool_use_identifier,
                        content,
                        stdout,
                        is_error,
                    ));
                }
                for child in map.values() {
                    self.collect_tool_results_from_value(record, child, results);
                }
            }
            Value::Array(array) => {
                for child in array {
                    self.collect_tool_results_from_value(record, child, results);
                }
            }
            _ => {}
        }
    }
}
