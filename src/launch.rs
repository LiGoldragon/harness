//! Session launch: executing the orchestrator's `LaunchSession` command.
//!
//! Production kinds launch inside a terminal-cell PTY session so the spawned
//! harness is discoverable through its session directory; the fixture kind
//! spawns its configured command directly for hermetic witnesses. The initial
//! prompt — carrying the orchestrator-minted agent identity — is delivered as
//! the final spawn argument (spawn-time prompt delivery).
//!
//! The terminal-cell leg drives the `terminal-cell` CLI with NOTA text and
//! reads launch facts from the session runtime directory. A typed library
//! dependency on terminal-cell is currently impossible: its build sits on the
//! new schema dialect while `signal-persona` (a required dependency here)
//! still pins the old one, and no lock can hold both emitter worlds. The
//! composed request text is golden-tested against output captured from the
//! owning `LaunchCell` type, and the session-directory layout
//! (`session-<stem>-<millis>/`, `child.pid`) is the same layout orchestrate's
//! reachability discovery already depends on.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use meta_signal_harness::MetaHarnessReply;
use nota::NotaEncode;
use signal_harness::{
    AgentIdentityToken, ContinuationRequest, HarnessKind, SessionDirectory,
    SessionLaunchRefusalReason, SessionLaunchRefused, SessionLaunchRequest, SessionLaunched,
};

/// The spawn row for one production harness kind: program plus leading
/// arguments, with the initial prompt appended as the final argument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessLaunchCommand {
    program: String,
    arguments: Vec<String>,
}

impl HarnessLaunchCommand {
    /// Cooperative-channel flag from the adopted Claude live-delivery
    /// posture; its effectiveness on the installed build is unverified.
    const CLAUDE_COOPERATIVE_CHANNEL_FLAG: &'static str = "--channels";

    fn for_request(request: &SessionLaunchRequest) -> Result<Self, SessionLaunchRefused> {
        let prompt = request.initial_prompt.as_str().to_string();
        match request.harness_kind {
            HarnessKind::Pi => Ok(Self {
                program: "pi".to_string(),
                arguments: vec![prompt],
            }),
            HarnessKind::Claude => Ok(Self {
                program: "claude".to_string(),
                arguments: vec![Self::CLAUDE_COOPERATIVE_CHANNEL_FLAG.to_string(), prompt],
            }),
            HarnessKind::Codex => Err(SessionLaunchRefused {
                request: request.clone(),
                reason: SessionLaunchRefusalReason::HarnessKindUnsupported,
                detail: "codex launch is deferred".to_string(),
            }),
            HarnessKind::Fixture => Err(SessionLaunchRefused {
                request: request.clone(),
                reason: SessionLaunchRefusalReason::HarnessKindUnsupported,
                detail: "fixture launches spawn directly, not through a terminal cell"
                    .to_string(),
            }),
        }
    }

    /// The `(LaunchCell …)` request text for this spawn row, shaped exactly
    /// as the owning `LaunchCell` type encodes it (golden-tested). Strings go
    /// through `NotaEncode`, so prompt text with brackets or quotes stays
    /// intact.
    fn launch_cell_nota(&self, agent_identity: &AgentIdentityToken) -> String {
        let requested_name = format!("agent-{}", agent_identity.as_str());
        let arguments = self
            .arguments
            .iter()
            .map(|argument| argument.to_nota())
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "(LaunchCell ((Some {}) None {} [{}] []))",
            requested_name.to_nota(),
            self.program.to_nota(),
            arguments
        )
    }
}

/// The terminal-cell session runtime root: where launched cells write their
/// session directories. Mirrors the CLI's root resolution
/// (`TERMINAL_CELL_RUNTIME_DIR`, then `XDG_RUNTIME_DIR`, then the temp
/// directory, each under `terminal-cell/`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalCellRuntimeRoot {
    path: PathBuf,
}

impl TerminalCellRuntimeRoot {
    const CHILD_PID_FILE: &'static str = "child.pid";
    const CHILD_PID_WAIT: Duration = Duration::from_millis(100);
    const CHILD_PID_ATTEMPTS: u32 = 20;

    pub fn from_environment() -> Self {
        let base = std::env::var_os("TERMINAL_CELL_RUNTIME_DIR")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from))
            .unwrap_or_else(std::env::temp_dir);
        Self {
            path: base.join("terminal-cell"),
        }
    }

    /// The newest session directory launched for this agent identity
    /// (`session-agent-<id>-<millis>`; the millis suffix orders launches).
    fn newest_session_directory_for(&self, agent_identity: &AgentIdentityToken) -> Option<PathBuf> {
        let prefix = format!("session-agent-{}-", agent_identity.as_str());
        let entries = std::fs::read_dir(&self.path).ok()?;
        entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().is_dir())
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                let suffix = name.strip_prefix(&prefix)?.parse::<u128>().ok()?;
                Some((suffix, entry.path()))
            })
            .max_by_key(|(suffix, _)| *suffix)
            .map(|(_, path)| path)
    }

    /// The launched child's pid, waiting briefly for the daemon to write the
    /// pid file after readiness.
    fn child_process_id(&self, session_directory: &Path) -> Option<u32> {
        let pid_path = session_directory.join(Self::CHILD_PID_FILE);
        for _ in 0..Self::CHILD_PID_ATTEMPTS {
            if let Ok(text) = std::fs::read_to_string(&pid_path) {
                if let Ok(pid) = text.trim().parse::<u32>() {
                    return Some(pid);
                }
            }
            std::thread::sleep(Self::CHILD_PID_WAIT);
        }
        None
    }
}

/// Direct-spawn command used for the fixture kind in hermetic witnesses. The
/// launch prompt is appended as the final argument, exactly like production
/// spawn rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FixtureLaunchCommand {
    program: PathBuf,
    arguments: Vec<String>,
}

impl FixtureLaunchCommand {
    pub fn new(program: impl Into<PathBuf>, arguments: Vec<String>) -> Self {
        Self {
            program: program.into(),
            arguments,
        }
    }
}

/// Launch policy owner. Production kinds go through the terminal-cell CLI so
/// the child lives inside a PTY-owning session directory; the fixture kind
/// spawns its configured command directly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionLauncher {
    terminal_cell_program: PathBuf,
    runtime_root: TerminalCellRuntimeRoot,
    fixture_command: Option<FixtureLaunchCommand>,
}

impl SessionLauncher {
    const TERMINAL_CELL_PROGRAM: &'static str = "terminal-cell";

    /// Production launcher: terminal-cell CLI from the process environment,
    /// no fixture command.
    pub fn from_environment() -> Self {
        Self {
            terminal_cell_program: PathBuf::from(Self::TERMINAL_CELL_PROGRAM),
            runtime_root: TerminalCellRuntimeRoot::from_environment(),
            fixture_command: None,
        }
    }

    /// Witness launcher: fixture-kind requests spawn this command directly.
    pub fn with_fixture_command(fixture_command: FixtureLaunchCommand) -> Self {
        Self {
            terminal_cell_program: PathBuf::from(Self::TERMINAL_CELL_PROGRAM),
            runtime_root: TerminalCellRuntimeRoot::from_environment(),
            fixture_command: Some(fixture_command),
        }
    }

    /// Execute one launch request; every outcome is a complete typed reply.
    pub fn launch(&self, request: SessionLaunchRequest) -> MetaHarnessReply {
        if !matches!(request.continuation, ContinuationRequest::Fresh) {
            return MetaHarnessReply::SessionLaunchRefused(SessionLaunchRefused {
                request,
                reason: SessionLaunchRefusalReason::ContinuationUnsupported,
                detail: "session continuation at launch is not built yet; launch fresh"
                    .to_string(),
            });
        }
        match request.harness_kind {
            HarnessKind::Fixture => self.launch_fixture(request),
            _ => self.launch_through_terminal_cell(request),
        }
    }

    fn launch_fixture(&self, request: SessionLaunchRequest) -> MetaHarnessReply {
        let Some(fixture) = &self.fixture_command else {
            return MetaHarnessReply::SessionLaunchRefused(SessionLaunchRefused {
                request,
                reason: SessionLaunchRefusalReason::LauncherUnavailable,
                detail: "no fixture launch command is configured".to_string(),
            });
        };
        let spawned = Command::new(&fixture.program)
            .args(&fixture.arguments)
            .arg(request.initial_prompt.as_str())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        match spawned {
            Ok(mut child) => {
                let child_process_id = child.id();
                std::thread::spawn(move || {
                    let _ = child.wait();
                });
                MetaHarnessReply::SessionLaunched(SessionLaunched {
                    agent_identity: request.agent_identity,
                    child_process_id,
                    session_directory: None,
                    continuation: None,
                })
            }
            Err(error) => MetaHarnessReply::SessionLaunchRefused(SessionLaunchRefused {
                request,
                reason: SessionLaunchRefusalReason::SpawnFailed,
                detail: format!("fixture spawn failed: {error}"),
            }),
        }
    }

    fn launch_through_terminal_cell(&self, request: SessionLaunchRequest) -> MetaHarnessReply {
        let command = match HarnessLaunchCommand::for_request(&request) {
            Ok(command) => command,
            Err(refused) => return MetaHarnessReply::SessionLaunchRefused(refused),
        };
        let output = Command::new(&self.terminal_cell_program)
            .arg(command.launch_cell_nota(&request.agent_identity))
            .stdin(Stdio::null())
            .output();
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                return MetaHarnessReply::SessionLaunchRefused(SessionLaunchRefused {
                    request,
                    reason: SessionLaunchRefusalReason::LauncherUnavailable,
                    detail: format!(
                        "terminal-cell launcher {} did not run: {error}",
                        self.terminal_cell_program.display()
                    ),
                });
            }
        };
        if !output.status.success() {
            return MetaHarnessReply::SessionLaunchRefused(SessionLaunchRefused {
                request,
                reason: SessionLaunchRefusalReason::SpawnFailed,
                detail: format!(
                    "terminal-cell launch exited {}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                ),
            });
        }
        let Some(session_directory) = self
            .runtime_root
            .newest_session_directory_for(&request.agent_identity)
        else {
            return MetaHarnessReply::SessionLaunchRefused(SessionLaunchRefused {
                request,
                reason: SessionLaunchRefusalReason::SpawnFailed,
                detail: "terminal-cell reported success but no session directory appeared"
                    .to_string(),
            });
        };
        let Some(child_process_id) = self.runtime_root.child_process_id(&session_directory) else {
            return MetaHarnessReply::SessionLaunchRefused(SessionLaunchRefused {
                request,
                reason: SessionLaunchRefusalReason::SpawnFailed,
                detail: format!(
                    "terminal-cell session {} wrote no readable child pid",
                    session_directory.display()
                ),
            });
        };
        MetaHarnessReply::SessionLaunched(SessionLaunched {
            agent_identity: request.agent_identity,
            child_process_id,
            session_directory: Some(SessionDirectory::new(
                session_directory.to_string_lossy().into_owned(),
            )),
            continuation: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_harness::InitialPrompt;

    fn request(kind: HarnessKind, prompt: &str) -> SessionLaunchRequest {
        SessionLaunchRequest {
            harness_kind: kind,
            agent_identity: AgentIdentityToken::new("xk3f"),
            initial_prompt: InitialPrompt::new(prompt),
            continuation: ContinuationRequest::Fresh,
        }
    }

    /// Golden captured from the owning `LaunchCell` type's `to_nota()` in the
    /// terminal-cell worktree (2026-07-18), environment emptied.
    #[test]
    fn launch_cell_nota_matches_owner_encoding() {
        let command = HarnessLaunchCommand::for_request(&request(
            HarnessKind::Pi,
            "You are agent xk3f. Do [things] \"quoted\" (parens)",
        ))
        .expect("pi spawn row");
        assert_eq!(
            command.launch_cell_nota(&AgentIdentityToken::new("xk3f")),
            "(LaunchCell ((Some agent-xk3f) None pi [[|You are agent xk3f. Do [things] \"quoted\" (parens)|]] []))"
        );
    }

    #[test]
    fn claude_spawn_row_carries_cooperative_channel_flag_before_prompt() {
        let command = HarnessLaunchCommand::for_request(&request(
            HarnessKind::Claude,
            "You are agent xk3f.",
        ))
        .expect("claude spawn row");
        assert_eq!(command.program, "claude");
        assert_eq!(
            command.arguments,
            vec!["--channels".to_string(), "You are agent xk3f.".to_string()]
        );
    }
}
