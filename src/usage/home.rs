//! Where the provider sources live under one home directory.

use std::path::{Path, PathBuf};

/// The home directory whose Claude and Codex state the snapshot reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageHome {
    root: PathBuf,
}

/// One Codex home (`~/.codex`, `~/.codex-next`, ...): its directory name is the
/// only part of it a reply carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodexHome {
    directory: PathBuf,
    name: String,
}

impl UsageHome {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The invoking user's home, from the process's own `HOME`.
    pub fn from_process() -> Option<Self> {
        std::env::var_os("HOME").map(Self::new)
    }

    pub fn claude_credentials(&self) -> PathBuf {
        self.root.join(".claude").join(".credentials.json")
    }

    pub fn claude_sessions(&self) -> PathBuf {
        self.root.join(".claude").join("sessions")
    }

    pub fn claude_projects(&self) -> PathBuf {
        self.root.join(".claude").join("projects")
    }

    /// Every `~/.codex*` directory holding a ChatGPT login, in name order.
    /// Only the login file's existence is checked; it is never opened.
    pub fn codex_homes(&self) -> Vec<CodexHome> {
        let Ok(entries) = std::fs::read_dir(&self.root) else {
            return Vec::new();
        };
        let mut homes: Vec<CodexHome> = entries
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name = entry.file_name().into_string().ok()?;
                let directory = entry.path();
                (name.starts_with(".codex")
                    && directory.is_dir()
                    && directory.join("auth.json").exists())
                .then_some(CodexHome { directory, name })
            })
            .collect();
        homes.sort_by(|left, right| left.name.cmp(&right.name));
        homes
    }
}

impl CodexHome {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// The app-server control sockets this home publishes.
    pub fn control_sockets(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(self.directory.join("app-server-control")) else {
            return Vec::new();
        };
        let mut sockets: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "sock")
            })
            .collect();
        sockets.sort();
        sockets
    }
}
