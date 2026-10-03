use std::path::{Path, PathBuf};

use signal_harness::HarnessDaemonConfiguration;
use triad_runtime::{BindingSurface, SocketMode as RuntimeSocketMode};

use crate::error::{Error, Result};

/// Harness's hand-written daemon configuration, wrapping the `signal-harness`
/// startup contract its emitter encodes before spawning `harness-daemon`. The
/// contract `HarnessDaemonConfiguration` is the externally-consumed boundary;
/// this type adds the cached `PathBuf`s the emitted daemon shell binds its
/// ordinary and meta listeners from through `triad_runtime::BindingSurface`,
/// the supervision socket the engine binds itself, and carries the decoded
/// contract through to the engine.
#[derive(Clone, Debug, PartialEq)]
pub struct Configuration {
    raw: HarnessDaemonConfiguration,
    harness_socket_path: PathBuf,
    meta_socket_path: PathBuf,
    supervision_socket_path: PathBuf,
    state_dir: PathBuf,
}

impl Configuration {
    pub fn from_raw(raw: HarnessDaemonConfiguration) -> Self {
        let harness_socket_path = PathBuf::from(&raw.domain_socket_path);
        let meta_socket_path = PathBuf::from(&raw.meta_socket_path);
        let supervision_socket_path = PathBuf::from(&raw.engine_management_socket_path);
        let state_dir = harness_socket_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        Self {
            raw,
            harness_socket_path,
            meta_socket_path,
            supervision_socket_path,
            state_dir,
        }
    }

    /// Read and decode the binary rkyv configuration from the daemon's single
    /// startup-argument file path. Daemons never parse NOTA — the contract is
    /// signal-encoded before it reaches the process.
    pub fn from_binary_path(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path).map_err(|source| Error::ConfigurationRead {
            path: path.to_path_buf(),
            source,
        })?;
        let raw = rkyv::from_bytes::<HarnessDaemonConfiguration, rkyv::rancor::Error>(&bytes)
            .map_err(|_| Error::ConfigurationArchiveDecode)?;
        Ok(Self::from_raw(raw))
    }

    pub fn raw(&self) -> &HarnessDaemonConfiguration {
        &self.raw
    }

    pub fn into_raw(self) -> HarnessDaemonConfiguration {
        self.raw
    }

    pub fn supervision_socket_path(&self) -> &Path {
        &self.supervision_socket_path
    }

    pub fn supervision_socket_mode(&self) -> u32 {
        self.raw.engine_management_socket_mode as u32
    }

    fn harness_socket_mode(&self) -> RuntimeSocketMode {
        RuntimeSocketMode::new(self.raw.domain_socket_mode as u32)
    }

    fn meta_listener_mode(&self) -> RuntimeSocketMode {
        RuntimeSocketMode::new(self.raw.meta_socket_mode as u32)
    }
}

impl BindingSurface for Configuration {
    fn socket_path(&self) -> &Path {
        &self.harness_socket_path
    }

    fn socket_mode(&self) -> Option<RuntimeSocketMode> {
        Some(self.harness_socket_mode())
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(&self.meta_socket_path)
    }

    fn meta_socket_mode(&self) -> Option<RuntimeSocketMode> {
        Some(self.meta_listener_mode())
    }

    fn database_path(&self) -> &Path {
        // `harness` opens no durable store — its harness instances are
        // transient kameo actors. The emitted shell never binds this path; the
        // accessor exists only because the trait requires it, so it points at
        // the daemon's state directory.
        &self.state_dir
    }
}

impl From<HarnessDaemonConfiguration> for Configuration {
    fn from(raw: HarnessDaemonConfiguration) -> Self {
        Self::from_raw(raw)
    }
}
