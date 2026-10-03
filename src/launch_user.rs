//! The user-service launch of `harness-daemon`.
//!
//! `harness-daemon` takes one argument: a binary rkyv
//! `HarnessDaemonConfiguration` file. A user's own service has no manager to
//! write that file, so this launcher writes it and then becomes the daemon.
//! It takes no argument and reads no configuration text: the configuration
//! is fixed by the service's runtime directory and the launching user.
//!
//! - The runtime directory is the one systemd created for the service
//!   (`RUNTIME_DIRECTORY`); without it the launcher refuses to start.
//! - The ordinary, meta and supervision sockets are `harness.sock`,
//!   `meta-harness.sock` and `supervision.sock` there, each mode `0600`.
//! - The owner is the launching user's own uid.
//! - The instance set is empty: the daemon answers daemon-scope requests such
//!   as `UsageSnapshotQuery` and launches no harness session.

use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};

use signal_harness::HarnessDaemonConfiguration;
use signal_persona::OwnerIdentity;

use crate::{Error, Result};

const OWNER_ONLY: i64 = 0o600;
const CONFIGURATION_FILE: &str = "harness-daemon.rkyv";
const DAEMON_PROGRAM: &str = "harness-daemon";

/// The service's runtime directory and the user it runs as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserServiceLaunch {
    runtime_directory: PathBuf,
    owner: u32,
}

impl UserServiceLaunch {
    pub fn new(runtime_directory: impl Into<PathBuf>, owner: u32) -> Self {
        Self {
            runtime_directory: runtime_directory.into(),
            owner,
        }
    }

    /// The launch systemd set up: its `RUNTIME_DIRECTORY` and this process's
    /// own uid.
    pub fn from_service() -> Result<Self> {
        let runtime_directory = std::env::var_os("RUNTIME_DIRECTORY")
            .filter(|directory| !directory.is_empty())
            .ok_or(Error::RuntimeDirectoryAbsent)?;
        Ok(Self::new(
            runtime_directory,
            rustix::process::getuid().as_raw(),
        ))
    }

    fn socket(&self, name: &str) -> String {
        self.runtime_directory.join(name).display().to_string()
    }

    /// The typed startup record this launch writes.
    pub fn configuration(&self) -> HarnessDaemonConfiguration {
        HarnessDaemonConfiguration {
            domain_socket_path: self.socket("harness.sock"),
            domain_socket_mode: OWNER_ONLY,
            meta_socket_path: self.socket("meta-harness.sock"),
            meta_socket_mode: OWNER_ONLY,
            engine_management_socket_path: self.socket("supervision.sock"),
            engine_management_socket_mode: OWNER_ONLY,
            owner_identity: OwnerIdentity::UnixUser(i64::from(self.owner)),
            harness_instance_configurations: Vec::new(),
        }
    }

    /// Write the configuration owner-only into the runtime directory.
    pub fn write_configuration(&self) -> Result<PathBuf> {
        let path = self.runtime_directory.join(CONFIGURATION_FILE);
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.configuration())
            .map_err(|_| Error::ConfigurationArchiveEncode)?;
        let _ = std::fs::remove_file(&path);
        let write = || -> std::io::Result<()> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
            file.write_all(bytes.as_ref())?;
            file.sync_all()
        };
        write().map_err(|source| Error::ConfigurationWrite {
            path: path.clone(),
            source,
        })?;
        Ok(path)
    }

    /// Write the configuration and replace this process with the
    /// `harness-daemon` installed beside it. Returns only on failure.
    pub fn exec(&self) -> Error {
        let configuration = match self.write_configuration() {
            Ok(path) => path,
            Err(error) => return error,
        };
        let daemon = match Self::sibling_daemon() {
            Ok(daemon) => daemon,
            Err(error) => return error,
        };
        Error::Io(std::process::Command::new(daemon).arg(configuration).exec())
    }

    fn sibling_daemon() -> Result<PathBuf> {
        let launcher = std::env::current_exe()?;
        let directory = launcher.parent().unwrap_or(Path::new("."));
        Ok(directory.join(DAEMON_PROGRAM))
    }
}
