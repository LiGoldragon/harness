use std::path::{Path, PathBuf};

use signal_harness::HarnessDaemonConfiguration;

use crate::Result;

/// A binary rkyv `HarnessDaemonConfiguration` file: the single startup argument
/// the harness daemon accepts. Daemons never parse NOTA (hard override) — the
/// `signal-harness` startup contract is signal-encoded into this file before it
/// reaches the process. Deploy/bootstrap tools and tests use this to write the
/// file the manager spawns `harness-daemon <path>` against.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessDaemonConfigurationFile {
    path: PathBuf,
}

impl HarnessDaemonConfigurationFile {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn as_path(&self) -> &Path {
        &self.path
    }

    pub fn configuration(&self) -> Result<HarnessDaemonConfiguration> {
        let bytes =
            std::fs::read(&self.path).map_err(|source| crate::Error::ConfigurationRead {
                path: self.path.clone(),
                source,
            })?;
        rkyv::from_bytes::<HarnessDaemonConfiguration, rkyv::rancor::Error>(&bytes)
            .map_err(|_| crate::Error::ConfigurationArchiveDecode)
    }

    pub fn write_configuration(&self, configuration: &HarnessDaemonConfiguration) -> Result<()> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(configuration)
            .map_err(|_| crate::Error::ConfigurationArchiveEncode)?;
        std::fs::write(&self.path, bytes.as_ref()).map_err(|source| {
            crate::Error::ConfigurationWrite {
                path: self.path.clone(),
                source,
            }
        })
    }
}
