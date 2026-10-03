use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use meta_signal_harness::{Query, Response};

use crate::Result;
use crate::cli_argument::{DatomArgument, DatomPrint};
use crate::wire::SignalWire;

const DEFAULT_META_HARNESS_SOCKET: &str = "/tmp/meta-harness.sock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHarnessEndpoint {
    socket: PathBuf,
}

impl MetaHarnessEndpoint {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn as_path(&self) -> &Path {
        &self.socket
    }
}

/// The `meta-signal-harness` client: one meta `Query` frame out, one meta
/// `Response` frame back, over the daemon's meta socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHarnessClient {
    endpoint: MetaHarnessEndpoint,
    wire: SignalWire,
}

impl MetaHarnessClient {
    pub fn new(endpoint: MetaHarnessEndpoint) -> Self {
        Self {
            endpoint,
            wire: SignalWire::default(),
        }
    }

    pub fn submit(&self, query: Query) -> Result<Response> {
        let mut stream = UnixStream::connect(self.endpoint.as_path())?;
        self.wire.write(&mut stream, &query)?;
        self.wire.read(&mut stream)
    }
}

/// `meta-harness '<Datom meta Query>'`: one inline Datom meta `Query`, one
/// Datom meta `Response` printed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHarnessCommandLine {
    arguments: Vec<String>,
    environment: MetaHarnessCommandEnvironment,
}

impl MetaHarnessCommandLine {
    pub fn from_env() -> Self {
        Self::from_arguments(std::env::args().skip(1))
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self::from_arguments_with_environment(
            arguments,
            MetaHarnessCommandEnvironment::from_process(),
        )
    }

    pub fn from_arguments_with_environment<Arguments, Argument>(
        arguments: Arguments,
        environment: MetaHarnessCommandEnvironment,
    ) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            arguments: arguments.into_iter().map(Into::into).collect(),
            environment,
        }
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let query: Query = DatomArgument::from_arguments(self.arguments)?.actualize()?;
        let response = MetaHarnessClient::new(self.environment.endpoint()).submit(query)?;
        writeln!(output, "{}", DatomPrint::of(&response).as_str())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHarnessCommandEnvironment {
    socket: String,
}

impl MetaHarnessCommandEnvironment {
    pub fn new(socket: impl Into<String>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn from_process() -> Self {
        Self::new(
            std::env::var("HARNESS_META_SOCKET").unwrap_or(DEFAULT_META_HARNESS_SOCKET.to_string()),
        )
    }

    pub fn endpoint(&self) -> MetaHarnessEndpoint {
        MetaHarnessEndpoint::new(&self.socket)
    }
}
