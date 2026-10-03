use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use signal_harness::{Query, Response};

use crate::cli_argument::{DatomArgument, DatomPrint};
use crate::usage::UsageView;
use crate::wire::SignalWire;
use crate::{Error, Result};

const DEFAULT_HARNESS_SOCKET: &str = "/tmp/harness.sock";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessEndpoint {
    socket: PathBuf,
}

impl HarnessEndpoint {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn as_path(&self) -> &Path {
        &self.socket
    }
}

/// The ordinary `signal-harness` client: one `Query` frame out, one
/// `Response` frame back, over the daemon's ordinary socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessClient {
    endpoint: HarnessEndpoint,
    wire: SignalWire,
}

impl HarnessClient {
    pub fn new(endpoint: HarnessEndpoint) -> Self {
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

/// `harness '<Datom Query>'`: one inline Datom `Query`, one Datom `Response`
/// printed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessCommandLine {
    arguments: Vec<String>,
    environment: HarnessCommandEnvironment,
}

impl HarnessCommandLine {
    pub fn from_env() -> Self {
        Self::from_arguments(std::env::args().skip(1))
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self::from_arguments_with_environment(arguments, HarnessCommandEnvironment::from_process())
    }

    pub fn from_arguments_with_environment<Arguments, Argument>(
        arguments: Arguments,
        environment: HarnessCommandEnvironment,
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
        let response = HarnessClient::new(self.environment.endpoint()).submit(query)?;
        writeln!(output, "{}", DatomPrint::of(&response).as_str())?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessCommandEnvironment {
    socket: String,
}

impl HarnessCommandEnvironment {
    pub fn new(socket: impl Into<String>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn from_process() -> Self {
        Self::new(std::env::var("HARNESS_SOCKET").unwrap_or(DEFAULT_HARNESS_SOCKET.to_string()))
    }

    pub fn endpoint(&self) -> HarnessEndpoint {
        HarnessEndpoint::new(&self.socket)
    }
}

/// `harness-usage`: one `UsageSnapshotQuery` to the daemon, the snapshot
/// printed as the human view. It takes no argument; the typed reply is
/// `harness UsageSnapshotQuery`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageCommandLine {
    arguments: Vec<String>,
    environment: HarnessCommandEnvironment,
}

impl UsageCommandLine {
    pub fn from_env() -> Self {
        Self::from_arguments_with_environment(
            std::env::args().skip(1),
            HarnessCommandEnvironment::from_process(),
        )
    }

    pub fn from_arguments_with_environment<Arguments, Argument>(
        arguments: Arguments,
        environment: HarnessCommandEnvironment,
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
        if !self.arguments.is_empty() {
            return Err(Error::ArgumentCount {
                count: self.arguments.len(),
            });
        }
        let response =
            HarnessClient::new(self.environment.endpoint()).submit(Query::UsageSnapshotQuery)?;
        match response {
            Response::UsageSnapshot(snapshot) => {
                write!(output, "{}", UsageView::new(snapshot).render())?;
                Ok(())
            }
            other => Err(Error::UnexpectedSignalFrame {
                got: DatomPrint::of(&other).as_str().to_owned(),
            }),
        }
    }
}
