use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use meta_signal_harness::{
    MetaHarnessFrame, MetaHarnessFrameBody, MetaHarnessReply, MetaHarnessRequest,
};
use nota_next::{NotaEncode, NotaSource};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use triad_runtime::{ComponentCommand, FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

use crate::cli_argument::NotaCommandText;
use crate::{Error, Result};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHarnessClient {
    endpoint: MetaHarnessEndpoint,
    codec: LengthPrefixedCodec,
}

impl MetaHarnessClient {
    pub fn new(endpoint: MetaHarnessEndpoint) -> Self {
        Self {
            endpoint,
            codec: LengthPrefixedCodec::default(),
        }
    }

    pub fn submit(&self, request: MetaHarnessRequest) -> Result<MetaHarnessReply> {
        let exchange = self.exchange();
        let frame = MetaHarnessFrame::new(MetaHarnessFrameBody::Request {
            exchange,
            request: signal_frame::Request::from_payload(request),
        });
        let mut stream = UnixStream::connect(self.endpoint.as_path())?;
        self.codec
            .write_body(&mut stream, &RuntimeFrameBody::new(frame.encode()?))?;
        let body = self.codec.read_body(&mut stream)?;
        self.reply_from_frame(MetaHarnessFrame::decode(body.bytes())?)
    }

    fn exchange(&self) -> ExchangeIdentifier {
        let _endpoint = &self.endpoint;
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    fn reply_from_frame(&self, frame: MetaHarnessFrame) -> Result<MetaHarnessReply> {
        match frame.into_body() {
            MetaHarnessFrameBody::Reply { reply, .. } => self.reply_output(reply),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    fn reply_output(&self, reply: Reply<MetaHarnessReply>) -> Result<MetaHarnessReply> {
        let _endpoint = &self.endpoint;
        match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(payload) => Ok(payload),
                other => Err(Error::UnexpectedSignalFrame {
                    got: format!("{other:?}"),
                }),
            },
            Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                got: reason.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaHarnessCommandLine {
    command: ComponentCommand,
    environment: MetaHarnessCommandEnvironment,
}

impl MetaHarnessCommandLine {
    pub fn from_env() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
            environment: MetaHarnessCommandEnvironment::from_process(),
        }
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
            command: ComponentCommand::from_arguments(arguments),
            environment,
        }
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let request = MetaHarnessRequestText::from_command(self.command)?.into_request()?;
        let reply = MetaHarnessClient::new(self.environment.endpoint()).submit(request)?;
        writeln!(output, "{}", reply.to_nota())?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetaHarnessRequestText {
    text: NotaCommandText,
}

impl MetaHarnessRequestText {
    fn from_command(command: ComponentCommand) -> Result<Self> {
        Ok(Self {
            text: NotaCommandText::from_command(command)?,
        })
    }

    fn into_request(self) -> Result<MetaHarnessRequest> {
        Ok(NotaSource::new(self.text.as_str()).parse::<MetaHarnessRequest>()?)
    }
}
