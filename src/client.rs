use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use nota_next::{NotaEncode, NotaSource};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use signal_harness::{HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessRequest};
use triad_runtime::{ComponentCommand, FrameBody as RuntimeFrameBody, LengthPrefixedCodec};

use crate::cli_argument::NotaCommandText;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessClient {
    endpoint: HarnessEndpoint,
    codec: LengthPrefixedCodec,
}

impl HarnessClient {
    pub fn new(endpoint: HarnessEndpoint) -> Self {
        Self {
            endpoint,
            codec: LengthPrefixedCodec::default(),
        }
    }

    pub fn submit(&self, request: HarnessRequest) -> Result<HarnessEvent> {
        let exchange = self.exchange();
        let frame = HarnessFrame::new(HarnessFrameBody::Request {
            exchange,
            request: signal_frame::Request::from_payload(request),
        });
        let mut stream = UnixStream::connect(self.endpoint.as_path())?;
        self.codec
            .write_body(&mut stream, &RuntimeFrameBody::new(frame.encode()?))?;
        let body = self.codec.read_body(&mut stream)?;
        self.reply_from_frame(HarnessFrame::decode(body.bytes())?)
    }

    fn exchange(&self) -> ExchangeIdentifier {
        let _endpoint = &self.endpoint;
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    fn reply_from_frame(&self, frame: HarnessFrame) -> Result<HarnessEvent> {
        match frame.into_body() {
            HarnessFrameBody::Reply { reply, .. } => self.reply_output(reply),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    fn reply_output(&self, reply: Reply<HarnessEvent>) -> Result<HarnessEvent> {
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
pub struct HarnessCommandLine {
    command: ComponentCommand,
    environment: HarnessCommandEnvironment,
}

impl HarnessCommandLine {
    pub fn from_env() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
            environment: HarnessCommandEnvironment::from_process(),
        }
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
            command: ComponentCommand::from_arguments(arguments),
            environment,
        }
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let request = HarnessRequestText::from_command(self.command)?.into_request()?;
        let reply = HarnessClient::new(self.environment.endpoint()).submit(request)?;
        writeln!(output, "{}", reply.to_nota())?;
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct HarnessRequestText {
    text: NotaCommandText,
}

impl HarnessRequestText {
    fn from_command(command: ComponentCommand) -> Result<Self> {
        Ok(Self {
            text: NotaCommandText::from_command(command)?,
        })
    }

    fn into_request(self) -> Result<HarnessRequest> {
        Ok(NotaSource::new(self.text.as_str()).parse::<HarnessRequest>()?)
    }
}
