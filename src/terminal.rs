use std::io::{BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, Request, SessionEpoch, SubReply,
};
use signal_terminal::{
    TerminalCapture, TerminalFrame, TerminalFrameBody, TerminalInput, TerminalInputBytes,
    TerminalName, TerminalReply, TerminalRequest,
};

use crate::{HarnessIdentifier, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessTerminalBinding {
    harness: HarnessIdentifier,
    terminal: TerminalName,
}

impl HarnessTerminalBinding {
    pub fn for_harness(harness: HarnessIdentifier) -> Self {
        let terminal = TerminalName::new(harness.as_str());
        Self { harness, terminal }
    }

    pub fn new(harness: HarnessIdentifier, terminal: TerminalName) -> Self {
        Self { harness, terminal }
    }

    pub fn harness(&self) -> &HarnessIdentifier {
        &self.harness
    }

    pub fn terminal(&self) -> &TerminalName {
        &self.terminal
    }

    pub fn input_request(&self, bytes: Vec<u8>) -> TerminalRequest {
        TerminalInput {
            terminal: self.terminal.clone(),
            bytes: TerminalInputBytes::new(bytes),
        }
        .into()
    }

    pub fn capture_request(&self) -> TerminalRequest {
        TerminalCapture {
            terminal: self.terminal.clone(),
        }
        .into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessTerminalEndpoint {
    FixtureOnlyHuman,
    PtySocket { path: PathBuf },
}

impl HarnessTerminalEndpoint {
    pub fn fixture_only_human() -> Self {
        Self::FixtureOnlyHuman
    }

    pub fn pty_socket(path: impl Into<PathBuf>) -> Self {
        Self::PtySocket { path: path.into() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalDeliveryPath {
    FixtureOnly,
    TerminalTransport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalDeliveryReceipt {
    delivered: bool,
    path: TerminalDeliveryPath,
    accepted_event: Option<TerminalReply>,
}

impl TerminalDeliveryReceipt {
    fn fixture_only() -> Self {
        Self {
            delivered: false,
            path: TerminalDeliveryPath::FixtureOnly,
            accepted_event: None,
        }
    }

    fn from_transport(delivered: bool, accepted_event: TerminalReply) -> Self {
        Self {
            delivered,
            path: TerminalDeliveryPath::TerminalTransport,
            accepted_event: Some(accepted_event),
        }
    }

    pub fn delivered(&self) -> bool {
        self.delivered
    }

    pub fn path(&self) -> TerminalDeliveryPath {
        self.path
    }

    pub fn accepted_event(&self) -> Option<&TerminalReply> {
        self.accepted_event.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessTerminalDelivery {
    endpoint: HarnessTerminalEndpoint,
    delivered_input_count: u64,
}

impl HarnessTerminalDelivery {
    pub fn new(endpoint: HarnessTerminalEndpoint) -> Self {
        Self {
            endpoint,
            delivered_input_count: 0,
        }
    }

    pub fn endpoint(&self) -> &HarnessTerminalEndpoint {
        &self.endpoint
    }

    pub fn delivered_input_count(&self) -> u64 {
        self.delivered_input_count
    }

    pub fn deliver_text(
        &mut self,
        binding: &HarnessTerminalBinding,
        text: &str,
    ) -> Result<TerminalDeliveryReceipt> {
        match self.endpoint.clone() {
            HarnessTerminalEndpoint::FixtureOnlyHuman => {
                Ok(TerminalDeliveryReceipt::fixture_only())
            }
            HarnessTerminalEndpoint::PtySocket { path } => {
                self.deliver_to_pty(binding, text, path.as_path())
            }
        }
    }

    fn deliver_to_pty(
        &mut self,
        binding: &HarnessTerminalBinding,
        text: &str,
        path: &Path,
    ) -> Result<TerminalDeliveryReceipt> {
        let mut bytes = text.as_bytes().to_vec();
        bytes.push(b'\r');
        let accepted_event = TerminalSignalTransport::new(path)
            .exchange(binding.input_request(bytes), self.delivered_input_count)?;
        let delivered = matches!(accepted_event, TerminalReply::TerminalInputAccepted(_));
        if delivered {
            self.delivered_input_count = self.delivered_input_count.saturating_add(1);
        }
        Ok(TerminalDeliveryReceipt::from_transport(
            delivered,
            accepted_event,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalSignalTransport {
    socket_path: PathBuf,
}

impl TerminalSignalTransport {
    fn new(socket_path: &Path) -> Self {
        Self {
            socket_path: socket_path.to_path_buf(),
        }
    }

    fn exchange(&self, request: TerminalRequest, sequence: u64) -> Result<TerminalReply> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::new(sequence.saturating_add(1)),
        );
        let request = Request::from_payload(request);
        let frame = TerminalFrame::new(TerminalFrameBody::Request { exchange, request });
        stream.write_all(&frame.encode_length_prefixed()?)?;
        stream.flush()?;

        let mut reader = BufReader::new(stream);
        match self.read_reply_frame(&mut reader)?.into_body() {
            TerminalFrameBody::Reply { reply, .. } => Self::terminal_reply(reply),
            other => Err(crate::Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    fn read_reply_frame(&self, reader: &mut impl Read) -> Result<TerminalFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(TerminalFrame::decode_length_prefixed(&bytes)?)
    }

    fn terminal_reply(reply: Reply<TerminalReply>) -> Result<TerminalReply> {
        match reply {
            Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                SubReply::Ok(payload) => Ok(payload),
                other => Err(crate::Error::UnexpectedSignalFrame {
                    got: format!("{other:?}"),
                }),
            },
            Reply::Rejected { reason } => Err(crate::Error::UnexpectedSignalFrame {
                got: format!("{reason:?}"),
            }),
        }
    }
}
