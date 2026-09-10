use std::io::{BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use signal_terminal::{
    ByteViewable, Query as TerminalInputRoot, Response as TerminalOutput, Restorable, Signal,
    Signalizable, TerminalCaptureRequest, TerminalInputRequest, TerminalName,
};

use crate::{HarnessIdentifier, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessTerminalBinding {
    harness: HarnessIdentifier,
    terminal: TerminalName,
}

impl HarnessTerminalBinding {
    pub fn for_harness(harness: HarnessIdentifier) -> Self {
        let terminal = harness.as_str().to_owned();
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

    pub fn input_request(&self, bytes: Vec<u8>) -> TerminalInputRoot {
        TerminalInputRoot::TerminalInput(TerminalInputRequest {
            terminal: self.terminal.clone(),
            input_bytes: bytes.into_iter().map(i64::from).collect(),
        })
    }

    pub fn capture_request(&self) -> TerminalInputRoot {
        TerminalInputRoot::TerminalCapture(TerminalCaptureRequest {
            terminal: self.terminal.clone(),
        })
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

#[derive(Debug, Clone, PartialEq)]
pub struct TerminalDeliveryReceipt {
    delivered: bool,
    path: TerminalDeliveryPath,
    accepted_event: Option<TerminalOutput>,
}

impl TerminalDeliveryReceipt {
    fn fixture_only() -> Self {
        Self {
            delivered: false,
            path: TerminalDeliveryPath::FixtureOnly,
            accepted_event: None,
        }
    }

    fn from_transport(delivered: bool, accepted_event: TerminalOutput) -> Self {
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

    pub fn accepted_event(&self) -> Option<&TerminalOutput> {
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
        let delivered = matches!(accepted_event, TerminalOutput::TerminalInputAccepted(_));
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

    fn exchange(&self, request: TerminalInputRoot, _sequence: u64) -> Result<TerminalOutput> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        let bytes = request
            .signalize()
            .map_err(|error| crate::Error::UnexpectedSignalFrame {
                got: error.to_string(),
            })?
            .bytes()
            .to_vec();
        stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
        stream.write_all(&bytes)?;
        stream.flush()?;
        let mut reader = BufReader::new(stream);
        let mut prefix = [0; 4];
        reader.read_exact(&mut prefix)?;
        let mut bytes = vec![0; u32::from_be_bytes(prefix) as usize];
        reader.read_exact(&mut bytes)?;
        Signal::<TerminalOutput>::from(bytes)
            .restore()
            .map_err(|error| crate::Error::UnexpectedSignalFrame {
                got: error.to_string(),
            })
    }
}
