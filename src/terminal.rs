use std::path::{Path, PathBuf};

use persona_terminal::contract::TerminalTransportBinding;
use signal_persona_terminal::{
    TerminalCapture, TerminalEvent, TerminalInput, TerminalInputBytes, TerminalName,
    TerminalRequest,
};

use crate::{HarnessId, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessTerminalBinding {
    harness: HarnessId,
    terminal: TerminalName,
}

impl HarnessTerminalBinding {
    pub fn for_harness(harness: HarnessId) -> Self {
        let terminal = TerminalName::new(harness.as_str());
        Self { harness, terminal }
    }

    pub fn new(harness: HarnessId, terminal: TerminalName) -> Self {
        Self { harness, terminal }
    }

    pub fn harness(&self) -> &HarnessId {
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
    accepted_event: Option<TerminalEvent>,
}

impl TerminalDeliveryReceipt {
    fn fixture_only() -> Self {
        Self {
            delivered: false,
            path: TerminalDeliveryPath::FixtureOnly,
            accepted_event: None,
        }
    }

    fn from_transport(delivered: bool, accepted_event: TerminalEvent) -> Self {
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

    pub fn accepted_event(&self) -> Option<&TerminalEvent> {
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
        let mut transport =
            TerminalTransportBinding::from_socket_path(binding.terminal().clone(), path);
        let mut bytes = text.as_bytes().to_vec();
        bytes.push(b'\r');
        let accepted_event = transport.handle_request(binding.input_request(bytes))?;
        let delivered = matches!(accepted_event, TerminalEvent::TerminalInputAccepted(_));
        if delivered {
            self.delivered_input_count = self.delivered_input_count.saturating_add(1);
        }
        Ok(TerminalDeliveryReceipt::from_transport(
            delivered,
            accepted_event,
        ))
    }
}
