use crate::{
    HarnessTerminalBinding, HarnessTerminalDelivery, HarnessTerminalEndpoint, PiRpcDeliveryReceipt,
    PiRpcSession, Result, TerminalDeliveryReceipt,
};

#[derive(Debug)]
pub enum HarnessDeliveryAdapter {
    Terminal(HarnessTerminalDelivery),
    PiRpc(PiRpcSession),
}

impl HarnessDeliveryAdapter {
    pub fn terminal(endpoint: HarnessTerminalEndpoint) -> Self {
        Self::Terminal(HarnessTerminalDelivery::new(endpoint))
    }

    pub fn pi_rpc(session: PiRpcSession) -> Self {
        Self::PiRpc(session)
    }

    pub fn deliver_text(
        &mut self,
        binding: &HarnessTerminalBinding,
        text: &str,
    ) -> Result<HarnessDeliveryReceipt> {
        match self {
            Self::Terminal(delivery) => delivery
                .deliver_text(binding, text)
                .map(HarnessDeliveryReceipt::Terminal),
            Self::PiRpc(session) => session
                .deliver_text(text)
                .map(HarnessDeliveryReceipt::PiRpc),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessDeliveryReceipt {
    Terminal(TerminalDeliveryReceipt),
    PiRpc(PiRpcDeliveryReceipt),
}

impl HarnessDeliveryReceipt {
    pub fn delivered(&self) -> bool {
        match self {
            Self::Terminal(receipt) => receipt.delivered(),
            Self::PiRpc(_) => true,
        }
    }
}
