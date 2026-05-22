use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_core::FrameError),

    #[error("actor call: {0}")]
    ActorCall(String),

    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },

    #[error("signal request failed structural checks: {reason}")]
    InvalidSignalRequest {
        reason: signal_core::RequestRejectionReason,
    },

    #[error("terminal transport failed: {0}")]
    TerminalTransport(#[from] persona_terminal::Error),

    #[error("nota-config: {0}")]
    NotaConfig(#[from] nota_config::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
