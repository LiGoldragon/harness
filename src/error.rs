use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_core::FrameError),

    #[error("actor call: {0}")]
    ActorCall(String),

    #[error("harness socket path is missing")]
    MissingSocket,

    #[error("unexpected harness command-line argument: {got}")]
    UnexpectedArgument { got: String },

    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },

    #[error("terminal transport failed: {0}")]
    TerminalTransport(#[from] persona_terminal::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
