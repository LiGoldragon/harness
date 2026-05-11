use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("terminal transport failed: {0}")]
    TerminalTransport(#[from] persona_terminal::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
