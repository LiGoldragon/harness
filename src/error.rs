use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("terminal transport failed: {0}")]
    TerminalTransport(#[from] persona_wezterm::Error),

    #[error("invalid WezTerm pane id {target:?}")]
    InvalidWezTermPane { target: String },
}

pub type Result<T> = std::result::Result<T, Error>;
