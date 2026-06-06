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

    #[error("nota-config: {0}")]
    NotaConfig(#[from] nota_config::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("pi rpc input stream was unavailable")]
    PiRpcInputUnavailable,

    #[error("pi rpc output stream was unavailable")]
    PiRpcOutputUnavailable,

    #[error("pi rpc response timed out for command {command_identifier}")]
    PiRpcResponseTimeout { command_identifier: String },

    #[error("pi rpc rejected command {command_identifier}: {error}")]
    PiRpcRejected {
        command_identifier: String,
        error: String,
    },

    #[error("pi rpc produced an unexpected response: {got}")]
    PiRpcUnexpectedResponse { got: String },
}

pub type Result<T> = std::result::Result<T, Error>;
