use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_core::FrameError),

    #[error("harness signal frame: {0}")]
    HarnessSignalFrame(#[from] signal_frame::FrameError),

    #[error("actor call: {0}")]
    ActorCall(String),

    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },

    #[error("signal request failed structural checks: {reason}")]
    InvalidSignalRequest {
        reason: signal_core::RequestRejectionReason,
    },

    #[error("daemon argument: {0}")]
    Argument(#[from] triad_runtime::ArgumentError),

    #[error("failed to read binary daemon configuration {path}: {source}")]
    ConfigurationRead {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to write binary daemon configuration {path}: {source}")]
    ConfigurationWrite {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to encode binary daemon configuration")]
    ConfigurationArchiveEncode,

    #[error("failed to decode binary daemon configuration")]
    ConfigurationArchiveDecode,

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
