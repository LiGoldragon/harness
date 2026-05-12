pub mod error;
pub mod harness;
pub mod runtime;
pub mod terminal;
pub mod transcript;

pub use error::{Error, Result};
pub use harness::{
    HarnessBinding, HarnessId, HarnessIdentityProjection, HarnessIdentityView, HarnessKind,
};
pub use runtime::{
    Harness, HarnessLifecycle, HarnessState, ReadState, RecordTranscriptLine, SetHarnessLifecycle,
};
pub use terminal::{
    HarnessTerminalBinding, HarnessTerminalDelivery, HarnessTerminalEndpoint, TerminalDeliveryPath,
    TerminalDeliveryReceipt,
};
pub use transcript::{TranscriptEvent, TranscriptLine};
