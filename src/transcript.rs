use crate::HarnessId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptLine {
    value: String,
}

impl TranscriptLine {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptEvent {
    harness: HarnessId,
    line: TranscriptLine,
}

impl TranscriptEvent {
    pub fn new(harness: HarnessId, line: TranscriptLine) -> Self {
        Self { harness, line }
    }

    pub fn line(&self) -> &TranscriptLine {
        &self.line
    }
}
