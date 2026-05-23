use crate::HarnessIdentifier;

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

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct TranscriptEvent {
    harness: HarnessIdentifier,
    line: TranscriptLine,
}

impl TranscriptEvent {
    pub fn new(harness: HarnessIdentifier, line: TranscriptLine) -> Self {
        Self { harness, line }
    }

    pub fn harness(&self) -> &HarnessIdentifier {
        &self.harness
    }

    pub fn line(&self) -> &TranscriptLine {
        &self.line
    }
}
