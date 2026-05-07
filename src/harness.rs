#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HarnessId {
    value: String,
}

impl HarnessId {
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
pub enum HarnessKind {
    Codex,
    Claude,
    Pi,
    Other { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessBinding {
    id: HarnessId,
    kind: HarnessKind,
    working_directory: String,
}

impl HarnessBinding {
    pub fn new(id: HarnessId, kind: HarnessKind, working_directory: impl Into<String>) -> Self {
        Self {
            id,
            kind,
            working_directory: working_directory.into(),
        }
    }

    pub fn id(&self) -> &HarnessId {
        &self.id
    }

    pub fn working_directory(&self) -> &str {
        &self.working_directory
    }
}
