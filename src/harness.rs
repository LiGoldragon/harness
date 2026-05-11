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

    pub fn identity_projection(&self, access: HarnessIdentityAccess) -> HarnessIdentityProjection {
        match access {
            HarnessIdentityAccess::Full => HarnessIdentityProjection {
                id: Some(self.id.clone()),
                kind: Some(self.kind.clone()),
                working_directory: Some(self.working_directory.clone()),
            },
            HarnessIdentityAccess::Redacted => HarnessIdentityProjection {
                id: Some(self.id.clone()),
                kind: None,
                working_directory: None,
            },
            HarnessIdentityAccess::Hidden => HarnessIdentityProjection {
                id: None,
                kind: None,
                working_directory: None,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessIdentityAccess {
    Full,
    Redacted,
    Hidden,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessIdentityProjection {
    id: Option<HarnessId>,
    kind: Option<HarnessKind>,
    working_directory: Option<String>,
}

impl HarnessIdentityProjection {
    pub fn id(&self) -> Option<&HarnessId> {
        self.id.as_ref()
    }

    pub fn kind(&self) -> Option<&HarnessKind> {
        self.kind.as_ref()
    }

    pub fn working_directory(&self) -> Option<&str> {
        self.working_directory.as_deref()
    }
}
