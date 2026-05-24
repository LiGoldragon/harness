#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HarnessIdentifier {
    value: String,
}

impl HarnessIdentifier {
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
    Fixture,
}

impl HarnessKind {
    /// Project a `signal-harness`-contract `HarnessKind` onto
    /// the internal kind enum.
    pub const fn from_contract(value: signal_harness::HarnessKind) -> Self {
        match value {
            signal_harness::HarnessKind::Codex => Self::Codex,
            signal_harness::HarnessKind::Claude => Self::Claude,
            signal_harness::HarnessKind::Pi => Self::Pi,
            signal_harness::HarnessKind::Fixture => Self::Fixture,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessBinding {
    id: HarnessIdentifier,
    kind: HarnessKind,
    working_directory: String,
}

impl HarnessBinding {
    pub fn new(
        id: HarnessIdentifier,
        kind: HarnessKind,
        working_directory: impl Into<String>,
    ) -> Self {
        Self {
            id,
            kind,
            working_directory: working_directory.into(),
        }
    }

    pub fn id(&self) -> &HarnessIdentifier {
        &self.id
    }

    pub fn working_directory(&self) -> &str {
        &self.working_directory
    }

    pub fn identity_projection(&self, view: HarnessIdentityView) -> HarnessIdentityProjection {
        match view {
            HarnessIdentityView::Full => HarnessIdentityProjection {
                id: Some(self.id.clone()),
                kind: Some(self.kind.clone()),
                working_directory: Some(self.working_directory.clone()),
            },
            HarnessIdentityView::Redacted => HarnessIdentityProjection {
                id: Some(self.id.clone()),
                kind: None,
                working_directory: None,
            },
            HarnessIdentityView::Hidden => HarnessIdentityProjection {
                id: None,
                kind: None,
                working_directory: None,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessIdentityView {
    Full,
    Redacted,
    Hidden,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessIdentityProjection {
    id: Option<HarnessIdentifier>,
    kind: Option<HarnessKind>,
    working_directory: Option<String>,
}

impl HarnessIdentityProjection {
    pub fn id(&self) -> Option<&HarnessIdentifier> {
        self.id.as_ref()
    }

    pub fn kind(&self) -> Option<&HarnessKind> {
        self.kind.as_ref()
    }

    pub fn working_directory(&self) -> Option<&str> {
        self.working_directory.as_deref()
    }
}
