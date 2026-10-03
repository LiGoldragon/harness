//! One-shot, read-only subscription-usage snapshot.
//!
//! One read assembles one `UsageSnapshot`: every Claude and Codex subscription's
//! quota windows and every live session's context, each with its own
//! observation time and freshness. There is no polling loop, watch, store or
//! refresh: the caller paces reads.
//!
//! The reply types are the `signal-harness` 5.0.0 contract, consumed here as
//! `usage_contract` because the rest of this crate is still wired to the
//! pre-Datom `signal-harness` 0.4.0. When the crate migrates, the two collapse.
//!
//! Security boundary: the Claude access token is read inside
//! [`claude_quota`] and reaches nothing but the HTTP `Authorization` header.
//! No credential, credential path, raw provider body, process argument or
//! environment value enters a reply; every failure is a typed category.

pub mod app_server;
pub mod claude_context;
pub mod claude_quota;
pub mod codex_context;
pub mod codex_quota;
pub mod home;
pub mod pace;
pub mod tail;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use usage_contract::{
    SessionContextObservation, SubscriptionObservation, SubscriptionUsage, UsageSnapshot,
};

pub use app_server::{AppServerFailure, AppServerSession, AppServerTimeout, JsonRpcExchange};
pub use claude_context::ClaudeLiveSessions;
pub use claude_quota::{ClaudeUsageDocument, ClaudeUsageEndpoint, ClaudeUsageSource};
pub use codex_context::CodexLiveThreads;
pub use codex_quota::{CodexRateLimitsDocument, CodexUsageSource};
pub use home::{CodexHome, UsageHome};
pub use pace::WindowReading;
pub use tail::{TailReader, TranscriptTail, TranscriptTailReading};

/// A source of subscription quota observations for one provider.
pub trait QuotaSource {
    fn observe_quota(&self, instant: ObservationInstant) -> Vec<SubscriptionObservation>;
}

/// A source of live-session context observations for one provider.
pub trait ContextSource {
    fn observe_context(&self, instant: ObservationInstant) -> Vec<SessionContextObservation>;
}

/// A recorded provider quota document that normalizes into the contract.
pub trait QuotaDocument {
    fn normalize(&self, instant: ObservationInstant) -> SubscriptionUsage;
}

/// The one-call read: one snapshot per call, nothing retained.
pub trait UsageSnapshotReading {
    fn read_snapshot(&self) -> UsageSnapshot;
}

/// The moment an observation was made, in Unix-epoch nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservationInstant {
    nanoseconds: i64,
}

impl From<SystemTime> for ObservationInstant {
    fn from(time: SystemTime) -> Self {
        let since = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        Self {
            nanoseconds: i64::try_from(since.as_nanos()).unwrap_or(i64::MAX),
        }
    }
}

// Exception (too trivial): plain accessors and constructors over one integer.
impl ObservationInstant {
    pub fn now() -> Self {
        SystemTime::now().into()
    }

    pub fn from_nanoseconds(nanoseconds: i64) -> Self {
        Self { nanoseconds }
    }

    pub fn nanoseconds(self) -> i64 {
        self.nanoseconds
    }

    pub fn seconds(self) -> i64 {
        self.nanoseconds.div_euclid(1_000_000_000)
    }
}

/// Every source the snapshot reads, bound to one home directory.
#[derive(Clone, Debug)]
pub struct UsageSnapshotReader {
    claude_quota: ClaudeUsageSource,
    codex_quota: CodexUsageSource,
    claude_context: ClaudeLiveSessions,
    codex_context: CodexLiveThreads,
}

impl UsageSnapshotReader {
    /// The production reader: the fixed Claude endpoint and the home's own
    /// provider directories.
    pub fn for_home(home: UsageHome) -> Self {
        Self::with_endpoint(home, ClaudeUsageEndpoint::anthropic())
    }

    /// A reader against another Claude endpoint, for recorded-provider tests.
    pub fn with_endpoint(home: UsageHome, endpoint: ClaudeUsageEndpoint) -> Self {
        Self {
            claude_quota: ClaudeUsageSource::new(home.clone(), endpoint),
            codex_quota: CodexUsageSource::new(home.clone(), AppServerTimeout::default()),
            claude_context: ClaudeLiveSessions::new(home.clone()),
            codex_context: CodexLiveThreads::new(home, AppServerTimeout::default()),
        }
    }
}

impl UsageSnapshotReading for UsageSnapshotReader {
    fn read_snapshot(&self) -> UsageSnapshot {
        let snapshot_time = ObservationInstant::now();
        let (claude_quota, codex_quota, claude_context, codex_context) =
            std::thread::scope(|scope| {
                let claude_quota =
                    scope.spawn(|| self.claude_quota.observe_quota(ObservationInstant::now()));
                let codex_quota =
                    scope.spawn(|| self.codex_quota.observe_quota(ObservationInstant::now()));
                let claude_context = scope.spawn(|| {
                    self.claude_context
                        .observe_context(ObservationInstant::now())
                });
                let codex_context = scope.spawn(|| {
                    self.codex_context
                        .observe_context(ObservationInstant::now())
                });
                (
                    claude_quota.join().unwrap_or_default(),
                    codex_quota.join().unwrap_or_default(),
                    claude_context.join().unwrap_or_default(),
                    codex_context.join().unwrap_or_default(),
                )
            });
        UsageSnapshot {
            snapshot_time: snapshot_time.nanoseconds(),
            subscription_observations: claude_quota.into_iter().chain(codex_quota).collect(),
            session_context_observations: claude_context.into_iter().chain(codex_context).collect(),
        }
    }
}
