//! One-shot, read-only subscription-usage snapshot.
//!
//! One read assembles one `UsageSnapshot`: every Claude and Codex subscription's
//! quota windows and every live session's context, each with its own
//! observation time and freshness. There is no polling loop, watch, store or
//! refresh: the caller paces reads.
//!
//! The reply types are the `signal-harness` contract's `UsageSnapshot`. Every
//! attempted provider, home and context collector appears in the reply: a
//! source that could not be read is a typed unavailable result with its
//! observation time and cause, never an omission, and a collector that failed
//! outright is `CollectorFailed`.
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
pub mod tail;
pub mod view;
pub mod window;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use signal_harness::{
    ContextSourceFailureReason, ContextSourceUnavailable, PlanningProjection,
    SessionContextObservation, SubscriptionObservation, SubscriptionUsage, UsageProvider,
    UsageSnapshot, UsageUnavailable, UsageUnavailableReason,
};

pub use app_server::{AppServerFailure, AppServerSession, AppServerTimeout, JsonRpcExchange};
pub use claude_context::ClaudeLiveSessions;
pub use claude_quota::{ClaudeUsageDocument, ClaudeUsageEndpoint, ClaudeUsageSource};
pub use codex_context::CodexLiveThreads;
pub use codex_quota::{CodexRateLimitsDocument, CodexUsageSource};
pub use home::{CodexHome, UsageHome};
pub use tail::{TailReader, TranscriptTail, TranscriptTailReading};
pub use view::UsageView;
pub use window::{LocalZone, ProviderPercent, WindowNormalizing, WindowObservation, WindowReading};

/// A source of subscription quota observations for one provider.
pub trait QuotaSource {
    fn observe_quota(&self, instant: ObservationInstant) -> Vec<SubscriptionObservation>;
}

/// A source of live-session context observations for one provider.
pub trait ContextSource {
    fn observe_context(&self, instant: ObservationInstant) -> Vec<SessionContextObservation>;
}

/// A recorded provider quota document that normalizes into the contract at
/// one observation instant, rendering resets in one zone.
pub trait QuotaDocument {
    fn normalize(&self, instant: ObservationInstant, zone: &LocalZone) -> SubscriptionUsage;
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

/// What one collector thread returned, or that it failed outright.
enum CollectorOutcome<Observation> {
    Returned(Vec<Observation>),
    Failed,
}

impl UsageSnapshotReader {
    /// The production reader: the fixed Claude endpoint, the home's own
    /// provider directories, and the host's configured time zone.
    pub fn for_home(home: UsageHome) -> Self {
        Self::with_endpoint(home, ClaudeUsageEndpoint::anthropic(), LocalZone::system())
    }

    /// A reader against another Claude endpoint and zone, for
    /// recorded-provider tests.
    pub fn with_endpoint(home: UsageHome, endpoint: ClaudeUsageEndpoint, zone: LocalZone) -> Self {
        Self {
            claude_quota: ClaudeUsageSource::new(home.clone(), endpoint, zone.clone()),
            codex_quota: CodexUsageSource::new(home.clone(), AppServerTimeout::default(), zone),
            claude_context: ClaudeLiveSessions::new(home.clone()),
            codex_context: CodexLiveThreads::new(home, AppServerTimeout::default()),
        }
    }

    /// The snapshot a daemon answers when its reader could not run at all:
    /// every collector is reported failed.
    pub fn failed_snapshot() -> UsageSnapshot {
        let instant = ObservationInstant::now();
        UsageSnapshot {
            snapshot_time: instant.nanoseconds(),
            planning_projection: PlanningProjection::NotConfigured,
            subscription_observations: [UsageProvider::Claude, UsageProvider::Codex]
                .into_iter()
                .map(|provider| Self::quota_failed(provider, instant))
                .collect(),
            session_context_observations: [UsageProvider::Claude, UsageProvider::Codex]
                .into_iter()
                .map(|provider| Self::context_failed(provider, instant))
                .collect(),
        }
    }

    fn quota_failed(
        provider: UsageProvider,
        instant: ObservationInstant,
    ) -> SubscriptionObservation {
        SubscriptionObservation::Unavailable(UsageUnavailable {
            usage_provider: provider,
            observation_time: instant.nanoseconds(),
            account_home_option: None,
            usage_unavailable_reason: UsageUnavailableReason::CollectorFailed,
        })
    }

    fn context_failed(
        provider: UsageProvider,
        instant: ObservationInstant,
    ) -> SessionContextObservation {
        SessionContextObservation::SourceUnavailable(ContextSourceUnavailable {
            usage_provider: provider,
            account_home_option: None,
            observation_time: instant.nanoseconds(),
            context_source_failure_reason: ContextSourceFailureReason::CollectorFailed,
        })
    }
}

impl<Observation> CollectorOutcome<Observation> {
    fn joined(joined: std::thread::Result<Vec<Observation>>) -> Self {
        match joined {
            Ok(observations) => Self::Returned(observations),
            Err(_) => Self::Failed,
        }
    }

    fn or_failed(self, failed: impl FnOnce() -> Observation) -> Vec<Observation> {
        match self {
            Self::Returned(observations) => observations,
            Self::Failed => vec![failed()],
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
                    CollectorOutcome::joined(claude_quota.join()),
                    CollectorOutcome::joined(codex_quota.join()),
                    CollectorOutcome::joined(claude_context.join()),
                    CollectorOutcome::joined(codex_context.join()),
                )
            });
        let failed_at = ObservationInstant::now();
        UsageSnapshot {
            snapshot_time: snapshot_time.nanoseconds(),
            planning_projection: PlanningProjection::NotConfigured,
            subscription_observations: claude_quota
                .or_failed(|| Self::quota_failed(UsageProvider::Claude, failed_at))
                .into_iter()
                .chain(
                    codex_quota.or_failed(|| Self::quota_failed(UsageProvider::Codex, failed_at)),
                )
                .collect(),
            session_context_observations: claude_context
                .or_failed(|| Self::context_failed(UsageProvider::Claude, failed_at))
                .into_iter()
                .chain(
                    codex_context
                        .or_failed(|| Self::context_failed(UsageProvider::Codex, failed_at)),
                )
                .collect(),
        }
    }
}
