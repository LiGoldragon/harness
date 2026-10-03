//! Codex subscription quota from each live app-server's
//! `account/rateLimits/read`.
//!
//! Every Codex home with a ChatGPT login is asked through its own control
//! socket; the app-server reads its own login, so no credential passes through
//! here. Homes that answer for the same account are one subscription: the
//! reply lists them together under that account.

use serde_json::Value;
use usage_contract::{
    QuotaLimit, ResetBasis, SubscriptionObservation, SubscriptionUsage, UsageProvider, UsageSource,
    UsageUnavailable, UsageUnavailableReason, WindowDurationBasis,
};

use super::app_server::{AppServerFailure, AppServerSession, AppServerTimeout, JsonRpcExchange};
use super::pace::{WindowNormalizing, WindowReading};
use super::{CodexHome, ObservationInstant, QuotaDocument, QuotaSource, UsageHome};

const SERVER_WINDOWS: [&str; 2] = ["primary", "secondary"];
/// Per-limit keys that are understood; any other non-null key is retained by
/// name as an unrecognized window.
const RECOGNIZED_LIMIT_KEYS: [&str; 9] = [
    "limitId",
    "limitName",
    "normalModelSlug",
    "primary",
    "secondary",
    "credits",
    "spendControlReached",
    "planType",
    "rateLimitReachedType",
];

/// The Codex quota source over every Codex home under one home directory.
#[derive(Clone, Debug)]
pub struct CodexUsageSource {
    home: UsageHome,
    timeout: AppServerTimeout,
}

/// One `account/rateLimits/read` result, with the homes that returned it.
#[derive(Clone, Debug, PartialEq)]
pub struct CodexRateLimitsDocument {
    result: Value,
    homes: Vec<String>,
}

/// What one Codex home answered.
enum HomeAnswer {
    Answered(CodexRateLimitsDocument),
    Failed {
        home: String,
        failure: AppServerFailure,
    },
}

impl CodexUsageSource {
    pub fn new(home: UsageHome, timeout: AppServerTimeout) -> Self {
        Self { home, timeout }
    }

    fn ask(&self, home: &CodexHome) -> HomeAnswer {
        let mut failure = AppServerFailure::Absent;
        for socket in home.control_sockets() {
            let result = AppServerSession::open(&socket, self.timeout)
                .and_then(|mut session| session.request("account/rateLimits/read", None));
            match result {
                Ok(result) => {
                    return HomeAnswer::Answered(CodexRateLimitsDocument::new(
                        result,
                        vec![home.name().to_owned()],
                    ));
                }
                Err(error) => failure = error,
            }
        }
        HomeAnswer::Failed {
            home: home.name().to_owned(),
            failure,
        }
    }
}

impl QuotaSource for CodexUsageSource {
    fn observe_quota(&self, instant: ObservationInstant) -> Vec<SubscriptionObservation> {
        let homes = self.home.codex_homes();
        if homes.is_empty() {
            return vec![SubscriptionObservation::Unavailable(UsageUnavailable {
                usage_provider: UsageProvider::Codex,
                observation_time: instant.nanoseconds(),
                account_home_option: None,
                usage_unavailable_reason: UsageUnavailableReason::NoLiveControlSocket,
            })];
        }
        let answers: Vec<HomeAnswer> = std::thread::scope(|scope| {
            let asked: Vec<_> = homes
                .iter()
                .map(|home| scope.spawn(move || self.ask(home)))
                .collect();
            asked
                .into_iter()
                .zip(&homes)
                .map(|(handle, home)| {
                    handle.join().unwrap_or(HomeAnswer::Failed {
                        home: home.name().to_owned(),
                        failure: AppServerFailure::TransportFailed,
                    })
                })
                .collect()
        });
        let mut accounts: Vec<CodexRateLimitsDocument> = Vec::new();
        let mut failures = Vec::new();
        for answer in answers {
            match answer {
                HomeAnswer::Answered(document) => {
                    match accounts
                        .iter_mut()
                        .find(|known| known.same_account(&document))
                    {
                        Some(known) => known.homes.extend(document.homes),
                        None => accounts.push(document),
                    }
                }
                HomeAnswer::Failed { home, failure } => {
                    failures.push(SubscriptionObservation::Unavailable(UsageUnavailable {
                        usage_provider: UsageProvider::Codex,
                        observation_time: instant.nanoseconds(),
                        account_home_option: Some(home),
                        usage_unavailable_reason: failure.into(),
                    }));
                }
            }
        }
        let observed = ObservationInstant::now();
        accounts
            .iter()
            .map(|document| SubscriptionObservation::Observed(document.normalize(observed)))
            .chain(failures)
            .collect()
    }
}

impl From<AppServerFailure> for UsageUnavailableReason {
    fn from(failure: AppServerFailure) -> Self {
        match failure {
            AppServerFailure::Absent => Self::NoLiveControlSocket,
            AppServerFailure::TimedOut => Self::TransportTimedOut,
            AppServerFailure::TransportFailed => Self::TransportFailed,
            AppServerFailure::Rejected => Self::ProviderRejected,
            AppServerFailure::Unreadable => Self::ProviderResponseUnreadable,
        }
    }
}

impl CodexRateLimitsDocument {
    pub fn new(result: Value, homes: Vec<String>) -> Self {
        Self { result, homes }
    }

    /// The account the server answered for; used only to merge homes, never
    /// emitted.
    fn account(&self) -> Option<&str> {
        self.result.get("accountId").and_then(Value::as_str)
    }

    fn same_account(&self, other: &Self) -> bool {
        matches!((self.account(), other.account()), (Some(mine), Some(theirs)) if mine == theirs)
    }

    /// Every limit the server returned: `rateLimitsByLimitId` when present,
    /// else the single `rateLimits` bucket.
    fn limit_buckets(&self) -> Vec<&Value> {
        match self
            .result
            .get("rateLimitsByLimitId")
            .and_then(Value::as_object)
        {
            Some(map) if !map.is_empty() => map.values().collect(),
            _ => self.result.get("rateLimits").into_iter().collect(),
        }
    }

    fn window(bucket: &Value, name: &str, observed: i64) -> Option<usage_contract::QuotaWindow> {
        let window = bucket.get(name).filter(|window| !window.is_null())?;
        let used = window.get("usedPercent").and_then(Value::as_f64)?;
        Some(
            WindowReading {
                name: name.to_owned(),
                scope: None,
                used_basis_points: (used * 100.0).round() as i64,
                reset_basis: window
                    .get("resetsAt")
                    .and_then(Value::as_i64)
                    .map_or(ResetBasis::Unknown, ResetBasis::ProviderResetTime),
                duration_basis: window
                    .get("windowDurationMins")
                    .and_then(Value::as_i64)
                    .map_or(
                        WindowDurationBasis::Unknown,
                        WindowDurationBasis::ProviderDeclared,
                    ),
            }
            .normalize_at(observed),
        )
    }
}

impl QuotaDocument for CodexRateLimitsDocument {
    fn normalize(&self, instant: ObservationInstant) -> SubscriptionUsage {
        let observed = instant.seconds();
        let mut unrecognized = Vec::new();
        let mut plan = None;
        let quota_limits = self
            .limit_buckets()
            .into_iter()
            .map(|bucket| {
                let identifier = bucket
                    .get("limitId")
                    .and_then(Value::as_str)
                    .unwrap_or("rateLimits")
                    .to_owned();
                plan = plan.take().or_else(|| {
                    bucket
                        .get("planType")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                });
                for (key, value) in bucket.as_object().into_iter().flatten() {
                    if !value.is_null() && !RECOGNIZED_LIMIT_KEYS.contains(&key.as_str()) {
                        unrecognized.push(format!("{identifier}.{key}"));
                    }
                }
                QuotaLimit {
                    quota_limit_name_option: bucket
                        .get("limitName")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    quota_windows: SERVER_WINDOWS
                        .iter()
                        .filter_map(|name| Self::window(bucket, name, observed))
                        .collect(),
                    quota_limit_identifier: identifier,
                }
            })
            .collect();
        SubscriptionUsage {
            usage_provider: UsageProvider::Codex,
            usage_source: UsageSource::CodexAppServerRateLimits,
            observation_time: instant.nanoseconds(),
            plan_name_option: plan,
            account_homes: self.homes.clone(),
            quota_limits,
            unrecognized_window_names: unrecognized,
        }
    }
}
