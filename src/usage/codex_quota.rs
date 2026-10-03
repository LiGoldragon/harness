//! Codex subscription quota from each live app-server's
//! `account/rateLimits/read`.
//!
//! Every Codex home with a ChatGPT login is asked through its own control
//! socket; the app-server reads its own login, so no credential passes through
//! here. Homes that answer for the same account are one subscription: the
//! reply lists them together under that account.

use serde_json::Value;
use signal_harness::{
    PeriodSemantics, QuotaLimit, QuotaWindow, ResetBasis, SubscriptionObservation,
    SubscriptionUsage, UsageProvider, UsageSource, UsageUnavailable, UsageUnavailableReason,
    WindowDurationBasis,
};

use super::app_server::{AppServerFailure, AppServerSession, AppServerTimeout, JsonRpcExchange};
use super::window::{
    LocalZone, ProviderPercent, WindowNormalizing, WindowObservation, WindowReading,
};
use super::{CodexHome, ObservationInstant, QuotaDocument, QuotaSource, UsageHome};

const SERVER_WINDOWS: [&str; 2] = ["primary", "secondary"];
/// Per-limit keys that are modelled; any other present key is an
/// unrecognized window when it is window-shaped (it has a `usedPercent`) and
/// an unmodeled source fact otherwise (credits, spend control, the reached
/// limit type and the like).
const MODELLED_LIMIT_KEYS: [&str; 5] = ["limitId", "limitName", "primary", "secondary", "planType"];
/// Top-level result keys that are modelled or deliberately withheld; any
/// other present key is an unmodeled source fact. The account identifier is
/// read only to merge homes and never named in a reply.
const MODELLED_RESULT_KEYS: [&str; 3] = ["rateLimits", "rateLimitsByLimitId", "accountId"];

/// The Codex quota source over every Codex home under one home directory.
#[derive(Clone, Debug)]
pub struct CodexUsageSource {
    home: UsageHome,
    timeout: AppServerTimeout,
    zone: LocalZone,
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
    CollectorFailed {
        home: String,
    },
}

impl CodexUsageSource {
    pub fn new(home: UsageHome, timeout: AppServerTimeout, zone: LocalZone) -> Self {
        Self {
            home,
            timeout,
            zone,
        }
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
                    handle.join().unwrap_or(HomeAnswer::CollectorFailed {
                        home: home.name().to_owned(),
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
                    failures.push(Self::unavailable(instant, home, failure.into()));
                }
                HomeAnswer::CollectorFailed { home } => {
                    failures.push(Self::unavailable(
                        instant,
                        home,
                        UsageUnavailableReason::CollectorFailed,
                    ));
                }
            }
        }
        let observed = ObservationInstant::now();
        accounts
            .iter()
            .map(|document| {
                SubscriptionObservation::Observed(document.normalize(observed, &self.zone))
            })
            .chain(failures)
            .collect()
    }
}

// Exception (too trivial): the private constructor of one home's failure.
impl CodexUsageSource {
    fn unavailable(
        instant: ObservationInstant,
        home: String,
        reason: UsageUnavailableReason,
    ) -> SubscriptionObservation {
        SubscriptionObservation::Unavailable(UsageUnavailable {
            usage_provider: UsageProvider::Codex,
            observation_time: instant.nanoseconds(),
            account_home_option: Some(home),
            usage_unavailable_reason: reason,
        })
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

    fn window(bucket: &Value, name: &str, observation: &WindowObservation) -> Option<QuotaWindow> {
        let window = bucket.get(name).filter(|window| !window.is_null())?;
        Some(
            WindowReading {
                name: name.to_owned(),
                scope: None,
                used: window
                    .get("usedPercent")
                    .and_then(Value::as_f64)
                    .map_or(ProviderPercent::Absent, ProviderPercent::Present),
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
                period_semantics: PeriodSemantics::NotEstablished,
            }
            .normalize_at(observation),
        )
    }

    /// The present keys of an object outside the modelled ones.
    fn present_keys<'a>(value: &'a Value, modelled: &'a [&str]) -> Vec<(&'a str, &'a Value)> {
        value
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(key, value)| !value.is_null() && !modelled.contains(&key.as_str()))
            .map(|(key, value)| (key.as_str(), value))
            .collect()
    }
}

impl QuotaDocument for CodexRateLimitsDocument {
    fn normalize(&self, instant: ObservationInstant, zone: &LocalZone) -> SubscriptionUsage {
        let observation = WindowObservation::new(instant.seconds(), zone.clone());
        let mut unrecognized = Vec::new();
        let mut facts: Vec<String> = Self::present_keys(&self.result, &MODELLED_RESULT_KEYS)
            .into_iter()
            .map(|(key, _)| key.to_owned())
            .collect();
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
                for (key, value) in Self::present_keys(bucket, &MODELLED_LIMIT_KEYS) {
                    if value.get("usedPercent").is_some() {
                        unrecognized.push(format!("{identifier}.{key}"));
                    } else {
                        facts.push(format!("{identifier}.{key}"));
                    }
                }
                QuotaLimit {
                    quota_limit_name_option: bucket
                        .get("limitName")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    quota_windows: SERVER_WINDOWS
                        .iter()
                        .filter_map(|name| Self::window(bucket, name, &observation))
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
            unmodeled_source_facts: facts,
        }
    }
}
