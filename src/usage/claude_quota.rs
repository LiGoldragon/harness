//! Claude subscription quota from the OAuth usage endpoint.
//!
//! The access token is read from the local credential file inside this module,
//! checked for expiry, and placed only in the request's `Authorization` header.
//! An expired token is reported, never refreshed: a refresh rotates the
//! refresh token and would race Claude Code's own writes to the file.

use std::io::Read;
use std::time::Duration;

use serde_json::Value;
use signal_harness::{
    PeriodSemantics, QuotaLimit, ResetBasis, SubscriptionObservation, SubscriptionUsage,
    UsageProvider, UsageSource, UsageUnavailable, UsageUnavailableReason, WindowDurationBasis,
};

use super::window::{
    LocalZone, ProviderPercent, WindowNormalizing, WindowObservation, WindowReading,
};
use super::{ObservationInstant, QuotaDocument, QuotaSource, UsageHome};

const ANTHROPIC_USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";
const ANTHROPIC_BETA: &str = "oauth-2025-04-20";
const CREDENTIAL_BYTE_LIMIT: u64 = 64 * 1024;
const RESPONSE_BYTE_LIMIT: u64 = 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const EXPIRY_MARGIN_MILLISECONDS: i64 = 60_000;
/// The provider's own named windows and the durations their names state.
const NAMED_WINDOWS: [(&str, i64); 2] = [("five_hour", 300), ("seven_day", 10_080)];
const LIMITS_KEY: &str = "limits";
/// Keys modelled inside a named window; any other present key is an
/// unmodeled source fact.
const NAMED_WINDOW_KEYS: [&str; 2] = ["utilization", "resets_at"];
/// Keys modelled inside a `limits[]` entry; any other present key is an
/// unmodeled source fact.
const LIMIT_ENTRY_KEYS: [&str; 5] = ["kind", "group", "percent", "resets_at", "scope"];
/// Top-level allowance, spend and account facts the reply names without
/// modelling and never turns into a quota window.
const AUXILIARY_KEYS: [&str; 4] = [
    "extra_usage",
    "spend",
    "seven_day_breakdown",
    "member_dashboard_available",
];

/// The endpoint the usage request goes to. Production uses the fixed Anthropic
/// URL; another URL exists only for recorded-provider tests and is never read
/// from the environment or the command line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaudeUsageEndpoint {
    url: String,
}

impl ClaudeUsageEndpoint {
    pub fn anthropic() -> Self {
        Self {
            url: ANTHROPIC_USAGE_URL.to_owned(),
        }
    }

    pub fn recorded(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

/// The Claude quota source bound to one home's credential file.
#[derive(Clone, Debug)]
pub struct ClaudeUsageSource {
    home: UsageHome,
    endpoint: ClaudeUsageEndpoint,
    zone: LocalZone,
}

/// A Claude usage response body with the plan named by the credential file.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaudeUsageDocument {
    body: Value,
    plan: Option<String>,
}

/// The bearer secret. It has no `Display` and its `Debug` is redacted.
struct AccessToken(String);

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AccessToken(redacted)")
    }
}

struct ClaudeCredential {
    token: AccessToken,
    plan: Option<String>,
}

impl ClaudeUsageSource {
    pub fn new(home: UsageHome, endpoint: ClaudeUsageEndpoint, zone: LocalZone) -> Self {
        Self {
            home,
            endpoint,
            zone,
        }
    }

    fn credential(
        &self,
        instant: ObservationInstant,
    ) -> Result<ClaudeCredential, UsageUnavailableReason> {
        let file =
            std::fs::File::open(self.home.claude_credentials()).map_err(|error| {
                match error.kind() {
                    std::io::ErrorKind::NotFound => UsageUnavailableReason::CredentialsAbsent,
                    _ => UsageUnavailableReason::CredentialsUnreadable,
                }
            })?;
        let mut text = String::new();
        file.take(CREDENTIAL_BYTE_LIMIT)
            .read_to_string(&mut text)
            .map_err(|_| UsageUnavailableReason::CredentialsUnreadable)?;
        let value: Value = serde_json::from_str(&text)
            .map_err(|_| UsageUnavailableReason::CredentialsUnreadable)?;
        let oauth = value
            .get("claudeAiOauth")
            .ok_or(UsageUnavailableReason::CredentialsAbsent)?;
        let token = oauth
            .get("accessToken")
            .and_then(Value::as_str)
            .filter(|token| !token.is_empty())
            .ok_or(UsageUnavailableReason::CredentialsAbsent)?;
        let expires = oauth
            .get("expiresAt")
            .and_then(Value::as_i64)
            .ok_or(UsageUnavailableReason::CredentialsUnreadable)?;
        if expires <= instant.nanoseconds() / 1_000_000 + EXPIRY_MARGIN_MILLISECONDS {
            return Err(UsageUnavailableReason::AccessTokenExpired);
        }
        Ok(ClaudeCredential {
            token: AccessToken(token.to_owned()),
            plan: oauth
                .get("subscriptionType")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    }

    fn fetch(
        &self,
        credential: ClaudeCredential,
    ) -> Result<ClaudeUsageDocument, UsageUnavailableReason> {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(REQUEST_TIMEOUT))
            .http_status_as_error(false)
            .build()
            .into();
        let authorization = format!("Bearer {}", credential.token.0);
        let mut response = agent
            .get(&self.endpoint.url)
            .header("Authorization", &authorization)
            .header("anthropic-beta", ANTHROPIC_BETA)
            .call()
            .map_err(|error| match error {
                ureq::Error::Timeout(_) => UsageUnavailableReason::TransportTimedOut,
                _ => UsageUnavailableReason::TransportFailed,
            })?;
        drop(authorization);
        let status = response.status().as_u16();
        if (400..500).contains(&status) {
            return Err(UsageUnavailableReason::ProviderRejected);
        }
        if status != 200 {
            return Err(UsageUnavailableReason::TransportFailed);
        }
        let body = response
            .body_mut()
            .with_config()
            .limit(RESPONSE_BYTE_LIMIT)
            .read_to_string()
            .map_err(|error| match error {
                ureq::Error::Timeout(_) => UsageUnavailableReason::TransportTimedOut,
                _ => UsageUnavailableReason::ProviderResponseUnreadable,
            })?;
        ClaudeUsageDocument::from_body(&body, credential.plan)
    }
}

impl QuotaSource for ClaudeUsageSource {
    fn observe_quota(&self, instant: ObservationInstant) -> Vec<SubscriptionObservation> {
        let observation = self
            .credential(instant)
            .and_then(|credential| self.fetch(credential))
            .map(|document| document.normalize(ObservationInstant::now(), &self.zone));
        vec![match observation {
            Ok(usage) => SubscriptionObservation::Observed(usage),
            Err(reason) => SubscriptionObservation::Unavailable(UsageUnavailable {
                usage_provider: UsageProvider::Claude,
                observation_time: instant.nanoseconds(),
                account_home_option: None,
                usage_unavailable_reason: reason,
            }),
        }]
    }
}

impl ClaudeUsageDocument {
    /// Parse a response body; anything but a JSON object is unreadable.
    pub fn from_body(body: &str, plan: Option<String>) -> Result<Self, UsageUnavailableReason> {
        let body: Value = serde_json::from_str(body)
            .map_err(|_| UsageUnavailableReason::ProviderResponseUnreadable)?;
        if !body.is_object() {
            return Err(UsageUnavailableReason::ProviderResponseUnreadable);
        }
        Ok(Self { body, plan })
    }

    fn reset_second(value: Option<&Value>) -> Option<i64> {
        let text = value?.as_str()?;
        let parsed =
            time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
                .ok()?;
        Some(parsed.unix_timestamp())
    }

    fn reset_basis(value: Option<&Value>) -> ResetBasis {
        Self::reset_second(value).map_or(ResetBasis::Unknown, ResetBasis::ProviderResetTime)
    }

    fn percent(value: Option<&Value>) -> ProviderPercent {
        value
            .and_then(Value::as_f64)
            .map_or(ProviderPercent::Absent, ProviderPercent::Present)
    }

    /// The present (non-null) keys of an object that are not modelled.
    fn unmodeled_keys<'a>(value: &'a Value, modelled: &'a [&str]) -> Vec<&'a str> {
        value
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(key, value)| !value.is_null() && !modelled.contains(&key.as_str()))
            .map(|(key, _)| key.as_str())
            .collect()
    }

    /// The provider's named top-level windows, each its own limit, with the
    /// duration its name states.
    fn named_limits(
        &self,
        observation: &WindowObservation,
        facts: &mut Vec<String>,
    ) -> Vec<QuotaLimit> {
        NAMED_WINDOWS
            .into_iter()
            .filter_map(|(key, minutes)| {
                let window = self.body.get(key).filter(|window| !window.is_null())?;
                facts.extend(
                    Self::unmodeled_keys(window, &NAMED_WINDOW_KEYS)
                        .into_iter()
                        .map(|fact| format!("{key}.{fact}")),
                );
                Some(QuotaLimit {
                    quota_limit_identifier: key.to_owned(),
                    quota_limit_name_option: None,
                    quota_windows: vec![
                        WindowReading {
                            name: key.to_owned(),
                            scope: None,
                            used: Self::percent(window.get("utilization")),
                            reset_basis: Self::reset_basis(window.get("resets_at")),
                            duration_basis: WindowDurationBasis::ProviderWindowNamed(minutes),
                            period_semantics: PeriodSemantics::NotEstablished,
                        }
                        .normalize_at(observation),
                    ],
                })
            })
            .collect()
    }

    /// The `limits[]` list grouped into limits by provider group. An entry
    /// carries a reset but no duration; a duration is never inferred from a
    /// reset shared with a named window.
    fn listed_limits(
        &self,
        observation: &WindowObservation,
        facts: &mut Vec<String>,
    ) -> Vec<QuotaLimit> {
        let mut limits: Vec<QuotaLimit> = Vec::new();
        let entries = self
            .body
            .get(LIMITS_KEY)
            .and_then(Value::as_array)
            .into_iter()
            .flatten();
        for entry in entries {
            let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("limit");
            let group = entry.get("group").and_then(Value::as_str).unwrap_or(kind);
            facts.extend(
                Self::unmodeled_keys(entry, &LIMIT_ENTRY_KEYS)
                    .into_iter()
                    .map(|fact| format!("{LIMITS_KEY}.{kind}.{fact}")),
            );
            let scope = entry.get("scope");
            let scope_name = scope
                .and_then(|scope| scope.get("model"))
                .and_then(|model| model.get("display_name").or_else(|| model.get("id")))
                .or_else(|| scope.and_then(|scope| scope.get("surface")))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let window = WindowReading {
                name: kind.to_owned(),
                scope: scope_name,
                used: Self::percent(entry.get("percent")),
                reset_basis: Self::reset_basis(entry.get("resets_at")),
                duration_basis: WindowDurationBasis::Unknown,
                period_semantics: PeriodSemantics::NotEstablished,
            }
            .normalize_at(observation);
            match limits
                .iter_mut()
                .find(|limit| limit.quota_limit_identifier == group)
            {
                Some(limit) => limit.quota_windows.push(window),
                None => limits.push(QuotaLimit {
                    quota_limit_identifier: group.to_owned(),
                    quota_limit_name_option: None,
                    quota_windows: vec![window],
                }),
            }
        }
        limits
    }

    /// Present top-level keys outside the modelled windows: a window-shaped
    /// object (one with a `utilization`) is an unrecognized window; anything
    /// else, including the known auxiliary facts, is an unmodeled fact.
    fn unrecognized(&self, facts: &mut Vec<String>) -> Vec<String> {
        let mut windows = Vec::new();
        let modelled: Vec<&str> = NAMED_WINDOWS
            .iter()
            .map(|(key, _)| *key)
            .chain([LIMITS_KEY])
            .collect();
        for key in Self::unmodeled_keys(&self.body, &modelled) {
            let window_shaped = self
                .body
                .get(key)
                .is_some_and(|value| value.get("utilization").is_some());
            if window_shaped && !AUXILIARY_KEYS.contains(&key) {
                windows.push(key.to_owned());
            } else {
                facts.push(key.to_owned());
            }
        }
        windows
    }
}

impl QuotaDocument for ClaudeUsageDocument {
    fn normalize(&self, instant: ObservationInstant, zone: &LocalZone) -> SubscriptionUsage {
        let observation = WindowObservation::new(instant.seconds(), zone.clone());
        let mut facts = Vec::new();
        let unrecognized = self.unrecognized(&mut facts);
        let quota_limits = self
            .named_limits(&observation, &mut facts)
            .into_iter()
            .chain(self.listed_limits(&observation, &mut facts))
            .collect();
        SubscriptionUsage {
            usage_provider: UsageProvider::Claude,
            usage_source: UsageSource::ClaudeOauthUsageEndpoint,
            observation_time: instant.nanoseconds(),
            plan_name_option: self.plan.clone(),
            account_homes: Vec::new(),
            quota_limits,
            unrecognized_window_names: unrecognized,
            unmodeled_source_facts: facts,
        }
    }
}
