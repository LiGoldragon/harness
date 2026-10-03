//! Claude subscription quota from the OAuth usage endpoint.
//!
//! The access token is read from the local credential file inside this module,
//! checked for expiry, and placed only in the request's `Authorization` header.
//! An expired token is reported, never refreshed: a refresh rotates the
//! refresh token and would race Claude Code's own writes to the file.

use std::io::Read;
use std::time::Duration;

use serde_json::Value;
use usage_contract::{
    QuotaLimit, ResetBasis, SubscriptionObservation, SubscriptionUsage, UsageProvider, UsageSource,
    UsageUnavailable, UsageUnavailableReason, WindowDurationBasis,
};

use super::pace::{WindowNormalizing, WindowReading};
use super::{ObservationInstant, QuotaDocument, QuotaSource, UsageHome};

const ANTHROPIC_USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";
const ANTHROPIC_BETA: &str = "oauth-2025-04-20";
const CREDENTIAL_BYTE_LIMIT: u64 = 64 * 1024;
const RESPONSE_BYTE_LIMIT: u64 = 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const EXPIRY_MARGIN_MILLISECONDS: i64 = 60_000;
const FIVE_HOUR_MINUTES: i64 = 300;
const SEVEN_DAY_MINUTES: i64 = 10_080;
/// Top-level keys that are understood; any other non-null key is retained by
/// name as an unrecognized window.
const RECOGNIZED_KEYS: [&str; 7] = [
    "five_hour",
    "seven_day",
    "limits",
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
    pub fn new(home: UsageHome, endpoint: ClaudeUsageEndpoint) -> Self {
        Self { home, endpoint }
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
            .map(|document| document.normalize(ObservationInstant::now()));
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

    fn basis_points(value: Option<&Value>) -> Option<i64> {
        let percent = value?.as_f64()?;
        percent
            .is_finite()
            .then(|| (percent * 100.0).round() as i64)
    }

    fn named_window_second(&self, key: &str) -> Option<i64> {
        Self::reset_second(self.body.get(key)?.get("resets_at"))
    }

    /// The window length implied by the provider's own names: the `weekly`
    /// group, or the reset shared with the `five_hour` / `seven_day` window.
    fn named_duration(&self, group: &str, reset: Option<i64>) -> WindowDurationBasis {
        if group == "weekly" {
            return WindowDurationBasis::ProviderWindowNamed(SEVEN_DAY_MINUTES);
        }
        match reset {
            Some(second) if Some(second) == self.named_window_second("five_hour") => {
                WindowDurationBasis::ProviderWindowNamed(FIVE_HOUR_MINUTES)
            }
            Some(second) if Some(second) == self.named_window_second("seven_day") => {
                WindowDurationBasis::ProviderWindowNamed(SEVEN_DAY_MINUTES)
            }
            _ => WindowDurationBasis::Unknown,
        }
    }

    fn reset_basis(second: Option<i64>) -> ResetBasis {
        second.map_or(ResetBasis::Unknown, ResetBasis::ProviderResetTime)
    }

    /// Group the normalized `limits[]` list into limits by provider group.
    fn listed_limits(
        &self,
        entries: &[Value],
        observed: i64,
        unrecognized: &mut Vec<String>,
    ) -> Vec<QuotaLimit> {
        let mut limits: Vec<QuotaLimit> = Vec::new();
        for entry in entries {
            let kind = entry.get("kind").and_then(Value::as_str).unwrap_or("limit");
            let group = entry.get("group").and_then(Value::as_str).unwrap_or(kind);
            let Some(used) = Self::basis_points(entry.get("percent")) else {
                unrecognized.push(format!("limits.{kind}"));
                continue;
            };
            let reset = Self::reset_second(entry.get("resets_at"));
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
                used_basis_points: used,
                reset_basis: Self::reset_basis(reset),
                duration_basis: self.named_duration(group, reset),
            }
            .normalize_at(observed);
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

    /// Without `limits[]`, the two named top-level windows stand alone.
    fn named_limits(&self, observed: i64) -> Vec<QuotaLimit> {
        [
            ("five_hour", FIVE_HOUR_MINUTES),
            ("seven_day", SEVEN_DAY_MINUTES),
        ]
        .into_iter()
        .filter_map(|(key, minutes)| {
            let window = self.body.get(key)?;
            let used = Self::basis_points(window.get("utilization"))?;
            Some(QuotaLimit {
                quota_limit_identifier: key.to_owned(),
                quota_limit_name_option: None,
                quota_windows: vec![
                    WindowReading {
                        name: key.to_owned(),
                        scope: None,
                        used_basis_points: used,
                        reset_basis: Self::reset_basis(Self::reset_second(window.get("resets_at"))),
                        duration_basis: WindowDurationBasis::ProviderWindowNamed(minutes),
                    }
                    .normalize_at(observed),
                ],
            })
        })
        .collect()
    }
}

impl QuotaDocument for ClaudeUsageDocument {
    fn normalize(&self, instant: ObservationInstant) -> SubscriptionUsage {
        let observed = instant.seconds();
        let mut unrecognized: Vec<String> = self
            .body
            .as_object()
            .into_iter()
            .flatten()
            .filter(|(key, value)| !value.is_null() && !RECOGNIZED_KEYS.contains(&key.as_str()))
            .map(|(key, _)| key.clone())
            .collect();
        let quota_limits = match self.body.get("limits").and_then(Value::as_array) {
            Some(entries) if !entries.is_empty() => {
                self.listed_limits(entries, observed, &mut unrecognized)
            }
            _ => self.named_limits(observed),
        };
        SubscriptionUsage {
            usage_provider: UsageProvider::Claude,
            usage_source: UsageSource::ClaudeOauthUsageEndpoint,
            observation_time: instant.nanoseconds(),
            plan_name_option: self.plan.clone(),
            account_homes: Vec::new(),
            quota_limits,
            unrecognized_window_names: unrecognized,
        }
    }
}
