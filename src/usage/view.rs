//! The human view of one usage snapshot.
//!
//! Each window leads with its remaining share, the time left until its
//! reset, the reset in local time, and the rate to use the remainder by that
//! reset. Rates are computed here from the reply's operands and rounded only
//! for display. A passed reset shows its old values as stale; an unknown
//! operand says so; nothing is shown as zero that the reply does not hold.

use std::fmt::Write;

use signal_harness::{
    ContextSourceUnavailable, ElapsedWindowDerivation, LocalReset, PlanningProjection, QuotaLimit,
    QuotaWindow, RemainderRateDerivation, ResetCountdown, SessionContext,
    SessionContextObservation, SessionContextUnavailable, SubscriptionObservation,
    SubscriptionUsage, UniformRateDerivation, UsageProvider, UsageSnapshot, UsageUnavailable,
    WindowDurationBasis, WindowUsage,
};

const BASIS_POINTS_PER_PERCENT: f64 = 100.0;
const SECONDS_PER_MINUTE: i64 = 60;
const SECONDS_PER_HOUR: i64 = 3_600;
const SECONDS_PER_DAY: i64 = 86_400;
const MINUTES_PER_DAY: f64 = 1_440.0;

/// One snapshot, rendered for reading.
#[derive(Clone, Debug, PartialEq)]
pub struct UsageView {
    snapshot: UsageSnapshot,
}

/// A typed reason, spoken as lower-case words.
struct Spoken<'a, Reason: std::fmt::Debug>(&'a Reason);

/// A span of seconds, spoken in its two largest units.
struct Span(i64);

/// A share in basis points, spoken as a percentage.
struct Percent(i64);

impl<Reason: std::fmt::Debug> std::fmt::Display for Spoken<'_, Reason> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = format!("{:?}", self.0);
        let name = name.split(['(', ' ']).next().unwrap_or_default();
        let mut words = String::new();
        for (index, character) in name.char_indices() {
            if character.is_uppercase() && index > 0 {
                words.push(' ');
            }
            words.extend(character.to_lowercase());
        }
        formatter.write_str(&words)
    }
}

impl std::fmt::Display for Span {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let seconds = self.0;
        let days = seconds / SECONDS_PER_DAY;
        let hours = seconds % SECONDS_PER_DAY / SECONDS_PER_HOUR;
        let minutes = seconds % SECONDS_PER_HOUR / SECONDS_PER_MINUTE;
        match (days, hours, minutes) {
            (0, 0, 0) => write!(formatter, "{seconds}s"),
            (0, 0, minutes) => write!(formatter, "{minutes}m"),
            (0, hours, minutes) => write!(formatter, "{hours}h {minutes}m"),
            (days, hours, _) => write!(formatter, "{days}d {hours}h"),
        }
    }
}

impl std::fmt::Display for Percent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0 % 100 == 0 {
            write!(formatter, "{}%", self.0 / 100)
        } else {
            write!(
                formatter,
                "{:.2}%",
                self.0 as f64 / BASIS_POINTS_PER_PERCENT
            )
        }
    }
}

impl UsageView {
    pub fn new(snapshot: UsageSnapshot) -> Self {
        Self { snapshot }
    }

    pub fn render(&self) -> String {
        let mut text = String::new();
        for observation in &self.snapshot.subscription_observations {
            match observation {
                SubscriptionObservation::Observed(usage) => Self::subscription(&mut text, usage),
                SubscriptionObservation::Unavailable(unavailable) => {
                    Self::subscription_unavailable(&mut text, unavailable)
                }
            }
        }
        match self.snapshot.planning_projection {
            PlanningProjection::NotConfigured => {
                let _ = writeln!(text, "planning projection: not configured");
            }
        }
        let _ = writeln!(text, "\ncontext");
        for observation in &self.snapshot.session_context_observations {
            match observation {
                SessionContextObservation::Observed(context) => Self::context(&mut text, context),
                SessionContextObservation::Unavailable(unavailable) => {
                    Self::context_unavailable(&mut text, unavailable)
                }
                SessionContextObservation::SourceUnavailable(unavailable) => {
                    Self::context_source_unavailable(&mut text, unavailable)
                }
            }
        }
        text
    }

    fn provider(provider: &UsageProvider) -> &'static str {
        match provider {
            UsageProvider::Claude => "Claude",
            UsageProvider::Codex => "Codex",
        }
    }

    fn subscription(text: &mut String, usage: &SubscriptionUsage) {
        let _ = write!(text, "{}", Self::provider(&usage.usage_provider));
        if let Some(plan) = &usage.plan_name_option {
            let _ = write!(text, " ({plan})");
        }
        if !usage.account_homes.is_empty() {
            let _ = write!(text, " · homes {}", usage.account_homes.join(", "));
        }
        let _ = writeln!(text);
        for limit in &usage.quota_limits {
            for window in &limit.quota_windows {
                Self::window(text, limit, window);
            }
        }
        if !usage.unrecognized_window_names.is_empty() {
            let _ = writeln!(
                text,
                "  unrecognized windows: {}",
                usage.unrecognized_window_names.join(", ")
            );
        }
        if !usage.unmodeled_source_facts.is_empty() {
            let _ = writeln!(
                text,
                "  unmodeled source facts: {}",
                usage.unmodeled_source_facts.join(", ")
            );
        }
    }

    fn subscription_unavailable(text: &mut String, unavailable: &UsageUnavailable) {
        let _ = write!(text, "{}", Self::provider(&unavailable.usage_provider));
        if let Some(home) = &unavailable.account_home_option {
            let _ = write!(text, " {home}");
        }
        let _ = writeln!(
            text,
            ": unavailable ({})",
            Spoken(&unavailable.usage_unavailable_reason)
        );
    }

    fn window_title(limit: &QuotaLimit, window: &QuotaWindow) -> String {
        let mut title = match &limit.quota_limit_name_option {
            Some(name) => format!("{} {name}", limit.quota_limit_identifier),
            None => limit.quota_limit_identifier.clone(),
        };
        if window.provider_window_name != limit.quota_limit_identifier {
            title = format!("{title} / {}", window.provider_window_name);
        }
        if let Some(scope) = &window.provider_scope_name_option {
            title = format!("{title} [{scope}]");
        }
        title
    }

    fn local_reset(reset: &LocalReset) -> String {
        match reset {
            LocalReset::Rendered(local) => {
                let offset = local.utc_offset_seconds;
                let sign = if offset < 0 { '-' } else { '+' };
                let offset = offset.abs();
                format!(
                    "{} {} (UTC{sign}{:02}:{:02})",
                    local.local_date_time.replace('T', " "),
                    local.timezone_name,
                    offset / SECONDS_PER_HOUR,
                    offset % SECONDS_PER_HOUR / SECONDS_PER_MINUTE
                )
            }
            LocalReset::Unknown(reason) => format!("local time unknown ({})", Spoken(reason)),
        }
    }

    fn window(text: &mut String, limit: &QuotaLimit, window: &QuotaWindow) {
        let title = Self::window_title(limit, window);
        let reset = Self::local_reset(&window.local_reset);
        let headline = match (&window.window_usage, &window.reset_countdown) {
            (WindowUsage::Current(share), ResetCountdown::Pending(seconds)) => format!(
                "{} remaining · {} left · resets {reset}",
                Percent(share.remaining_basis_points),
                Span(*seconds)
            ),
            (WindowUsage::Current(share), _) => format!(
                "{} remaining · reset unknown",
                Percent(share.remaining_basis_points)
            ),
            (WindowUsage::StaleAfterReset(share), ResetCountdown::Passed(seconds)) => format!(
                "stale: {} used before a reset that passed {} ago at {reset}; not a fresh window",
                Percent(share.used_basis_points),
                Span(*seconds)
            ),
            (WindowUsage::StaleAfterReset(share), _) => format!(
                "stale: {} used before a passed reset; not a fresh window",
                Percent(share.used_basis_points)
            ),
            (WindowUsage::Unreadable(reason), ResetCountdown::Pending(seconds)) => format!(
                "usage unreadable ({}) · {} left · resets {reset}",
                Spoken(reason),
                Span(*seconds)
            ),
            (WindowUsage::Unreadable(reason), _) => {
                format!("usage unreadable ({})", Spoken(reason))
            }
        };
        let _ = writeln!(text, "  {title}: {headline}");
        Self::remainder_rate(text, &window.remainder_rate_derivation);
        Self::uniform_rate(
            text,
            &window.uniform_rate_derivation,
            &window.window_duration_basis,
        );
        Self::elapsed(text, &window.elapsed_window_derivation);
    }

    fn remainder_rate(text: &mut String, derivation: &RemainderRateDerivation) {
        match derivation {
            RemainderRateDerivation::Derived(rate) => {
                let remaining = rate.remaining_basis_points as f64 / BASIS_POINTS_PER_PERCENT;
                let until = rate.seconds_until_reset as f64;
                let _ = writeln!(
                    text,
                    "    use remaining by reset: {:.2} percentage points/hour, {:.2} per day \
                     (clock allowance from this snapshot, not observed burn)",
                    remaining * SECONDS_PER_HOUR as f64 / until,
                    remaining * SECONDS_PER_DAY as f64 / until
                );
            }
            RemainderRateDerivation::Unknown(reason) => {
                let _ = writeln!(
                    text,
                    "    use remaining by reset: unavailable ({})",
                    Spoken(reason)
                );
            }
        }
    }

    fn uniform_rate(
        text: &mut String,
        derivation: &UniformRateDerivation,
        basis: &WindowDurationBasis,
    ) {
        let UniformRateDerivation::Derived(rate) = derivation else {
            return;
        };
        let minutes = rate.window_duration_minutes;
        let _ = writeln!(
            text,
            "    uniform rate: {:.2} percentage points/day over a {} window ({})",
            100.0 * MINUTES_PER_DAY / minutes as f64,
            Span(minutes * SECONDS_PER_MINUTE),
            Spoken(basis)
        );
    }

    fn elapsed(text: &mut String, derivation: &ElapsedWindowDerivation) {
        match derivation {
            ElapsedWindowDerivation::Derived(position) => {
                let _ = writeln!(
                    text,
                    "    elapsed-window difference: {:+.2} percentage points ({} used, {} of the fixed period elapsed)",
                    position.used_minus_elapsed_basis_points as f64 / BASIS_POINTS_PER_PERCENT,
                    Percent(position.used_basis_points),
                    Percent(position.elapsed_basis_points)
                );
            }
            ElapsedWindowDerivation::Unknown(reason) => {
                let _ = writeln!(
                    text,
                    "    elapsed-window difference: unavailable ({})",
                    Spoken(reason)
                );
            }
        }
    }

    fn context(text: &mut String, context: &SessionContext) {
        let label = context
            .session_name_option
            .clone()
            .unwrap_or_else(|| context.session_identifier.clone());
        let model = context
            .model_identifier_option
            .clone()
            .unwrap_or_else(|| "model unknown".to_owned());
        let tokens = match (
            context.context_tokens_option,
            context.context_window_tokens_option,
            context.context_used_basis_points_option,
        ) {
            (Some(tokens), Some(window), Some(used)) => {
                format!("{tokens} of {window} tokens ({})", Percent(used))
            }
            (Some(tokens), Some(window), None) => format!("{tokens} of {window} tokens"),
            (Some(tokens), None, _) => format!("{tokens} tokens, window unknown"),
            (None, Some(window), _) => format!("tokens unknown of {window}"),
            (None, None, _) => "tokens unknown".to_owned(),
        };
        let _ = writeln!(
            text,
            "  {} {label} · {model}: {tokens} · {} ({})",
            Self::provider(&context.usage_provider),
            Spoken(&context.context_freshness),
            Spoken(&context.context_basis)
        );
    }

    fn context_unavailable(text: &mut String, unavailable: &SessionContextUnavailable) {
        let _ = writeln!(
            text,
            "  {} {}: unavailable ({})",
            Self::provider(&unavailable.usage_provider),
            unavailable.session_identifier,
            Spoken(&unavailable.context_unavailable_reason)
        );
    }

    fn context_source_unavailable(text: &mut String, unavailable: &ContextSourceUnavailable) {
        let _ = write!(text, "  {}", Self::provider(&unavailable.usage_provider));
        if let Some(home) = &unavailable.account_home_option {
            let _ = write!(text, " {home}");
        }
        let _ = writeln!(
            text,
            ": context source unavailable ({})",
            Spoken(&unavailable.context_source_failure_reason)
        );
    }
}
