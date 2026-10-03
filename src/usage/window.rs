//! One quota window as read, and its normalization at one observation.
//!
//! The provider's percentage becomes a share in basis points only when it is a
//! finite value from 0 to 100; neither provider documents an overrun, so any
//! other value is unreadable rather than clamped. The reset countdown, its
//! local rendering and three derivations are kept apart, each `Derived` with
//! its operands or `Unknown` with its reason:
//!
//! - the rate to use the remainder by the reset, `r / T`, which needs a
//!   current share and a pending reset but no window duration;
//! - the window's uniform rate, one full share over its known duration;
//! - the elapsed position of a fixed period, `e = (W - T) / W` and `u - e`,
//!   which needs fixed-period semantics the providers do not establish.
//!
//! Every rate is rounded toward zero; the operands stay in the reply so a
//! reader can compute the exact figure. Nothing divides by zero.

use signal_harness::{
    AbsoluteLimit, ElapsedUnknownReason, ElapsedWindowDerivation, ElapsedWindowPosition,
    LocalReset, LocalResetTime, LocalResetUnknownReason, PeriodSemantics, QuotaShare, QuotaWindow,
    RateBasis, RateRounding, RemainderByResetRate, RemainderRateDerivation,
    RemainderRateUnknownReason, ResetBasis, ResetCountdown, ShareConversion, UniformRateDerivation,
    UniformRateUnknownReason, UniformWindowRate, UsageUnreadableReason, WindowDurationBasis,
    WindowUsage,
};

const FULL_BASIS_POINTS: i64 = 10_000;
const BASIS_POINTS_PER_PERCENT: f64 = 100.0;
const FULL_PERCENT: f64 = 100.0;
const SECONDS_PER_HOUR: i64 = 3_600;
const SECONDS_PER_DAY: i64 = 86_400;
const SECONDS_PER_MINUTE: i64 = 60;

/// The provider's used percentage as it arrived.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ProviderPercent {
    Present(f64),
    Absent,
}

/// One provider window as read, before normalization.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowReading {
    pub name: String,
    pub scope: Option<String>,
    pub used: ProviderPercent,
    pub reset_basis: ResetBasis,
    pub duration_basis: WindowDurationBasis,
    pub period_semantics: PeriodSemantics,
}

/// The observation a window is normalized at: the second it was observed
/// and the zone its reset is rendered in.
#[derive(Clone, Debug)]
pub struct WindowObservation {
    second: i64,
    zone: LocalZone,
}

/// The host's configured time zone, when one is configured.
#[derive(Clone, Debug)]
pub struct LocalZone {
    zone: Option<NamedZone>,
}

#[derive(Clone, Debug)]
struct NamedZone {
    name: String,
    zone: jiff::tz::TimeZone,
}

/// The window normalized at one observation.
pub trait WindowNormalizing {
    fn normalize_at(&self, observation: &WindowObservation) -> QuotaWindow;
}

impl WindowObservation {
    pub fn new(second: i64, zone: LocalZone) -> Self {
        Self { second, zone }
    }

    pub fn second(&self) -> i64 {
        self.second
    }

    pub fn zone(&self) -> &LocalZone {
        &self.zone
    }
}

impl LocalZone {
    /// The host's configured zone (`TZ`, else `/etc/localtime`), named by its
    /// IANA identifier. A zone with no IANA name is not invented.
    pub fn system() -> Self {
        let zone = jiff::tz::TimeZone::try_system().ok().and_then(|zone| {
            let name = zone.iana_name()?.to_owned();
            Some(NamedZone { name, zone })
        });
        Self { zone }
    }

    /// A zone looked up by its IANA identifier.
    pub fn named(name: &str) -> Self {
        Self {
            zone: jiff::tz::TimeZone::get(name).ok().map(|zone| NamedZone {
                name: name.to_owned(),
                zone,
            }),
        }
    }

    /// No configured zone.
    pub fn unavailable() -> Self {
        Self { zone: None }
    }

    /// The reset second rendered in this zone, to the second.
    pub fn render(&self, reset_second: i64) -> LocalReset {
        let Some(named) = &self.zone else {
            return LocalReset::Unknown(LocalResetUnknownReason::TimezoneUnavailable);
        };
        let Ok(instant) = jiff::Timestamp::from_second(reset_second) else {
            return LocalReset::Unknown(LocalResetUnknownReason::TimezoneUnavailable);
        };
        let zoned = instant.to_zoned(named.zone.clone());
        LocalReset::Rendered(LocalResetTime {
            local_date_time: zoned.datetime().strftime("%Y-%m-%dT%H:%M:%S").to_string(),
            timezone_name: named.name.clone(),
            utc_offset_seconds: i64::from(zoned.offset().seconds()),
        })
    }
}

impl WindowNormalizing for WindowReading {
    fn normalize_at(&self, observation: &WindowObservation) -> QuotaWindow {
        let reset = match self.reset_basis {
            ResetBasis::ProviderResetTime(second) => Some(second),
            ResetBasis::Unknown => None,
        };
        let countdown = match reset {
            Some(second) if second > observation.second => {
                ResetCountdown::Pending(second - observation.second)
            }
            Some(second) => ResetCountdown::Passed(observation.second - second),
            None => ResetCountdown::Unknown,
        };
        let usage = match (self.share(), &countdown) {
            (Err(reason), _) => WindowUsage::Unreadable(reason),
            (Ok(share), ResetCountdown::Passed(_)) => WindowUsage::StaleAfterReset(share),
            (Ok(share), _) => WindowUsage::Current(share),
        };
        let local_reset = match reset {
            Some(second) => observation.zone.render(second),
            None => LocalReset::Unknown(LocalResetUnknownReason::ResetUnknown),
        };
        let derivations = WindowDerivations {
            usage: &usage,
            countdown: &countdown,
            duration: &self.duration_basis,
            period: &self.period_semantics,
        };
        let remainder_rate_derivation = derivations.remainder_rate();
        let uniform_rate_derivation = derivations.uniform_rate();
        let elapsed_window_derivation = derivations.elapsed();
        QuotaWindow {
            provider_window_name: self.name.clone(),
            provider_scope_name_option: self.scope.clone(),
            window_usage: usage,
            reset_basis: self.reset_basis.clone(),
            reset_countdown: countdown,
            local_reset,
            window_duration_basis: self.duration_basis.clone(),
            period_semantics: self.period_semantics.clone(),
            absolute_limit: AbsoluteLimit::NotExposedByProvider,
            remainder_rate_derivation,
            uniform_rate_derivation,
            elapsed_window_derivation,
        }
    }
}

// Exception (too trivial): the private validation step of `normalize_at`.
impl WindowReading {
    fn share(&self) -> Result<QuotaShare, UsageUnreadableReason> {
        let ProviderPercent::Present(percent) = self.used else {
            return Err(UsageUnreadableReason::PercentageAbsent);
        };
        if !percent.is_finite() {
            return Err(UsageUnreadableReason::PercentageNotFinite);
        }
        if percent < 0.0 {
            return Err(UsageUnreadableReason::PercentageNegative);
        }
        if percent > FULL_PERCENT {
            return Err(UsageUnreadableReason::PercentageAboveFull);
        }
        let used = (percent * BASIS_POINTS_PER_PERCENT).round() as i64;
        Ok(QuotaShare {
            used_basis_points: used,
            remaining_basis_points: FULL_BASIS_POINTS - used,
            share_conversion: ShareConversion::ProviderPercentRoundedToBasisPoint,
        })
    }
}

/// The operands the three derivations read.
struct WindowDerivations<'a> {
    usage: &'a WindowUsage,
    countdown: &'a ResetCountdown,
    duration: &'a WindowDurationBasis,
    period: &'a PeriodSemantics,
}

impl WindowDerivations<'_> {
    fn duration_minutes(&self) -> Option<i64> {
        match self.duration {
            WindowDurationBasis::ProviderDeclared(minutes)
            | WindowDurationBasis::ProviderWindowNamed(minutes) => Some(*minutes),
            WindowDurationBasis::Unknown => None,
        }
    }

    fn remainder_rate(&self) -> RemainderRateDerivation {
        let share = match self.usage {
            WindowUsage::Current(share) => share,
            WindowUsage::StaleAfterReset(_) => {
                return RemainderRateDerivation::Unknown(RemainderRateUnknownReason::UsageStale);
            }
            WindowUsage::Unreadable(_) => {
                return RemainderRateDerivation::Unknown(
                    RemainderRateUnknownReason::UsageUnreadable,
                );
            }
        };
        let until = match self.countdown {
            ResetCountdown::Pending(seconds) => *seconds,
            ResetCountdown::Passed(_) => {
                return RemainderRateDerivation::Unknown(RemainderRateUnknownReason::ResetPassed);
            }
            ResetCountdown::Unknown => {
                return RemainderRateDerivation::Unknown(RemainderRateUnknownReason::ResetUnknown);
            }
        };
        let remaining = share.remaining_basis_points;
        RemainderRateDerivation::Derived(RemainderByResetRate {
            rate_basis: RateBasis::OneSnapshotClockAllowance,
            remaining_basis_points: remaining,
            seconds_until_reset: until,
            remaining_basis_points_per_clock_hour: remaining * SECONDS_PER_HOUR / until,
            remaining_basis_points_per_clock_day: remaining * SECONDS_PER_DAY / until,
            rate_rounding: RateRounding::TowardZero,
        })
    }

    fn uniform_rate(&self) -> UniformRateDerivation {
        let Some(minutes) = self.duration_minutes() else {
            return UniformRateDerivation::Unknown(UniformRateUnknownReason::WindowDurationUnknown);
        };
        if minutes <= 0 {
            return UniformRateDerivation::Unknown(
                UniformRateUnknownReason::WindowDurationNotPositive,
            );
        }
        UniformRateDerivation::Derived(UniformWindowRate {
            window_duration_minutes: minutes,
            uniform_basis_points_per_clock_day: FULL_BASIS_POINTS * SECONDS_PER_DAY
                / (minutes * SECONDS_PER_MINUTE),
            rate_rounding: RateRounding::TowardZero,
        })
    }

    fn elapsed(&self) -> ElapsedWindowDerivation {
        let unknown = ElapsedWindowDerivation::Unknown;
        if !matches!(self.period, PeriodSemantics::FixedPeriod) {
            return unknown(ElapsedUnknownReason::PeriodSemanticsNotEstablished);
        }
        let used = match self.usage {
            WindowUsage::Current(share) => share.used_basis_points,
            WindowUsage::StaleAfterReset(_) => return unknown(ElapsedUnknownReason::UsageStale),
            WindowUsage::Unreadable(_) => return unknown(ElapsedUnknownReason::UsageUnreadable),
        };
        let until = match self.countdown {
            ResetCountdown::Pending(seconds) => *seconds,
            ResetCountdown::Passed(_) => return unknown(ElapsedUnknownReason::ResetPassed),
            ResetCountdown::Unknown => return unknown(ElapsedUnknownReason::ResetUnknown),
        };
        let Some(minutes) = self.duration_minutes() else {
            return unknown(ElapsedUnknownReason::WindowDurationUnknown);
        };
        if minutes <= 0 {
            return unknown(ElapsedUnknownReason::WindowDurationNotPositive);
        }
        let window = minutes * SECONDS_PER_MINUTE;
        if until > window {
            return unknown(ElapsedUnknownReason::ResetBeyondWindow);
        }
        let elapsed = (window - until) * FULL_BASIS_POINTS / window;
        ElapsedWindowDerivation::Derived(ElapsedWindowPosition {
            window_duration_minutes: minutes,
            seconds_until_reset: until,
            elapsed_basis_points: elapsed,
            used_basis_points: used,
            used_minus_elapsed_basis_points: used - elapsed,
            rate_rounding: RateRounding::TowardZero,
        })
    }
}
