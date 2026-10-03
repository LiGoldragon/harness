//! One quota window's reading and the pace derived from it.
//!
//! A pace is derived only when every operand is known and current: the
//! remaining share, the seconds until the provider's named reset, and the
//! window duration. Otherwise the reply says which operand is missing.

use usage_contract::{
    AbsoluteLimit, PaceDerivation, PaceUnknownReason, QuotaPace, QuotaWindow, ResetBasis,
    WindowDurationBasis, WindowFreshness,
};

const FULL_BASIS_POINTS: i64 = 10_000;
const SECONDS_PER_DAY: i64 = 86_400;
const SECONDS_PER_MINUTE: i64 = 60;

/// One provider window as read, before normalization.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowReading {
    pub name: String,
    pub scope: Option<String>,
    pub used_basis_points: i64,
    pub reset_basis: ResetBasis,
    pub duration_basis: WindowDurationBasis,
}

/// The window normalized at one observation second.
pub trait WindowNormalizing {
    fn normalize_at(&self, observed_second: i64) -> QuotaWindow;
}

impl WindowNormalizing for WindowReading {
    fn normalize_at(&self, observed_second: i64) -> QuotaWindow {
        let remaining = (FULL_BASIS_POINTS - self.used_basis_points).max(0);
        let reset = match self.reset_basis {
            ResetBasis::ProviderResetTime(second) => Some(second),
            ResetBasis::Unknown => None,
        };
        let freshness = match reset {
            Some(second) if second <= observed_second => WindowFreshness::ResetPassed,
            _ => WindowFreshness::Current,
        };
        QuotaWindow {
            provider_window_name: self.name.clone(),
            provider_scope_name_option: self.scope.clone(),
            used_basis_points: self.used_basis_points,
            remaining_basis_points: remaining,
            reset_basis: self.reset_basis.clone(),
            window_duration_basis: self.duration_basis.clone(),
            absolute_limit: AbsoluteLimit::NotExposedByProvider,
            window_freshness: freshness,
            pace_derivation: self.pace(remaining, reset, observed_second),
        }
    }
}

// Exception (too trivial): the private arithmetic step of `normalize_at`.
impl WindowReading {
    fn pace(&self, remaining: i64, reset: Option<i64>, observed_second: i64) -> PaceDerivation {
        let Some(reset) = reset else {
            return PaceDerivation::Unknown(PaceUnknownReason::ResetUnknown);
        };
        let minutes = match self.duration_basis {
            WindowDurationBasis::ProviderDeclared(minutes)
            | WindowDurationBasis::ProviderWindowNamed(minutes) => minutes,
            WindowDurationBasis::Unknown => {
                return PaceDerivation::Unknown(PaceUnknownReason::WindowDurationUnknown);
            }
        };
        let until = reset - observed_second;
        if until <= 0 {
            return PaceDerivation::Unknown(PaceUnknownReason::ResetPassed);
        }
        let window = minutes.saturating_mul(SECONDS_PER_MINUTE);
        if window <= 0 {
            return PaceDerivation::Unknown(PaceUnknownReason::WindowDurationUnknown);
        }
        if until > window {
            return PaceDerivation::Unknown(PaceUnknownReason::ResetBeyondWindow);
        }
        let even = (window - until) * FULL_BASIS_POINTS / window;
        PaceDerivation::Derived(QuotaPace {
            remaining_basis_points: remaining,
            seconds_until_reset: until,
            window_duration_minutes: minutes,
            remaining_basis_points_per_day: remaining.saturating_mul(SECONDS_PER_DAY) / until,
            even_pace_used_basis_points: even,
            pace_variance_basis_points: self.used_basis_points - even,
        })
    }
}
