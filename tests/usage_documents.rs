//! Recorded-provider fixtures for the usage snapshot's normalization and its
//! time metrics.
//!
//! `claude-usage.json` is the Claude OAuth usage body witnessed on
//! 2026-10-03 (it carries no identifiers). `codex-rate-limits.json` is the
//! witnessed `account/rateLimits/read` result with its account id replaced,
//! plus a constructed second limit (`codex_bengalfox`, modeled on an earlier
//! witness) carrying two windows and an unrecognized `individualLimit`.
//! Expected figures were computed independently of this crate.

use harness::usage::{
    ClaudeUsageDocument, CodexRateLimitsDocument, LocalZone, ObservationInstant, ProviderPercent,
    QuotaDocument, WindowNormalizing, WindowObservation, WindowReading,
};
use signal_harness::{
    AbsoluteLimit, ElapsedUnknownReason, ElapsedWindowDerivation, ElapsedWindowPosition,
    LocalReset, LocalResetTime, LocalResetUnknownReason, PeriodSemantics, QuotaShare, QuotaWindow,
    RateBasis, RateRounding, RemainderByResetRate, RemainderRateDerivation,
    RemainderRateUnknownReason, ResetBasis, ResetCountdown, ShareConversion, UniformRateDerivation,
    UniformRateUnknownReason, UniformWindowRate, UsageProvider, UsageUnavailableReason,
    UsageUnreadableReason, WindowDurationBasis, WindowUsage,
};

/// 2026-10-03T21:04:51Z, the Claude body's own `as_of` second.
const OBSERVED_SECOND: i64 = 1_791_061_491;
const ZONE: &str = "America/Mexico_City";

fn observed() -> ObservationInstant {
    ObservationInstant::from_nanoseconds(OBSERVED_SECOND * 1_000_000_000)
}

fn zone() -> LocalZone {
    LocalZone::named(ZONE)
}

fn claude_body() -> &'static str {
    include_str!("fixtures/usage/claude-usage.json")
}

fn share(used: i64) -> WindowUsage {
    WindowUsage::Current(QuotaShare {
        used_basis_points: used,
        remaining_basis_points: 10_000 - used,
        share_conversion: ShareConversion::ProviderPercentRoundedToBasisPoint,
    })
}

fn local(date_time: &str) -> LocalReset {
    LocalReset::Rendered(LocalResetTime {
        local_date_time: date_time.into(),
        timezone_name: ZONE.into(),
        utc_offset_seconds: -21_600,
    })
}

fn rate(remaining: i64, until: i64, per_hour: i64, per_day: i64) -> RemainderRateDerivation {
    RemainderRateDerivation::Derived(RemainderByResetRate {
        rate_basis: RateBasis::OneSnapshotClockAllowance,
        remaining_basis_points: remaining,
        seconds_until_reset: until,
        remaining_basis_points_per_clock_hour: per_hour,
        remaining_basis_points_per_clock_day: per_day,
        rate_rounding: RateRounding::TowardZero,
    })
}

fn uniform(minutes: i64, per_day: i64) -> UniformRateDerivation {
    UniformRateDerivation::Derived(UniformWindowRate {
        window_duration_minutes: minutes,
        uniform_basis_points_per_clock_day: per_day,
        rate_rounding: RateRounding::TowardZero,
    })
}

const NOT_FIXED: ElapsedWindowDerivation =
    ElapsedWindowDerivation::Unknown(ElapsedUnknownReason::PeriodSemanticsNotEstablished);

#[test]
fn claude_named_windows_and_listed_limits_are_each_enumerated() {
    let usage = ClaudeUsageDocument::from_body(claude_body(), Some("max".into()))
        .expect("recorded body parses")
        .normalize(observed(), &zone());
    assert_eq!(usage.usage_provider, UsageProvider::Claude);
    assert_eq!(usage.plan_name_option.as_deref(), Some("max"));
    assert_eq!(usage.observation_time, OBSERVED_SECOND * 1_000_000_000);
    assert!(usage.unrecognized_window_names.is_empty());
    let identifiers: Vec<&str> = usage
        .quota_limits
        .iter()
        .map(|limit| limit.quota_limit_identifier.as_str())
        .collect();
    assert_eq!(identifiers, ["five_hour", "seven_day", "session", "weekly"]);

    assert_eq!(
        usage.quota_limits[0].quota_windows[0],
        QuotaWindow {
            provider_window_name: "five_hour".into(),
            provider_scope_name_option: None,
            window_usage: share(3300),
            reset_basis: ResetBasis::ProviderResetTime(1_791_069_599),
            reset_countdown: ResetCountdown::Pending(8108),
            local_reset: local("2026-10-03T17:19:59"),
            window_duration_basis: WindowDurationBasis::ProviderWindowNamed(300),
            period_semantics: PeriodSemantics::NotEstablished,
            absolute_limit: AbsoluteLimit::NotExposedByProvider,
            remainder_rate_derivation: rate(6700, 8108, 2974, 71_396),
            uniform_rate_derivation: uniform(300, 48_000),
            elapsed_window_derivation: NOT_FIXED,
        }
    );
    let seven_day = &usage.quota_limits[1].quota_windows[0];
    assert_eq!(
        seven_day.remainder_rate_derivation,
        rate(8500, 575_708, 53, 1275)
    );
    assert_eq!(seven_day.uniform_rate_derivation, uniform(10_080, 1428));
    assert_eq!(seven_day.local_reset, local("2026-10-10T06:59:59"));
}

#[test]
fn a_listed_limit_keeps_its_reset_and_rate_without_an_inferred_duration() {
    let usage = ClaudeUsageDocument::from_body(claude_body(), None)
        .expect("parses")
        .normalize(observed(), &zone());
    // `session` shares its reset second with `five_hour`; that does not make
    // it a five-hour window.
    let session = &usage.quota_limits[2].quota_windows[0];
    assert_eq!(session.provider_window_name, "session");
    assert_eq!(session.reset_countdown, ResetCountdown::Pending(8108));
    assert_eq!(session.local_reset, local("2026-10-03T17:19:59"));
    assert_eq!(session.window_duration_basis, WindowDurationBasis::Unknown);
    assert_eq!(
        session.remainder_rate_derivation,
        rate(6700, 8108, 2974, 71_396)
    );
    assert_eq!(
        session.uniform_rate_derivation,
        UniformRateDerivation::Unknown(UniformRateUnknownReason::WindowDurationUnknown)
    );

    let weekly = &usage.quota_limits[3].quota_windows;
    assert_eq!(weekly.len(), 2);
    assert_eq!(weekly[0].provider_window_name, "weekly_all");
    assert_eq!(
        weekly[0].window_duration_basis,
        WindowDurationBasis::Unknown
    );
    assert_eq!(weekly[1].provider_window_name, "weekly_scoped");
    assert_eq!(
        weekly[1].provider_scope_name_option.as_deref(),
        Some("Fable")
    );
    assert_eq!(weekly[1].window_usage, share(1200));
    assert_eq!(
        weekly[1].remainder_rate_derivation,
        rate(8800, 575_708, 55, 1320)
    );
}

#[test]
fn claude_auxiliary_allowance_and_spend_facts_are_named_not_windowed() {
    let usage = ClaudeUsageDocument::from_body(claude_body(), None)
        .expect("parses")
        .normalize(observed(), &zone());
    assert_eq!(
        usage.unmodeled_source_facts,
        [
            "extra_usage",
            "member_dashboard_available",
            "seven_day_breakdown",
            "spend",
            "limits.session.is_active",
            "limits.session.severity",
            "limits.weekly_all.is_active",
            "limits.weekly_all.severity",
            "limits.weekly_scoped.is_active",
            "limits.weekly_scoped.severity",
        ]
    );
    let windows: Vec<&str> = usage
        .quota_limits
        .iter()
        .flat_map(|limit| &limit.quota_windows)
        .map(|window| window.provider_window_name.as_str())
        .collect();
    assert!(!windows.contains(&"extra_usage") && !windows.contains(&"spend"));
}

#[test]
fn claude_unrecognized_windows_and_unreadable_percentages_are_retained() {
    let mut body: serde_json::Value = serde_json::from_str(claude_body()).expect("json");
    body["tangelo"] = serde_json::json!({ "utilization": 4.0, "resets_at": null });
    body["limits"].as_array_mut().expect("limits").extend([
        serde_json::json!({ "kind": "mystery", "group": "mystery", "percent": null }),
        serde_json::json!({ "kind": "overrun", "group": "mystery", "percent": 104.0 }),
        serde_json::json!({ "kind": "negative", "group": "mystery", "percent": -1.0 }),
    ]);
    let usage = ClaudeUsageDocument::from_body(&body.to_string(), None)
        .expect("parses")
        .normalize(observed(), &zone());
    assert_eq!(usage.unrecognized_window_names, ["tangelo"]);
    let mystery = &usage.quota_limits[4];
    assert_eq!(mystery.quota_limit_identifier, "mystery");
    let usages: Vec<&WindowUsage> = mystery
        .quota_windows
        .iter()
        .map(|window| &window.window_usage)
        .collect();
    assert_eq!(
        usages,
        [
            &WindowUsage::Unreadable(UsageUnreadableReason::PercentageAbsent),
            &WindowUsage::Unreadable(UsageUnreadableReason::PercentageAboveFull),
            &WindowUsage::Unreadable(UsageUnreadableReason::PercentageNegative),
        ]
    );
    for window in &mystery.quota_windows {
        assert_eq!(
            window.remainder_rate_derivation,
            RemainderRateDerivation::Unknown(RemainderRateUnknownReason::UsageUnreadable)
        );
        assert_eq!(window.reset_countdown, ResetCountdown::Unknown);
        assert_eq!(
            window.local_reset,
            LocalReset::Unknown(LocalResetUnknownReason::ResetUnknown)
        );
    }
}

#[test]
fn claude_body_that_is_not_an_object_is_unreadable() {
    assert_eq!(
        ClaudeUsageDocument::from_body("[1, 2]", None).unwrap_err(),
        UsageUnavailableReason::ProviderResponseUnreadable
    );
    assert_eq!(
        ClaudeUsageDocument::from_body("<html>", None).unwrap_err(),
        UsageUnavailableReason::ProviderResponseUnreadable
    );
}

#[test]
fn codex_every_limit_window_and_source_fact_is_enumerated() {
    let result: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/usage/codex-rate-limits.json")).expect("json");
    let usage = CodexRateLimitsDocument::new(result, vec![".codex".into(), ".codex-next".into()])
        .normalize(observed(), &zone());
    assert_eq!(usage.usage_provider, UsageProvider::Codex);
    assert_eq!(usage.plan_name_option.as_deref(), Some("pro"));
    assert_eq!(usage.account_homes, [".codex", ".codex-next"]);
    assert_eq!(
        usage.unrecognized_window_names,
        ["codex_bengalfox.individualLimit"]
    );
    assert_eq!(
        usage.unmodeled_source_facts,
        [
            "ordinaryUsageAllowed",
            "rateLimitResetCredits",
            "codex.credits",
            "codex.spendControlReached",
            "codex_bengalfox.spendControlReached",
        ]
    );
    assert!(!format!("{usage:?}").contains("accountId"));
    assert_eq!(usage.quota_limits.len(), 2);

    let codex = &usage.quota_limits[0];
    assert_eq!(codex.quota_limit_identifier, "codex");
    assert_eq!(codex.quota_windows.len(), 1);
    let primary = &codex.quota_windows[0];
    assert_eq!(
        primary.window_duration_basis,
        WindowDurationBasis::ProviderDeclared(10_080)
    );
    assert_eq!(
        primary.remainder_rate_derivation,
        rate(7800, 518_897, 54, 1298)
    );
    assert_eq!(primary.uniform_rate_derivation, uniform(10_080, 1428));
    assert_eq!(primary.elapsed_window_derivation, NOT_FIXED);

    let spark = &usage.quota_limits[1];
    assert_eq!(spark.quota_limit_identifier, "codex_bengalfox");
    assert_eq!(spark.quota_limit_name_option.as_deref(), Some("Spark"));
    let windows: Vec<(&str, &WindowDurationBasis)> = spark
        .quota_windows
        .iter()
        .map(|window| {
            (
                window.provider_window_name.as_str(),
                &window.window_duration_basis,
            )
        })
        .collect();
    assert_eq!(
        windows,
        [
            ("primary", &WindowDurationBasis::ProviderDeclared(300)),
            ("secondary", &WindowDurationBasis::ProviderDeclared(10_080)),
        ]
    );
    assert_eq!(
        spark.quota_windows[0].remainder_rate_derivation,
        rate(10_000, 11_509, 3127, 75_071)
    );
}

const NOW: i64 = 1_000_000;

fn reading(reset: ResetBasis, duration: WindowDurationBasis) -> WindowReading {
    WindowReading {
        name: "window".into(),
        scope: None,
        used: ProviderPercent::Present(40.0),
        reset_basis: reset,
        duration_basis: duration,
        period_semantics: PeriodSemantics::NotEstablished,
    }
}

fn at_now(reading: &WindowReading) -> QuotaWindow {
    reading.normalize_at(&WindowObservation::new(NOW, LocalZone::named("UTC")))
}

#[test]
fn countdown_is_its_own_state_for_pending_unknown_and_passed_resets() {
    let pending = at_now(&reading(
        ResetBasis::ProviderResetTime(NOW + 18_000),
        WindowDurationBasis::Unknown,
    ));
    assert_eq!(pending.reset_countdown, ResetCountdown::Pending(18_000));
    assert_eq!(pending.window_usage, share(4000));
    // 40 % over five clock hours: 8 percentage points an hour, 800 basis points.
    assert_eq!(
        pending.remainder_rate_derivation,
        rate(6000, 18_000, 1200, 28_800)
    );
    assert_eq!(
        pending.uniform_rate_derivation,
        UniformRateDerivation::Unknown(UniformRateUnknownReason::WindowDurationUnknown)
    );

    let unknown = at_now(&reading(
        ResetBasis::Unknown,
        WindowDurationBasis::ProviderDeclared(300),
    ));
    assert_eq!(unknown.reset_countdown, ResetCountdown::Unknown);
    assert_eq!(unknown.window_usage, share(4000));
    assert_eq!(
        unknown.remainder_rate_derivation,
        RemainderRateDerivation::Unknown(RemainderRateUnknownReason::ResetUnknown)
    );
    assert_eq!(
        unknown.local_reset,
        LocalReset::Unknown(LocalResetUnknownReason::ResetUnknown)
    );

    let passed = at_now(&reading(
        ResetBasis::ProviderResetTime(NOW - 600),
        WindowDurationBasis::ProviderDeclared(300),
    ));
    assert_eq!(passed.reset_countdown, ResetCountdown::Passed(600));
    let WindowUsage::StaleAfterReset(stale) = &passed.window_usage else {
        panic!("a passed reset leaves stale values: {passed:?}");
    };
    assert_eq!(stale.used_basis_points, 4000);
    assert_eq!(
        passed.remainder_rate_derivation,
        RemainderRateDerivation::Unknown(RemainderRateUnknownReason::UsageStale)
    );
}

#[test]
fn a_reset_at_the_observation_second_has_passed_and_divides_nothing() {
    let at_reset = at_now(&reading(
        ResetBasis::ProviderResetTime(NOW),
        WindowDurationBasis::ProviderDeclared(300),
    ));
    assert_eq!(at_reset.reset_countdown, ResetCountdown::Passed(0));
    assert!(matches!(
        at_reset.window_usage,
        WindowUsage::StaleAfterReset(_)
    ));
    assert!(matches!(
        at_reset.remainder_rate_derivation,
        RemainderRateDerivation::Unknown(_)
    ));
}

#[test]
fn a_full_share_has_no_remainder_and_a_zero_rate() {
    let mut full = reading(
        ResetBasis::ProviderResetTime(NOW + 3600),
        WindowDurationBasis::ProviderDeclared(300),
    );
    full.used = ProviderPercent::Present(100.0);
    let window = at_now(&full);
    assert_eq!(window.window_usage, share(10_000));
    assert_eq!(window.remainder_rate_derivation, rate(0, 3600, 0, 0));
}

#[test]
fn percentages_outside_the_documented_domain_are_unreadable_not_clamped() {
    for (percent, reason) in [
        (f64::NAN, UsageUnreadableReason::PercentageNotFinite),
        (f64::INFINITY, UsageUnreadableReason::PercentageNotFinite),
        (-0.5, UsageUnreadableReason::PercentageNegative),
        (100.01, UsageUnreadableReason::PercentageAboveFull),
    ] {
        let mut odd = reading(
            ResetBasis::ProviderResetTime(NOW + 3600),
            WindowDurationBasis::ProviderDeclared(300),
        );
        odd.used = ProviderPercent::Present(percent);
        let window = at_now(&odd);
        assert_eq!(window.window_usage, WindowUsage::Unreadable(reason));
        assert_eq!(window.reset_countdown, ResetCountdown::Pending(3600));
        assert_eq!(window.absolute_limit, AbsoluteLimit::NotExposedByProvider);
    }
}

#[test]
fn rates_round_toward_zero_and_the_weekly_uniform_rate_is_a_seventh() {
    let mut nearly_spent = reading(
        ResetBasis::ProviderResetTime(NOW + 604_800),
        WindowDurationBasis::ProviderDeclared(10_080),
    );
    nearly_spent.used = ProviderPercent::Present(99.0);
    let window = at_now(&nearly_spent);
    // 100 basis points over seven days: 0.595 an hour, 14.29 a day.
    assert_eq!(window.remainder_rate_derivation, rate(100, 604_800, 0, 14));
    // 100/7 percent a day is 1428.57 basis points.
    assert_eq!(window.uniform_rate_derivation, uniform(10_080, 1428));

    let mut fine = reading(
        ResetBasis::ProviderResetTime(NOW + 3600),
        WindowDurationBasis::Unknown,
    );
    fine.used = ProviderPercent::Present(33.333);
    assert_eq!(at_now(&fine).window_usage, share(3333));
}

#[test]
fn the_elapsed_position_needs_an_established_fixed_period() {
    let mut fixed = reading(
        ResetBasis::ProviderResetTime(NOW + 9599),
        WindowDurationBasis::ProviderWindowNamed(300),
    );
    fixed.used = ProviderPercent::Present(33.0);
    assert_eq!(at_now(&fixed).elapsed_window_derivation, NOT_FIXED);

    fixed.period_semantics = PeriodSemantics::FixedPeriod;
    assert_eq!(
        at_now(&fixed).elapsed_window_derivation,
        ElapsedWindowDerivation::Derived(ElapsedWindowPosition {
            window_duration_minutes: 300,
            seconds_until_reset: 9599,
            elapsed_basis_points: 4667,
            used_basis_points: 3300,
            used_minus_elapsed_basis_points: -1367,
            rate_rounding: RateRounding::TowardZero,
        })
    );

    let cases = [
        (
            ResetBasis::ProviderResetTime(NOW + 20_000),
            WindowDurationBasis::ProviderDeclared(300),
            ElapsedUnknownReason::ResetBeyondWindow,
        ),
        (
            ResetBasis::ProviderResetTime(NOW + 600),
            WindowDurationBasis::Unknown,
            ElapsedUnknownReason::WindowDurationUnknown,
        ),
        (
            ResetBasis::ProviderResetTime(NOW + 600),
            WindowDurationBasis::ProviderDeclared(0),
            ElapsedUnknownReason::WindowDurationNotPositive,
        ),
        (
            ResetBasis::ProviderResetTime(NOW - 600),
            WindowDurationBasis::ProviderDeclared(300),
            ElapsedUnknownReason::UsageStale,
        ),
        (
            ResetBasis::Unknown,
            WindowDurationBasis::ProviderDeclared(300),
            ElapsedUnknownReason::ResetUnknown,
        ),
    ];
    for (reset, duration, reason) in cases {
        let mut case = reading(reset, duration);
        case.period_semantics = PeriodSemantics::FixedPeriod;
        assert_eq!(
            at_now(&case).elapsed_window_derivation,
            ElapsedWindowDerivation::Unknown(reason)
        );
    }
    let mut zero = reading(
        ResetBasis::ProviderResetTime(NOW + 600),
        WindowDurationBasis::ProviderDeclared(0),
    );
    zero.period_semantics = PeriodSemantics::FixedPeriod;
    assert_eq!(
        at_now(&zero).uniform_rate_derivation,
        UniformRateDerivation::Unknown(UniformRateUnknownReason::WindowDurationNotPositive)
    );
}

#[test]
fn the_local_reset_is_rendered_only_in_a_configured_zone() {
    let window = reading(
        ResetBasis::ProviderResetTime(1_791_069_599),
        WindowDurationBasis::Unknown,
    );
    assert_eq!(
        window
            .normalize_at(&WindowObservation::new(OBSERVED_SECOND, zone()))
            .local_reset,
        local("2026-10-03T17:19:59")
    );
    assert_eq!(
        window
            .normalize_at(&WindowObservation::new(
                OBSERVED_SECOND,
                LocalZone::unavailable()
            ))
            .local_reset,
        LocalReset::Unknown(LocalResetUnknownReason::TimezoneUnavailable)
    );
}
