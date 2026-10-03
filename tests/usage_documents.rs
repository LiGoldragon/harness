//! Recorded-provider fixtures for the usage snapshot's normalization.
//!
//! `claude-usage.json` is the Claude OAuth usage body witnessed on
//! 2026-10-03 (it carries no identifiers). `codex-rate-limits.json` is the
//! witnessed `account/rateLimits/read` result with its account id replaced,
//! plus a constructed second limit (`codex_bengalfox`, modeled on an earlier
//! witness) carrying two windows and an unrecognized `individualLimit`.
//! Expected pace figures were computed independently of this crate.

use harness::usage::pace::WindowNormalizing;
use harness::usage::{
    ClaudeUsageDocument, CodexRateLimitsDocument, ObservationInstant, QuotaDocument, WindowReading,
};
use usage_contract::{
    AbsoluteLimit, PaceDerivation, PaceUnknownReason, QuotaPace, ResetBasis, UsageProvider,
    UsageUnavailableReason, WindowDurationBasis, WindowFreshness,
};

/// 2026-10-03T21:04:51Z, the Claude body's own `as_of` second.
const OBSERVED_SECOND: i64 = 1_791_061_491;

fn observed() -> ObservationInstant {
    ObservationInstant::from_nanoseconds(OBSERVED_SECOND * 1_000_000_000)
}

fn claude_body() -> &'static str {
    include_str!("fixtures/usage/claude-usage.json")
}

#[test]
fn claude_limits_list_is_normalized_by_group_with_named_durations() {
    let usage = ClaudeUsageDocument::from_body(claude_body(), Some("max".into()))
        .expect("recorded body parses")
        .normalize(observed());
    assert_eq!(usage.usage_provider, UsageProvider::Claude);
    assert_eq!(usage.plan_name_option.as_deref(), Some("max"));
    assert_eq!(usage.observation_time, OBSERVED_SECOND * 1_000_000_000);
    assert!(usage.unrecognized_window_names.is_empty());
    let identifiers: Vec<&str> = usage
        .quota_limits
        .iter()
        .map(|limit| limit.quota_limit_identifier.as_str())
        .collect();
    assert_eq!(identifiers, ["session", "weekly"]);

    let session = &usage.quota_limits[0].quota_windows[0];
    assert_eq!(session.provider_window_name, "session");
    assert_eq!(session.used_basis_points, 3300);
    assert_eq!(session.remaining_basis_points, 6700);
    assert_eq!(
        session.reset_basis,
        ResetBasis::ProviderResetTime(1_791_069_599)
    );
    assert_eq!(
        session.window_duration_basis,
        WindowDurationBasis::ProviderWindowNamed(300)
    );
    assert_eq!(session.absolute_limit, AbsoluteLimit::NotExposedByProvider);
    assert_eq!(
        session.pace_derivation,
        PaceDerivation::Derived(QuotaPace {
            remaining_basis_points: 6700,
            seconds_until_reset: 8108,
            window_duration_minutes: 300,
            remaining_basis_points_per_day: 71_396,
            even_pace_used_basis_points: 5495,
            pace_variance_basis_points: -2195,
        })
    );

    let weekly = &usage.quota_limits[1].quota_windows;
    assert_eq!(weekly.len(), 2);
    assert_eq!(weekly[0].provider_window_name, "weekly_all");
    assert_eq!(
        weekly[0].pace_derivation,
        PaceDerivation::Derived(QuotaPace {
            remaining_basis_points: 8500,
            seconds_until_reset: 575_708,
            window_duration_minutes: 10_080,
            remaining_basis_points_per_day: 1275,
            even_pace_used_basis_points: 481,
            pace_variance_basis_points: 1019,
        })
    );
    assert_eq!(weekly[1].provider_window_name, "weekly_scoped");
    assert_eq!(
        weekly[1].provider_scope_name_option.as_deref(),
        Some("Fable")
    );
    assert_eq!(weekly[1].used_basis_points, 1200);
}

#[test]
fn claude_unrecognized_present_windows_are_retained_by_name() {
    let mut body: serde_json::Value = serde_json::from_str(claude_body()).expect("json");
    body["tangelo"] = serde_json::json!({ "utilization": 4.0, "resets_at": null });
    body["limits"]
        .as_array_mut()
        .expect("limits")
        .push(serde_json::json!({ "kind": "mystery", "group": "mystery", "percent": null }));
    let usage = ClaudeUsageDocument::from_body(&body.to_string(), None)
        .expect("parses")
        .normalize(observed());
    assert_eq!(
        usage.unrecognized_window_names,
        ["tangelo", "limits.mystery"]
    );
    assert_eq!(usage.plan_name_option, None);
}

#[test]
fn claude_without_limits_list_falls_back_to_named_windows() {
    let mut body: serde_json::Value = serde_json::from_str(claude_body()).expect("json");
    body.as_object_mut().expect("object").remove("limits");
    let usage = ClaudeUsageDocument::from_body(&body.to_string(), None)
        .expect("parses")
        .normalize(observed());
    let names: Vec<&str> = usage
        .quota_limits
        .iter()
        .map(|limit| limit.quota_windows[0].provider_window_name.as_str())
        .collect();
    assert_eq!(names, ["five_hour", "seven_day"]);
    assert_eq!(
        usage.quota_limits[1].quota_windows[0].window_duration_basis,
        WindowDurationBasis::ProviderWindowNamed(10_080)
    );
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
fn codex_every_limit_and_every_declared_window_is_enumerated() {
    let result: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/usage/codex-rate-limits.json")).expect("json");
    let usage = CodexRateLimitsDocument::new(result, vec![".codex".into(), ".codex-next".into()])
        .normalize(observed());
    assert_eq!(usage.usage_provider, UsageProvider::Codex);
    assert_eq!(usage.plan_name_option.as_deref(), Some("pro"));
    assert_eq!(usage.account_homes, [".codex", ".codex-next"]);
    assert_eq!(
        usage.unrecognized_window_names,
        ["codex_bengalfox.individualLimit"]
    );
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
        primary.pace_derivation,
        PaceDerivation::Derived(QuotaPace {
            remaining_basis_points: 7800,
            seconds_until_reset: 518_897,
            window_duration_minutes: 10_080,
            remaining_basis_points_per_day: 1298,
            even_pace_used_basis_points: 1420,
            pace_variance_basis_points: 780,
        })
    );

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
}

fn reading(reset: ResetBasis, duration: WindowDurationBasis) -> WindowReading {
    WindowReading {
        name: "window".into(),
        scope: None,
        used_basis_points: 4000,
        reset_basis: reset,
        duration_basis: duration,
    }
}

#[test]
fn pace_is_unknown_whenever_an_operand_is_unknown_or_stale() {
    let cases = [
        (
            reading(
                ResetBasis::Unknown,
                WindowDurationBasis::ProviderDeclared(300),
            ),
            PaceUnknownReason::ResetUnknown,
            WindowFreshness::Current,
        ),
        (
            reading(
                ResetBasis::ProviderResetTime(1_000_100),
                WindowDurationBasis::Unknown,
            ),
            PaceUnknownReason::WindowDurationUnknown,
            WindowFreshness::Current,
        ),
        (
            reading(
                ResetBasis::ProviderResetTime(999_000),
                WindowDurationBasis::ProviderDeclared(300),
            ),
            PaceUnknownReason::ResetPassed,
            WindowFreshness::ResetPassed,
        ),
        (
            reading(
                ResetBasis::ProviderResetTime(1_100_000),
                WindowDurationBasis::ProviderDeclared(300),
            ),
            PaceUnknownReason::ResetBeyondWindow,
            WindowFreshness::Current,
        ),
    ];
    for (reading, reason, freshness) in cases {
        let window = reading.normalize_at(1_000_000);
        assert_eq!(window.pace_derivation, PaceDerivation::Unknown(reason));
        assert_eq!(window.window_freshness, freshness);
        assert_eq!(window.remaining_basis_points, 6000);
        assert_eq!(window.absolute_limit, AbsoluteLimit::NotExposedByProvider);
    }
}

#[test]
fn overage_leaves_no_negative_remaining() {
    let mut over = reading(
        ResetBasis::ProviderResetTime(1_009_000),
        WindowDurationBasis::ProviderDeclared(300),
    );
    over.used_basis_points = 10_400;
    let window = over.normalize_at(1_000_000);
    assert_eq!(window.remaining_basis_points, 0);
    let PaceDerivation::Derived(pace) = window.pace_derivation else {
        panic!("all operands known");
    };
    assert_eq!(pace.remaining_basis_points_per_day, 0);
    assert_eq!(pace.even_pace_used_basis_points, 5000);
    assert_eq!(pace.pace_variance_basis_points, 5400);
}
