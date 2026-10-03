//! The usage snapshot's provider sources against recorded providers: a fake
//! Claude endpoint, fake Codex app-servers on real Unix sockets, and fixture
//! home directories.

#[path = "support/usage_fixtures.rs"]
mod usage_fixtures;

use std::time::Duration;

use datom_codec::Datomizable;
use harness::usage::{
    AppServerTimeout, ClaudeLiveSessions, ClaudeUsageEndpoint, ClaudeUsageSource, CodexLiveThreads,
    CodexUsageSource, ContextSource, LocalZone, ObservationInstant, QuotaSource, UsageHome,
    UsageSnapshotReader, UsageSnapshotReading,
};
use protos::{Protosizable, Textualizable};
use serde_json::{Value, json};
use signal_harness::{
    ContextBasis, ContextFreshness, ContextSourceFailureReason, ContextUnavailableReason,
    PlanningProjection, SessionContextObservation, SubscriptionObservation, UsageProvider,
    UsageUnavailableReason,
};
use usage_fixtures::{
    AppServerBehavior, FIXTURE_TOKEN, FakeAppServer, FakeClaudeEndpoint, FixtureHome,
};

const CLAUDE_BODY: &str = include_str!("fixtures/usage/claude-usage.json");
const THREAD_BOUND: &str = "01a10384-16ed-7dd2-93b3-b6bd66c26f85";
const THREAD_SUPERSEDED: &str = "01a10384-b7af-7c53-bf9d-57d84676f3ec";
const THREAD_UNBOUND: &str = "01a10384-ed06-7e10-944c-cd0a3e2e0b80";

fn future_milliseconds() -> i64 {
    ObservationInstant::now().nanoseconds() / 1_000_000 + 3_600_000
}

fn rate_limits() -> Value {
    serde_json::from_str(include_str!("fixtures/usage/codex-rate-limits.json")).expect("json")
}

fn reasons(
    observations: &[SubscriptionObservation],
) -> Vec<(Option<String>, UsageUnavailableReason)> {
    observations
        .iter()
        .filter_map(|observation| match observation {
            SubscriptionObservation::Unavailable(unavailable) => Some((
                unavailable.account_home_option.clone(),
                unavailable.usage_unavailable_reason.clone(),
            )),
            SubscriptionObservation::Observed(_) => None,
        })
        .collect()
}

fn claude_quota(home: &FixtureHome, endpoint: &FakeClaudeEndpoint) -> Vec<SubscriptionObservation> {
    ClaudeUsageSource::new(
        UsageHome::new(home.path()),
        ClaudeUsageEndpoint::recorded(endpoint.url.clone()),
        LocalZone::named("UTC"),
    )
    .observe_quota(ObservationInstant::now())
}

#[test]
fn claude_token_reaches_only_the_authorization_header() {
    let home = FixtureHome::new();
    home.claude_credentials(future_milliseconds());
    let endpoint = FakeClaudeEndpoint::start(200, CLAUDE_BODY);
    let observations = claude_quota(&home, &endpoint);
    let SubscriptionObservation::Observed(usage) = &observations[0] else {
        panic!("Claude usage observed: {observations:?}");
    };
    assert_eq!(usage.plan_name_option.as_deref(), Some("max"));
    assert_eq!(usage.quota_limits.len(), 4);

    let requests = endpoint.requests.lock().expect("requests").clone();
    assert_eq!(requests.len(), 1);
    let head = requests[0].to_ascii_lowercase();
    assert!(head.contains(&format!("authorization: bearer {FIXTURE_TOKEN}")));
    assert!(head.contains("anthropic-beta: oauth-2025-04-20"));
    assert!(!head.contains("refresh"));

    let reply = format!("{observations:?}");
    assert!(!reply.contains(FIXTURE_TOKEN));
    assert!(!reply.contains(".credentials.json"));
}

#[test]
fn expired_claude_token_is_reported_and_never_sent() {
    let home = FixtureHome::new();
    home.claude_credentials(ObservationInstant::now().nanoseconds() / 1_000_000 - 1000);
    let endpoint = FakeClaudeEndpoint::start(200, CLAUDE_BODY);
    let observations = claude_quota(&home, &endpoint);
    assert_eq!(
        reasons(&observations),
        [(None, UsageUnavailableReason::AccessTokenExpired)]
    );
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(endpoint.request_count(), 0);
}

#[test]
fn claude_credential_and_provider_failures_are_typed_categories() {
    let absent = FixtureHome::new();
    let endpoint = FakeClaudeEndpoint::start(200, CLAUDE_BODY);
    assert_eq!(
        reasons(&claude_quota(&absent, &endpoint)),
        [(None, UsageUnavailableReason::CredentialsAbsent)]
    );

    let garbled = FixtureHome::new();
    garbled.write(".claude/.credentials.json", "{ not json");
    assert_eq!(
        reasons(&claude_quota(&garbled, &endpoint)),
        [(None, UsageUnavailableReason::CredentialsUnreadable)]
    );

    let rejected = FixtureHome::new();
    rejected.claude_credentials(future_milliseconds());
    let refusing = FakeClaudeEndpoint::start(401, "{\"error\":\"echo me if you dare\"}");
    let observations = claude_quota(&rejected, &refusing);
    assert_eq!(
        reasons(&observations),
        [(None, UsageUnavailableReason::ProviderRejected)]
    );
    assert!(!format!("{observations:?}").contains("echo me"));

    let broken = FixtureHome::new();
    broken.claude_credentials(future_milliseconds());
    let garbage = FakeClaudeEndpoint::start(200, "<html>maintenance</html>");
    assert_eq!(
        reasons(&claude_quota(&broken, &garbage)),
        [(None, UsageUnavailableReason::ProviderResponseUnreadable)]
    );
}

/// Six Codex homes: two answering for one account, one hung, one with no
/// socket, one rejecting, and one with a stale socket file.
fn codex_homes(home: &FixtureHome) {
    let answering = || {
        AppServerBehavior::answering(vec![
            ("account/rateLimits/read", rate_limits()),
            (
                "thread/loaded/list",
                json!({ "data": [THREAD_BOUND], "nextCursor": null }),
            ),
        ])
    };
    FakeAppServer::start(&home.codex_socket(".codex"), answering());
    FakeAppServer::start(&home.codex_socket(".codex-next"), answering());
    FakeAppServer::start(
        &home.codex_socket(".codex-hung"),
        AppServerBehavior::Hanging,
    );
    home.codex_home(".codex-gone");
    FakeAppServer::start(
        &home.codex_socket(".codex-rejecting"),
        AppServerBehavior::answering(vec![]),
    );
    FakeAppServer::stale(&home.codex_socket(".codex-stale"));
}

#[test]
fn codex_same_account_homes_are_one_subscription_and_failures_stay_per_home() {
    let home = FixtureHome::new();
    codex_homes(&home);
    let observations = CodexUsageSource::new(
        UsageHome::new(home.path()),
        AppServerTimeout::new(Duration::from_millis(400)),
        LocalZone::named("UTC"),
    )
    .observe_quota(ObservationInstant::now());
    let observed: Vec<_> = observations
        .iter()
        .filter_map(|observation| match observation {
            SubscriptionObservation::Observed(usage) => Some(usage),
            SubscriptionObservation::Unavailable(_) => None,
        })
        .collect();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].account_homes, [".codex", ".codex-next"]);
    assert_eq!(observed[0].quota_limits.len(), 2);
    assert!(!format!("{observations:?}").contains("00000000-0000-4000-8000-000000000001"));
    assert_eq!(
        reasons(&observations),
        [
            (
                Some(".codex-gone".into()),
                UsageUnavailableReason::NoLiveControlSocket
            ),
            (
                Some(".codex-hung".into()),
                UsageUnavailableReason::TransportTimedOut
            ),
            (
                Some(".codex-rejecting".into()),
                UsageUnavailableReason::ProviderRejected
            ),
            (
                Some(".codex-stale".into()),
                UsageUnavailableReason::NoLiveControlSocket
            ),
        ]
    );
}

#[test]
fn no_codex_home_is_reported_unavailable() {
    let home = FixtureHome::new();
    let observations = CodexUsageSource::new(
        UsageHome::new(home.path()),
        AppServerTimeout::default(),
        LocalZone::named("UTC"),
    )
    .observe_quota(ObservationInstant::now());
    assert_eq!(
        reasons(&observations),
        [(None, UsageUnavailableReason::NoLiveControlSocket)]
    );
}

fn rollout(home: &FixtureHome, thread: &str, superseded: bool) -> String {
    let mut lines = vec![
        json!({ "timestamp": "2026-10-03T21:00:00.000Z", "type": "session_meta", "payload": { "id": thread } }),
        json!({ "timestamp": "2026-10-03T21:07:27.157Z", "type": "event_msg", "payload": {
            "type": "token_count",
            "info": { "last_token_usage": { "input_tokens": 76_239, "output_tokens": 49 },
                      "model_context_window": 258_400 } } }),
    ];
    if superseded {
        lines.push(
            json!({ "timestamp": "2026-10-03T21:08:00.000Z", "type": "response_item",
            "payload": { "type": "message", "role": "user" } }),
        );
    }
    let text: Vec<String> = lines.iter().map(Value::to_string).collect();
    home.write(
        &format!(".codex-next/sessions/rollout-2026-10-03T14-45-50-{thread}.jsonl"),
        &(text.join("\n") + "\n"),
    )
    .display()
    .to_string()
}

#[test]
fn codex_loaded_threads_report_bound_context_and_unbound_threads() {
    let home = FixtureHome::new();
    let bound = rollout(&home, THREAD_BOUND, false);
    let superseded = rollout(&home, THREAD_SUPERSEDED, true);
    let thread = |id: &str, path: &str, name: Value| {
        (
            format!("thread/read {id}"),
            json!({ "thread": { "id": id, "path": path, "model": "gpt-6-astra", "name": name } }),
        )
    };
    let table = vec![
        (
            "thread/loaded/list".to_owned(),
            json!({ "data": [THREAD_BOUND, THREAD_SUPERSEDED, THREAD_UNBOUND], "nextCursor": null }),
        ),
        thread(THREAD_BOUND, &bound, json!("Mind.{ Astra d66c26 }")),
        thread(THREAD_SUPERSEDED, &superseded, Value::Null),
        thread(THREAD_UNBOUND, &bound, Value::Null),
    ];
    let behavior = AppServerBehavior::Answering(std::sync::Arc::new(table));
    FakeAppServer::start(&home.codex_socket(".codex"), behavior.clone());
    FakeAppServer::start(&home.codex_socket(".codex-next"), behavior);
    let observations =
        CodexLiveThreads::new(UsageHome::new(home.path()), AppServerTimeout::default())
            .observe_context(ObservationInstant::now());
    assert_eq!(
        observations.len(),
        3,
        "each loaded thread once: {observations:?}"
    );

    let SessionContextObservation::Observed(first) = &observations[0] else {
        panic!("bound thread observed");
    };
    assert_eq!(first.usage_provider, UsageProvider::Codex);
    // A Flow-shaped thread name is display metadata, not a witnessed binding.
    assert_eq!(first.flow_identifier_option, None);
    assert_eq!(
        first.session_name_option.as_deref(),
        Some("Mind.{ Astra d66c26 }")
    );
    assert_eq!(
        first.context_basis,
        ContextBasis::CodexRolloutLastTokenCount
    );
    assert_eq!(first.context_freshness, ContextFreshness::Proxy);
    assert_eq!(first.context_tokens_option, Some(76_239));
    assert_eq!(first.context_window_tokens_option, Some(258_400));
    assert_eq!(first.context_used_basis_points_option, Some(2950));
    assert_eq!(first.event_time_option, Some(1_791_061_647_157_000_000));

    let SessionContextObservation::Observed(second) = &observations[1] else {
        panic!("superseded thread observed");
    };
    assert_eq!(second.context_freshness, ContextFreshness::Superseded);
    assert_eq!(second.flow_identifier_option, None);

    let SessionContextObservation::Unavailable(third) = &observations[2] else {
        panic!("path mismatch is unbound");
    };
    assert_eq!(
        third.context_unavailable_reason,
        ContextUnavailableReason::ThreadUnbound
    );
}

fn claude_session(home: &FixtureHome, pid: u64, session: &str, transcript: Option<Vec<Value>>) {
    home.write(
        &format!(".claude/sessions/{pid}.json"),
        &json!({ "pid": pid, "sessionId": session, "cwd": "/work/primary.x",
                 "name": "Psyche.{ Opus 28d847 }", "kind": "interactive" })
        .to_string(),
    );
    if let Some(records) = transcript {
        let text: Vec<String> = records.iter().map(Value::to_string).collect();
        home.write(
            &format!(".claude/projects/-work-primary-x/{session}.jsonl"),
            &(text.join("\n") + "\n"),
        );
    }
}

#[test]
fn claude_live_sessions_report_transcript_proxy_and_its_states() {
    let home = FixtureHome::new();
    let live = u64::from(std::process::id());
    let assistant = |session: &str| {
        json!({ "type": "assistant", "sessionId": session, "timestamp": "2026-10-03T21:04:51.000Z",
                "message": { "model": "claude-opus-5-5", "content": "never emitted",
                             "usage": { "input_tokens": 10, "cache_creation_input_tokens": 200,
                                        "cache_read_input_tokens": 3000, "output_tokens": 5 } } })
    };
    let first = "28d847ee-f350-4a24-8cb0-0e88abf16cbe";
    let second = "5ed94b76-31d3-471b-99c2-1e7ed36a1893";
    let third = "6e782cf5-4c64-4753-8a3e-39d59e414a37";
    claude_session(&home, live, first, Some(vec![assistant(first)]));
    home.write(&format!(".claude/sessions/{live}.key"), "never read");
    claude_session(&home, 999_999_999, second, Some(vec![assistant(second)]));
    let other = FixtureHome::new();
    claude_session(
        &other,
        live,
        second,
        Some(vec![
            assistant(second),
            json!({ "type": "user", "sessionId": second, "message": { "content": "never emitted" } }),
        ]),
    );
    claude_session(&other, live + 1_000_000_000, third, None);

    let observations = ClaudeLiveSessions::new(UsageHome::new(home.path()))
        .observe_context(ObservationInstant::now());
    assert_eq!(
        observations.len(),
        1,
        "dead pid passed over: {observations:?}"
    );
    let SessionContextObservation::Observed(context) = &observations[0] else {
        panic!("live session observed");
    };
    // A session identifier's prefix is not a witnessed Flow binding.
    assert_eq!(context.flow_identifier_option, None);
    assert_eq!(context.context_freshness, ContextFreshness::Proxy);
    assert_eq!(context.context_tokens_option, Some(3210));
    assert_eq!(context.context_window_tokens_option, None);
    assert_eq!(context.context_used_basis_points_option, None);
    assert_eq!(
        context.model_identifier_option.as_deref(),
        Some("claude-opus-5-5")
    );
    assert!(!format!("{observations:?}").contains("never emitted"));

    let superseded = ClaudeLiveSessions::new(UsageHome::new(other.path()))
        .observe_context(ObservationInstant::now());
    let SessionContextObservation::Observed(context) = &superseded[0] else {
        panic!("superseded session observed");
    };
    assert_eq!(context.context_freshness, ContextFreshness::Superseded);
}

#[test]
fn claude_session_without_transcript_is_unavailable() {
    let home = FixtureHome::new();
    claude_session(
        &home,
        u64::from(std::process::id()),
        "6e782cf5-4c64-4753-8a3e-39d59e414a37",
        None,
    );
    let observations = ClaudeLiveSessions::new(UsageHome::new(home.path()))
        .observe_context(ObservationInstant::now());
    let SessionContextObservation::Unavailable(unavailable) = &observations[0] else {
        panic!("absent transcript is unavailable");
    };
    assert_eq!(
        unavailable.context_unavailable_reason,
        ContextUnavailableReason::TranscriptAbsent
    );
}

#[test]
fn one_read_carries_both_providers_when_one_fails() {
    let home = FixtureHome::new();
    home.claude_credentials(ObservationInstant::now().nanoseconds() / 1_000_000 - 1000);
    FakeAppServer::start(
        &home.codex_socket(".codex"),
        AppServerBehavior::answering(vec![("account/rateLimits/read", rate_limits())]),
    );
    let endpoint = FakeClaudeEndpoint::start(200, CLAUDE_BODY);
    let snapshot = UsageSnapshotReader::with_endpoint(
        UsageHome::new(home.path()),
        ClaudeUsageEndpoint::recorded(endpoint.url.clone()),
        LocalZone::named("UTC"),
    )
    .read_snapshot();
    assert_eq!(
        snapshot.planning_projection,
        PlanningProjection::NotConfigured
    );
    let providers: Vec<(UsageProvider, bool)> = snapshot
        .subscription_observations
        .iter()
        .map(|observation| match observation {
            SubscriptionObservation::Observed(usage) => (usage.usage_provider.clone(), true),
            SubscriptionObservation::Unavailable(unavailable) => {
                (unavailable.usage_provider.clone(), false)
            }
        })
        .collect();
    assert_eq!(
        providers,
        [(UsageProvider::Claude, false), (UsageProvider::Codex, true)]
    );
    assert!(
        snapshot
            .subscription_observations
            .iter()
            .all(|observation| match observation {
                SubscriptionObservation::Observed(usage) =>
                    usage.observation_time >= snapshot.snapshot_time,
                SubscriptionObservation::Unavailable(unavailable) => {
                    unavailable.observation_time >= snapshot.snapshot_time
                }
            })
    );

    let text = signal_harness::Response::UsageSnapshot(snapshot)
        .datomize(vec![])
        .protosize()
        .textualize();
    assert!(text.starts_with("UsageSnapshot.{"));
    for forbidden in [
        FIXTURE_TOKEN,
        ".credentials.json",
        "auth.json",
        "accountId",
        "00000000-0000-4000",
    ] {
        assert!(!text.contains(forbidden), "{forbidden} leaked into {text}");
    }
}

fn source_failures(
    observations: &[SessionContextObservation],
) -> Vec<(Option<String>, ContextSourceFailureReason)> {
    observations
        .iter()
        .filter_map(|observation| match observation {
            SessionContextObservation::SourceUnavailable(unavailable) => Some((
                unavailable.account_home_option.clone(),
                unavailable.context_source_failure_reason.clone(),
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn every_attempted_codex_context_source_reports_its_failure() {
    let home = FixtureHome::new();
    codex_homes(&home);
    let observations = CodexLiveThreads::new(
        UsageHome::new(home.path()),
        AppServerTimeout::new(Duration::from_millis(400)),
    )
    .observe_context(ObservationInstant::now());
    assert_eq!(
        source_failures(&observations),
        [
            (
                Some(".codex-gone".into()),
                ContextSourceFailureReason::NoLiveControlSocket
            ),
            (
                Some(".codex-hung".into()),
                ContextSourceFailureReason::TransportTimedOut
            ),
            (
                Some(".codex-rejecting".into()),
                ContextSourceFailureReason::ProviderRejected
            ),
            (
                Some(".codex-stale".into()),
                ContextSourceFailureReason::NoLiveControlSocket
            ),
        ]
    );

    let empty = FixtureHome::new();
    let observations =
        CodexLiveThreads::new(UsageHome::new(empty.path()), AppServerTimeout::default())
            .observe_context(ObservationInstant::now());
    assert_eq!(
        source_failures(&observations),
        [(None, ContextSourceFailureReason::NoLiveControlSocket)]
    );
}

#[test]
fn an_absent_or_unreadable_claude_registry_is_reported_not_skipped() {
    let absent = FixtureHome::new();
    let observations = ClaudeLiveSessions::new(UsageHome::new(absent.path()))
        .observe_context(ObservationInstant::now());
    assert_eq!(
        source_failures(&observations),
        [(None, ContextSourceFailureReason::RegistryAbsent)]
    );

    let garbled = FixtureHome::new();
    garbled.write(".claude/sessions/12345.json", "{ not json");
    let observations = ClaudeLiveSessions::new(UsageHome::new(garbled.path()))
        .observe_context(ObservationInstant::now());
    assert_eq!(
        source_failures(&observations),
        [(None, ContextSourceFailureReason::RegistryEntryUnreadable)]
    );
}

#[test]
fn a_reader_that_cannot_run_reports_every_collector_failed() {
    let snapshot = UsageSnapshotReader::failed_snapshot();
    assert!(
        snapshot
            .subscription_observations
            .iter()
            .all(|observation| matches!(
                observation,
                SubscriptionObservation::Unavailable(unavailable)
                    if unavailable.usage_unavailable_reason
                        == UsageUnavailableReason::CollectorFailed
            ))
    );
    assert_eq!(snapshot.subscription_observations.len(), 2);
    assert_eq!(
        source_failures(&snapshot.session_context_observations),
        [
            (None, ContextSourceFailureReason::CollectorFailed),
            (None, ContextSourceFailureReason::CollectorFailed),
        ]
    );
}
