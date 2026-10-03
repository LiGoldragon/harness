//! The component CLIs against fake sockets: each sends one Signal frame of its
//! contract's `Query`, built from one inline Datom argument, and prints the
//! `Response` it gets back — as Datom for `harness` and `meta-harness`, and as
//! the human view for `harness-usage`.

use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::Command;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use harness::{DatomPrint, SignalWire};
use meta_signal_harness::{
    MetaOperationKind, Query as MetaHarnessRequest, RequestUnimplemented,
    Response as MetaHarnessReply, UnimplementedReason,
};
use signal_harness::{
    AbsoluteLimit, HarnessDaemonConfiguration, HarnessHealth, HarnessReadiness, HarnessStatus,
    HarnessStatusQuery, LocalReset, LocalResetTime, PeriodSemantics, PlanningProjection,
    Query as HarnessRequest, QuotaLimit, QuotaShare, QuotaWindow, RateBasis, RateRounding,
    RemainderByResetRate, RemainderRateDerivation, RemainderRateUnknownReason, ResetBasis,
    ResetCountdown, Response as HarnessEvent, ShareConversion, SubscriptionObservation,
    SubscriptionUsage, UniformRateDerivation, UniformRateUnknownReason, UsageProvider,
    UsageSnapshot, UsageSource, UsageUnavailable, UsageUnavailableReason, WindowDurationBasis,
    WindowUsage,
};
use signal_harness::{ElapsedUnknownReason, ElapsedWindowDerivation};
use signal_persona::OwnerIdentity;

#[derive(Debug)]
struct CliSocketFixture {
    root: PathBuf,
}

impl CliSocketFixture {
    fn new(name: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after Unix epoch")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("harness-cli-{name}-{}-{now}", std::process::id()));
        std::fs::create_dir_all(&root).expect("create harness cli fixture directory");
        Self { root }
    }

    fn socket(&self) -> PathBuf {
        self.root.join("harness.sock")
    }

    fn meta_socket(&self) -> PathBuf {
        self.root.join("meta-harness.sock")
    }

    fn configuration(&self) -> HarnessDaemonConfiguration {
        HarnessDaemonConfiguration {
            domain_socket_path: self.socket().display().to_string(),
            domain_socket_mode: 0o600,
            meta_socket_path: self.meta_socket().display().to_string(),
            meta_socket_mode: 0o600,
            engine_management_socket_path: self.root.join("supervision.sock").display().to_string(),
            engine_management_socket_mode: 0o600,
            owner_identity: OwnerIdentity::UnixUser(1000),
            harness_instance_configurations: Vec::new(),
        }
    }
}

impl Drop for CliSocketFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// One fake server connection: read one query frame, answer one reply frame.
struct FakeServer;

impl FakeServer {
    fn answer<Query, Reply>(
        listener: UnixListener,
        expected: Query,
        reply: Reply,
    ) -> thread::JoinHandle<()>
    where
        Query: std::fmt::Debug + PartialEq + Send + 'static,
        Reply: Send + 'static,
        signal::Signal<Query>: signal::Restorable<Query>,
        Reply: signal::Signalizable,
    {
        thread::spawn(move || {
            let (mut stream, _address) = listener.accept().expect("cli connects");
            let request: Query = SignalWire::default()
                .read(&mut stream)
                .expect("read one query frame");
            assert_eq!(request, expected);
            SignalWire::default()
                .write(&mut stream, &reply)
                .expect("write one reply frame");
        })
    }
}

#[test]
fn harness_cli_reaches_working_socket_and_prints_typed_reply() {
    let fixture = CliSocketFixture::new("working");
    let listener = UnixListener::bind(fixture.socket()).expect("fake harness socket binds");
    let query = HarnessRequest::HarnessStatusQuery(HarnessStatusQuery {
        harness_name: "operator".into(),
    });
    let server = FakeServer::answer(
        listener,
        query.clone(),
        HarnessEvent::HarnessStatus(HarnessStatus {
            harness_name: "operator".into(),
            harness_health: HarnessHealth::Running,
            harness_readiness: HarnessReadiness::Ready,
        }),
    );

    let output = Command::new(env!("CARGO_BIN_EXE_harness"))
        .env("HARNESS_SOCKET", fixture.socket())
        .arg(DatomPrint::of(&query).as_str())
        .output()
        .expect("run harness cli");

    assert!(
        output.status.success(),
        "harness cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("harness cli stdout is utf8");
    assert!(
        stdout.contains("HarnessStatus"),
        "unexpected stdout: {stdout}"
    );
    assert!(stdout.contains("Running"), "unexpected stdout: {stdout}");
    server.join().expect("fake harness server exits");
}

#[test]
fn harness_cli_takes_the_usage_query_as_its_one_inline_datom_value() {
    let fixture = CliSocketFixture::new("usage-datom");
    let listener = UnixListener::bind(fixture.socket()).expect("fake harness socket binds");
    let server = FakeServer::answer(
        listener,
        HarnessRequest::UsageSnapshotQuery,
        HarnessEvent::UsageSnapshot(snapshot()),
    );
    let output = Command::new(env!("CARGO_BIN_EXE_harness"))
        .env("HARNESS_SOCKET", fixture.socket())
        .arg("UsageSnapshotQuery")
        .output()
        .expect("run harness cli");
    assert!(
        output.status.success(),
        "harness cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    assert!(stdout.starts_with("UsageSnapshot.{"), "{stdout}");
    assert!(stdout.contains("OneSnapshotClockAllowance"), "{stdout}");
    server.join().expect("fake harness server exits");
}

#[test]
fn harness_cli_refuses_anything_but_one_argument() {
    for arguments in [vec![], vec!["UsageSnapshotQuery", "UsageSnapshotQuery"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_harness"))
            .env("HARNESS_SOCKET", "/nonexistent/harness.sock")
            .args(&arguments)
            .output()
            .expect("run harness cli");
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("exactly one inline Datom argument"),
            "{stderr}"
        );
    }
}

#[test]
fn meta_harness_cli_reaches_policy_socket_and_prints_typed_reply() {
    let fixture = CliSocketFixture::new("meta");
    let configuration = fixture.configuration();
    let listener =
        UnixListener::bind(fixture.meta_socket()).expect("fake meta-harness socket binds");
    let query = MetaHarnessRequest::Configure(configuration);
    let server = FakeServer::answer(
        listener,
        query.clone(),
        MetaHarnessReply::RequestUnimplemented(RequestUnimplemented {
            meta_operation_kind: MetaOperationKind::ConfigureDaemon,
            unimplemented_reason: UnimplementedReason::NotBuiltYet,
        }),
    );

    let output = Command::new(env!("CARGO_BIN_EXE_meta-harness"))
        .env("HARNESS_META_SOCKET", fixture.meta_socket())
        .arg(DatomPrint::of(&query).as_str())
        .output()
        .expect("run meta-harness cli");

    assert!(
        output.status.success(),
        "meta-harness cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("meta-harness cli stdout is utf8");
    assert!(
        stdout.contains("RequestUnimplemented"),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains("NotBuiltYet"),
        "unexpected stdout: {stdout}"
    );
    server.join().expect("fake meta-harness server exits");
}

fn snapshot() -> UsageSnapshot {
    let observed = 1_791_061_491_000_000_000;
    UsageSnapshot {
        snapshot_time: observed,
        planning_projection: PlanningProjection::NotConfigured,
        subscription_observations: vec![
            SubscriptionObservation::Observed(SubscriptionUsage {
                usage_provider: UsageProvider::Claude,
                usage_source: UsageSource::ClaudeOauthUsageEndpoint,
                observation_time: observed,
                plan_name_option: Some("max".into()),
                account_homes: vec![],
                quota_limits: vec![QuotaLimit {
                    quota_limit_identifier: "weekly".into(),
                    quota_limit_name_option: None,
                    quota_windows: vec![
                        QuotaWindow {
                            provider_window_name: "primary".into(),
                            provider_scope_name_option: None,
                            window_usage: WindowUsage::Current(QuotaShare {
                                used_basis_points: 6000,
                                remaining_basis_points: 4000,
                                share_conversion:
                                    ShareConversion::ProviderPercentRoundedToBasisPoint,
                            }),
                            reset_basis: ResetBasis::ProviderResetTime(1_791_079_491),
                            reset_countdown: ResetCountdown::Pending(18_000),
                            local_reset: LocalReset::Rendered(LocalResetTime {
                                local_date_time: "2026-10-04T02:04:51".into(),
                                timezone_name: "America/Mexico_City".into(),
                                utc_offset_seconds: -21_600,
                            }),
                            window_duration_basis: WindowDurationBasis::Unknown,
                            period_semantics: PeriodSemantics::NotEstablished,
                            absolute_limit: AbsoluteLimit::NotExposedByProvider,
                            remainder_rate_derivation: RemainderRateDerivation::Derived(
                                RemainderByResetRate {
                                    rate_basis: RateBasis::OneSnapshotClockAllowance,
                                    remaining_basis_points: 4000,
                                    seconds_until_reset: 18_000,
                                    remaining_basis_points_per_clock_hour: 800,
                                    remaining_basis_points_per_clock_day: 19_200,
                                    rate_rounding: RateRounding::TowardZero,
                                },
                            ),
                            uniform_rate_derivation: UniformRateDerivation::Unknown(
                                UniformRateUnknownReason::WindowDurationUnknown,
                            ),
                            elapsed_window_derivation: ElapsedWindowDerivation::Unknown(
                                ElapsedUnknownReason::PeriodSemanticsNotEstablished,
                            ),
                        },
                        QuotaWindow {
                            provider_window_name: "old".into(),
                            provider_scope_name_option: None,
                            window_usage: WindowUsage::StaleAfterReset(QuotaShare {
                                used_basis_points: 2200,
                                remaining_basis_points: 7800,
                                share_conversion:
                                    ShareConversion::ProviderPercentRoundedToBasisPoint,
                            }),
                            reset_basis: ResetBasis::ProviderResetTime(1_791_041_891),
                            reset_countdown: ResetCountdown::Passed(19_600),
                            local_reset: LocalReset::Rendered(LocalResetTime {
                                local_date_time: "2026-10-03T09:38:11".into(),
                                timezone_name: "America/Mexico_City".into(),
                                utc_offset_seconds: -21_600,
                            }),
                            window_duration_basis: WindowDurationBasis::Unknown,
                            period_semantics: PeriodSemantics::NotEstablished,
                            absolute_limit: AbsoluteLimit::NotExposedByProvider,
                            remainder_rate_derivation: RemainderRateDerivation::Unknown(
                                RemainderRateUnknownReason::UsageStale,
                            ),
                            uniform_rate_derivation: UniformRateDerivation::Unknown(
                                UniformRateUnknownReason::WindowDurationUnknown,
                            ),
                            elapsed_window_derivation: ElapsedWindowDerivation::Unknown(
                                ElapsedUnknownReason::PeriodSemanticsNotEstablished,
                            ),
                        },
                    ],
                }],
                unrecognized_window_names: vec![],
                unmodeled_source_facts: vec!["spend".into()],
            }),
            SubscriptionObservation::Unavailable(UsageUnavailable {
                usage_provider: UsageProvider::Codex,
                observation_time: observed,
                account_home_option: Some(".codex".into()),
                usage_unavailable_reason: UsageUnavailableReason::TransportTimedOut,
            }),
        ],
        session_context_observations: vec![],
    }
}

#[test]
fn usage_cli_leads_each_window_with_remaining_time_left_reset_and_rate() {
    let fixture = CliSocketFixture::new("usage-view");
    let listener = UnixListener::bind(fixture.socket()).expect("fake harness socket binds");
    let server = FakeServer::answer(
        listener,
        HarnessRequest::UsageSnapshotQuery,
        HarnessEvent::UsageSnapshot(snapshot()),
    );
    let output = Command::new(env!("CARGO_BIN_EXE_harness-usage"))
        .env("HARNESS_SOCKET", fixture.socket())
        .output()
        .expect("run harness-usage");
    assert!(
        output.status.success(),
        "harness-usage failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf8");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines[0], "Claude (max)");
    assert_eq!(
        lines[1],
        "  weekly / primary: 40% remaining · 5h 0m left · resets 2026-10-04 02:04:51 America/Mexico_City (UTC-06:00)"
    );
    assert_eq!(
        lines[2],
        "    use remaining by reset: 8.00 percentage points/hour, 192.00 per day (clock allowance from this snapshot, not observed burn)"
    );
    assert_eq!(
        lines[3],
        "    elapsed-window difference: unavailable (period semantics not established)"
    );
    assert!(
        lines[4]
            .starts_with("  weekly / old: stale: 22% used before a reset that passed 5h 26m ago"),
        "{stdout}"
    );
    assert!(lines[4].ends_with("not a fresh window"), "{stdout}");
    assert_eq!(
        lines[5],
        "    use remaining by reset: unavailable (usage stale)"
    );
    assert!(
        stdout.contains("  unmodeled source facts: spend\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("Codex .codex: unavailable (transport timed out)\n"),
        "{stdout}"
    );
    assert!(
        stdout.contains("planning projection: not configured\n"),
        "{stdout}"
    );
    server.join().expect("fake harness server exits");
}
