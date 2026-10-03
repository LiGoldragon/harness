//! The one-call usage snapshot end to end: the real `harness-daemon`, started
//! with an empty instance set on a fixture home, answers `UsageSnapshotQuery`
//! at daemon scope, and both installed clients — `harness UsageSnapshotQuery`
//! and `harness-usage` — print it. The user-service launcher writes the same
//! shape of configuration and becomes that daemon.

#[path = "support/usage_fixtures.rs"]
mod usage_fixtures;

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use harness::usage::ObservationInstant;
use harness::{HarnessDaemonConfigurationFile, SignalWire};
use serde_json::{Value, json};
use signal_harness::{
    HarnessDaemonConfiguration, PlanningProjection, Query, Response, SubscriptionObservation,
    UsageProvider, UsageUnavailableReason,
};
use signal_persona::OwnerIdentity;
use usage_fixtures::{AppServerBehavior, FIXTURE_TOKEN, FakeAppServer, FixtureHome};

struct UsageDaemon {
    child: Child,
    socket: PathBuf,
    _home: FixtureHome,
}

impl UsageDaemon {
    fn start() -> Self {
        let home = FixtureHome::new();
        home.claude_credentials(ObservationInstant::now().nanoseconds() / 1_000_000 - 1000);
        let rate_limits: Value =
            serde_json::from_str(include_str!("fixtures/usage/codex-rate-limits.json"))
                .expect("json");
        FakeAppServer::start(
            &home.codex_socket(".codex"),
            AppServerBehavior::answering(vec![
                ("account/rateLimits/read", rate_limits),
                (
                    "thread/loaded/list",
                    json!({ "data": [], "nextCursor": null }),
                ),
            ]),
        );
        let runtime = home.path().join("run");
        std::fs::create_dir_all(&runtime).expect("runtime directory");
        let socket = runtime.join("harness.sock");
        let configuration = HarnessDaemonConfiguration {
            domain_socket_path: socket.display().to_string(),
            domain_socket_mode: 0o600,
            meta_socket_path: runtime.join("meta-harness.sock").display().to_string(),
            meta_socket_mode: 0o600,
            engine_management_socket_path: runtime.join("supervision.sock").display().to_string(),
            engine_management_socket_mode: 0o600,
            owner_identity: OwnerIdentity::UnixUser(1000),
            harness_instance_configurations: Vec::new(),
        };
        let configuration_path = runtime.join("harness-daemon.rkyv");
        HarnessDaemonConfigurationFile::new(configuration_path.clone())
            .write_configuration(&configuration)
            .expect("configuration writes");
        let child = Command::new(env!("CARGO_BIN_EXE_harness-daemon"))
            .arg(&configuration_path)
            .env("HOME", home.path())
            .env("TZ", "UTC")
            .spawn()
            .expect("harness-daemon starts");
        Self::wait_for(&socket);
        Self {
            child,
            socket,
            _home: home,
        }
    }

    fn wait_for(socket: &Path) {
        let started = Instant::now();
        while !socket.exists() {
            assert!(started.elapsed() < Duration::from_secs(5), "socket bound");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for UsageDaemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn the_daemon_answers_the_usage_query_without_any_configured_instance() {
    let daemon = UsageDaemon::start();
    let mut stream =
        std::os::unix::net::UnixStream::connect(&daemon.socket).expect("client connects");
    let wire = SignalWire::default();
    wire.write(&mut stream, &Query::UsageSnapshotQuery)
        .expect("query writes");
    let Response::UsageSnapshot(snapshot) = wire.read(&mut stream).expect("reply reads") else {
        panic!("a usage snapshot answers the usage query");
    };
    assert_eq!(
        snapshot.planning_projection,
        PlanningProjection::NotConfigured
    );
    let providers: Vec<(UsageProvider, Option<UsageUnavailableReason>)> = snapshot
        .subscription_observations
        .iter()
        .map(|observation| match observation {
            SubscriptionObservation::Observed(usage) => (usage.usage_provider.clone(), None),
            SubscriptionObservation::Unavailable(unavailable) => (
                unavailable.usage_provider.clone(),
                Some(unavailable.usage_unavailable_reason.clone()),
            ),
        })
        .collect();
    assert_eq!(
        providers,
        [
            (
                UsageProvider::Claude,
                Some(UsageUnavailableReason::AccessTokenExpired)
            ),
            (UsageProvider::Codex, None),
        ]
    );
    assert!(!format!("{snapshot:?}").contains(FIXTURE_TOKEN));
}

#[test]
fn both_clients_print_the_snapshot_in_one_call() {
    let daemon = UsageDaemon::start();
    let typed = Command::new(env!("CARGO_BIN_EXE_harness"))
        .env("HARNESS_SOCKET", &daemon.socket)
        .arg("UsageSnapshotQuery")
        .output()
        .expect("harness runs");
    assert!(
        typed.status.success(),
        "{}",
        String::from_utf8_lossy(&typed.stderr)
    );
    let typed = String::from_utf8(typed.stdout).expect("utf8");
    assert!(typed.starts_with("UsageSnapshot.{"), "{typed}");
    assert!(typed.contains("AccessTokenExpired"), "{typed}");

    let view = Command::new(env!("CARGO_BIN_EXE_harness-usage"))
        .env("HARNESS_SOCKET", &daemon.socket)
        .output()
        .expect("harness-usage runs");
    assert!(
        view.status.success(),
        "{}",
        String::from_utf8_lossy(&view.stderr)
    );
    let view = String::from_utf8(view.stdout).expect("utf8");
    assert!(
        view.starts_with("Claude: unavailable (access token expired)\n"),
        "{view}"
    );
    assert!(view.contains("Codex (pro) · homes .codex\n"), "{view}");
    assert!(view.contains("  codex / primary: "), "{view}");
    assert!(view.contains("use remaining by reset: "), "{view}");
    assert!(view.contains(" UTC (UTC+00:00)"), "{view}");
    for forbidden in [FIXTURE_TOKEN, ".credentials.json", "00000000-0000-4000"] {
        assert!(!view.contains(forbidden), "{forbidden} leaked: {view}");
        assert!(!typed.contains(forbidden), "{forbidden} leaked: {typed}");
    }
}

#[test]
fn the_user_service_launcher_writes_its_typed_configuration_and_becomes_the_daemon() {
    use std::os::unix::fs::PermissionsExt;

    let home = FixtureHome::new();
    let runtime = home.path().join("run-harness");
    std::fs::create_dir_all(&runtime).expect("runtime directory");
    let mut launcher = Command::new(env!("CARGO_BIN_EXE_harness-daemon-launch"))
        .env("RUNTIME_DIRECTORY", &runtime)
        .env("HOME", home.path())
        .env("TZ", "UTC")
        .spawn()
        .expect("launcher starts");
    let socket = runtime.join("harness.sock");
    for name in ["harness.sock", "meta-harness.sock", "supervision.sock"] {
        UsageDaemon::wait_for(&runtime.join(name));
        let mode = std::fs::metadata(runtime.join(name))
            .expect("socket metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "{name} is owner-only");
    }
    let configuration = HarnessDaemonConfigurationFile::new(runtime.join("harness-daemon.rkyv"))
        .configuration()
        .expect("written configuration decodes");
    assert_eq!(
        configuration,
        harness::UserServiceLaunch::new(&runtime, created_file_owner()).configuration()
    );
    assert!(configuration.harness_instance_configurations.is_empty());
    let mode = std::fs::metadata(runtime.join("harness-daemon.rkyv"))
        .expect("configuration metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600);

    let mut stream = std::os::unix::net::UnixStream::connect(&socket).expect("client connects");
    let wire = SignalWire::default();
    wire.write(&mut stream, &Query::UsageSnapshotQuery)
        .expect("query writes");
    assert!(matches!(
        wire.read(&mut stream).expect("reply reads"),
        Response::UsageSnapshot(_)
    ));
    let _ = launcher.kill();
    let _ = launcher.wait();
}

#[test]
fn the_launcher_refuses_without_a_service_runtime_directory_or_with_an_argument() {
    let without = Command::new(env!("CARGO_BIN_EXE_harness-daemon-launch"))
        .env_remove("RUNTIME_DIRECTORY")
        .output()
        .expect("launcher runs");
    assert!(!without.status.success());
    assert!(String::from_utf8_lossy(&without.stderr).contains("RUNTIME_DIRECTORY"));

    let with_argument = Command::new(env!("CARGO_BIN_EXE_harness-daemon-launch"))
        .arg("anything")
        .output()
        .expect("launcher runs");
    assert!(!with_argument.status.success());
    assert!(String::from_utf8_lossy(&with_argument.stderr).contains("takes no argument"));
}

/// This test process's uid, as the owner of a file it creates.
fn created_file_owner() -> u32 {
    use std::os::unix::fs::MetadataExt;
    let probe = tempfile::NamedTempFile::new().expect("probe file");
    probe.as_file().metadata().expect("probe metadata").uid()
}
