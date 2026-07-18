//! Session-launch witnesses: typed refusals and the fixture spawn leg
//! (spawn-time prompt delivery carrying the orchestrator-minted identity).

use std::time::{Duration, Instant};

use harness::launch::{FixtureLaunchCommand, SessionLauncher};
use meta_signal_harness::MetaHarnessReply;
use signal_harness::{
    AgentIdentityToken, ContinuationHandle, ContinuationRequest, HarnessKind, InitialPrompt,
    PiContinuationIdentifier, SessionLaunchRefusalReason, SessionLaunchRequest,
};

fn launch_request(
    kind: HarnessKind,
    continuation: ContinuationRequest,
    prompt: &str,
) -> SessionLaunchRequest {
    SessionLaunchRequest {
        harness_kind: kind,
        agent_identity: AgentIdentityToken::new("xk3f"),
        initial_prompt: InitialPrompt::new(prompt),
        continuation,
    }
}

#[test]
fn codex_launch_is_refused_typed() {
    let launcher = SessionLauncher::from_environment();
    let reply = launcher.launch(launch_request(
        HarnessKind::Codex,
        ContinuationRequest::Fresh,
        "You are agent xk3f.",
    ));
    match reply {
        MetaHarnessReply::SessionLaunchRefused(refused) => {
            assert_eq!(
                refused.reason,
                SessionLaunchRefusalReason::HarnessKindUnsupported
            );
        }
        other => panic!("expected typed refusal, got {other:?}"),
    }
}

#[test]
fn continuation_launch_is_refused_typed_until_the_resume_leg_lands() {
    let launcher = SessionLauncher::from_environment();
    let reply = launcher.launch(launch_request(
        HarnessKind::Pi,
        ContinuationRequest::Require(ContinuationHandle::Pi(PiContinuationIdentifier::new(
            "pi-session-1",
        ))),
        "You are agent xk3f.",
    ));
    match reply {
        MetaHarnessReply::SessionLaunchRefused(refused) => {
            assert_eq!(
                refused.reason,
                SessionLaunchRefusalReason::ContinuationUnsupported
            );
        }
        other => panic!("expected typed refusal, got {other:?}"),
    }
}

#[test]
fn unconfigured_fixture_launch_is_refused_typed() {
    let launcher = SessionLauncher::from_environment();
    let reply = launcher.launch(launch_request(
        HarnessKind::Fixture,
        ContinuationRequest::Fresh,
        "You are agent xk3f.",
    ));
    match reply {
        MetaHarnessReply::SessionLaunchRefused(refused) => {
            assert_eq!(
                refused.reason,
                SessionLaunchRefusalReason::LauncherUnavailable
            );
        }
        other => panic!("expected typed refusal, got {other:?}"),
    }
}

#[test]
fn fixture_launch_delivers_identity_bearing_prompt_as_final_spawn_argument() {
    let capture_directory = std::env::temp_dir().join(format!(
        "harness-launch-witness-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&capture_directory).expect("create capture directory");
    let capture_file = capture_directory.join("prompt.txt");
    let launcher = SessionLauncher::with_fixture_command(FixtureLaunchCommand::new(
        "/bin/sh",
        vec![
            "-c".to_string(),
            format!("printf %s \"$1\" > {}", capture_file.display()),
            "fixture-shell".to_string(),
        ],
    ));
    let prompt = "You are agent xk3f. Map the repo.";
    let reply = launcher.launch(launch_request(
        HarnessKind::Fixture,
        ContinuationRequest::Fresh,
        prompt,
    ));
    let launched = match reply {
        MetaHarnessReply::SessionLaunched(launched) => launched,
        other => panic!("expected launch, got {other:?}"),
    };
    assert_eq!(launched.agent_identity.as_str(), "xk3f");
    assert!(launched.child_process_id > 0);
    assert!(launched.session_directory.is_none());

    let deadline = Instant::now() + Duration::from_secs(5);
    let captured = loop {
        if let Ok(text) = std::fs::read_to_string(&capture_file) {
            if !text.is_empty() {
                break text;
            }
        }
        assert!(
            Instant::now() < deadline,
            "fixture child never wrote its prompt capture"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(captured, prompt);
    assert!(captured.contains("agent xk3f"));

    std::fs::remove_dir_all(&capture_directory).expect("remove capture directory");
}
