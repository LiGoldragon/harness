use std::time::Duration;

use harness::{PiRpcDeliveryCommand, PiRpcProcessConfiguration, PiRpcSession};
use tempfile::TempDir;

#[test]
fn live_pi_rpc_accepts_prompt_on_low_quant_gemma_moe() {
    if std::env::var("HARNESS_LIVE_PI_RPC").as_deref() != Ok("1") {
        eprintln!("skipping live Pi RPC test; set HARNESS_LIVE_PI_RPC=1 to run");
        return;
    }

    let root = TempDir::new().expect("live pi rpc tempdir");
    let command_path =
        std::env::var("HARNESS_LIVE_PI_COMMAND").unwrap_or_else(|_| "pi".to_string());
    let provider =
        std::env::var("HARNESS_LIVE_PI_PROVIDER").unwrap_or_else(|_| "criomos-local".to_string());
    let model = std::env::var("HARNESS_LIVE_PI_MODEL")
        .unwrap_or_else(|_| "gemma-4-26b-a4b-ud-q4-k-xl".to_string());
    let configuration = PiRpcProcessConfiguration::new(command_path, root.path().join("session"))
        .with_command_arguments(vec![
            "--no-context-files".to_string(),
            "--no-extensions".to_string(),
            "--no-skills".to_string(),
            "--no-prompt-templates".to_string(),
            "--no-themes".to_string(),
            "--no-tools".to_string(),
            "--provider".to_string(),
            provider,
            "--thinking".to_string(),
            "off".to_string(),
        ])
        .with_model_pattern(model)
        .with_session_name("harness-live-pi-rpc")
        .with_delivery_command(PiRpcDeliveryCommand::Prompt)
        .with_response_timeout(Duration::from_secs(120));

    let mut session = PiRpcSession::spawn(configuration).expect("spawn live Pi RPC session");
    let receipt = session
        .deliver_text("Reply with exactly: OK")
        .expect("live Pi RPC accepts prompt");
    session.stop().expect("live Pi RPC stops");

    assert_eq!(receipt.command(), PiRpcDeliveryCommand::Prompt);
    assert_eq!(receipt.command_identifier(), "harness-1");
}
