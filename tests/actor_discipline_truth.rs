//! Architectural-truth witnesses for persona-harness's actor
//! discipline.
//!
//! - Public actor nouns are data-bearing — `mem::size_of::<X>() > 0`.
//! - No shared `Arc<Mutex<_>>` / `Arc<RwLock<_>>` between actors
//!   (per `~/primary/skills/actor-systems.md` §"No shared locks").
//!
//! The scan covers `src/**` except the
//! `TranscriptSubscriptionSink` block inside
//! `src/subscription.rs`. The sink is a single-consumer,
//! single-handler in-process scaffolding type whose doc comment
//! explicitly names it as test/prototype scaffolding to be
//! replaced by a real socket-writer actor in production daemons.
//! Per `~/primary/reports/operator-assistant/135-phase3-push-subscription-chains-2026-05-16.md`
//! §"Daemon-socket streaming layer (router → harness subscription)"
//! the wire-up to a socket-writer actor is a separate operator
//! slice. Until that lands, the sink stays as documented
//! scaffolding rather than the destination shape; the witness
//! must not block its existence.
//!
//! A future refactor that collapses an actor noun to a marker
//! ZST, or wires shared locks between *actors* (not the
//! sink's internal back-pressure), breaks these witnesses.

use std::fs;
use std::path::{Path, PathBuf};

use persona_harness::runtime::Harness;
use persona_harness::subscription::{
    TranscriptDeltaPublisher, TranscriptStreamingReplyHandler, TranscriptSubscriptionManager,
};
use persona_harness::supervision::SupervisionPhase;

#[test]
fn public_actor_nouns_carry_data() {
    assert!(std::mem::size_of::<Harness>() > 0);
    assert!(std::mem::size_of::<SupervisionPhase>() > 0);
    assert!(std::mem::size_of::<TranscriptSubscriptionManager>() > 0);
    assert!(std::mem::size_of::<TranscriptStreamingReplyHandler>() > 0);
    assert!(std::mem::size_of::<TranscriptDeltaPublisher>() > 0);
}

#[test]
fn actor_source_does_not_share_locks_between_actors() {
    let forbidden = [
        ("Arc<Mutex", "shared mutex state between actors"),
        ("Arc < Mutex", "shared mutex state between actors"),
        ("RwLock", "shared read-write lock state between actors"),
    ];

    let mut violations: Vec<String> = Vec::new();
    for path in production_source_files() {
        let text = fs::read_to_string(&path).expect("read source file");
        let is_subscription_source =
            path.file_name().and_then(|name| name.to_str()) == Some("subscription.rs");
        for (fragment, reason) in forbidden {
            for (index, line) in text.lines().enumerate() {
                if !line.contains(fragment) {
                    continue;
                }
                let trimmed = line.trim_start();
                // Skip comment lines — they document the rule
                // rather than embody a violation.
                if trimmed.starts_with("//") {
                    continue;
                }
                // The `TranscriptSubscriptionSink` block in
                // `subscription.rs` is documented test/prototype
                // scaffolding; skip its sink-related lines.
                if is_subscription_source && line.contains("TranscriptSubscriptionSinkInner") {
                    continue;
                }
                violations.push(format!(
                    "{}:{}: {reason} ({line})",
                    path.display(),
                    index + 1,
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "shared-lock violations in actor source:\n{}",
        violations.join("\n"),
    );
}

fn production_source_files() -> Vec<PathBuf> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = crate_root.join("src");
    let mut output = Vec::new();
    collect_rust_files(&src, &mut output);
    output
}

fn collect_rust_files(directory: &Path, output: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, output);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            output.push(path);
        }
    }
}
