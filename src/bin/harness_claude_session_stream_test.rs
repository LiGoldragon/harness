//! harness-claude-session-stream-test — a real, unmocked proof that a headless
//! `claude` turn is observed and its `ClaudeSessionObservation` is PUSHED to a
//! subscriber on `HarnessTranscriptStream`, with no polling on the consumer
//! path.
//!
//! It runs one headless `claude` turn in a throwaway sandbox (refusing the
//! primary workspace), recovers the turn through the harness JSONL observer,
//! folds it into a `ClaudeSessionObservation` via `ObservedClaudeTurn` (response
//! sourced from the recovered turn's `assistant_text()` getter;
//! `accumulated_context` left `None` pending og38.1), then drives the real
//! producer-plane actors: the subscriber opens once, parks on `recv().await`,
//! and is WOKEN by the producer's push — never a sleep or interval tick.
//!
//! This binary needs the `claude` CLI and network, so it is a manually-run
//! `-test` witness, not a Nix flake check. The projection and push transport
//! themselves are covered by `tests/claude_session_observation.rs` and
//! `tests/claude_session_stream.rs`, which run under `nix flake check`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use harness::{
    ClaudeArtifactObserver, ClaudeArtifactSnapshot, CloseTranscriptSubscription,
    ObservedClaudeTurn, OpenTranscriptSubscription, PublishStreamEvent, TranscriptDeliveryEvent,
    TranscriptDeltaPublisher, TranscriptSubscriptionManager, TranscriptSubscriptionSink,
};
use kameo::actor::Spawn;
use serde_json::Value;
use signal_harness::{
    ClaudeSessionLifecycle, ClaudeSessionObservation, HarnessName, HarnessStreamEvent,
    StreamedEventCount, TurnLaunch,
};
use thiserror::Error;

/// The one workspace a headless run must never be pointed at.
const PRIMARY_WORKSPACE: &str = "/home/li/primary";

#[tokio::main]
async fn main() {
    let outcome = match Witness::from_process_arguments() {
        Ok(witness) => witness.run().await,
        Err(error) => Err(error),
    };
    if let Err(error) = outcome {
        eprintln!("HarnessClaudeSessionStreamWitnessBlocked {error}");
        std::process::exit(2);
    }
}

/// The typed boundary error for this witness binary.
#[derive(Debug, Error)]
enum WitnessError {
    #[error("usage: harness-claude-session-stream-test <harness-name> <prompt> [--model <alias>]")]
    Usage,
    #[error("refused: sandbox {0:?} is inside the primary workspace {PRIMARY_WORKSPACE}")]
    PrimaryWorkspaceRefused(PathBuf),
    #[error("could not mint a session identifier: {0}")]
    SessionIdentifier(std::io::Error),
    #[error("filesystem error at {path:?}: {source}")]
    Filesystem {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to spawn `claude`: {0}")]
    Spawn(std::io::Error),
    #[error("`claude` produced no result event; combined output was:\n{0}")]
    MissingResultEvent(String),
    #[error("observing the claude transcript failed: {0}")]
    Observation(harness::Error),
    #[error("the harness observer never saw session {0}")]
    SessionNeverObserved(String),
    #[error("the subscriber received {got} instead of the pushed ClaudeSessionObservation")]
    UnexpectedDelivery { got: String },
    #[error("subscription channel closed before the {stage} arrived")]
    SubscriptionClosed { stage: &'static str },
}

struct Witness {
    harness: HarnessName,
    prompt: String,
    model: String,
}

impl Witness {
    fn from_process_arguments() -> Result<Self, WitnessError> {
        let mut arguments = std::env::args().skip(1);
        let harness = HarnessName::new(arguments.next().ok_or(WitnessError::Usage)?);
        let prompt = arguments.next().ok_or(WitnessError::Usage)?;
        let mut model = "haiku".to_string();
        while let Some(flag) = arguments.next() {
            match flag.as_str() {
                "--model" => model = arguments.next().ok_or(WitnessError::Usage)?,
                _ => return Err(WitnessError::Usage),
            }
        }
        Ok(Self {
            harness,
            prompt,
            model,
        })
    }

    async fn run(self) -> Result<(), WitnessError> {
        let sandbox = Sandbox::for_harness(self.harness.as_str())?;
        let session = SessionIdentifier::generate()?;

        println!("== harness-claude-session-stream-test ==");
        println!("harness  : {}", self.harness.as_str());
        println!("sandbox  : {}", sandbox.path().display());
        println!("session  : {}", session.as_str());
        println!("model    : {}", self.model);
        println!("prompt   : {}", self.prompt);
        println!();

        // 1) Run one real headless turn and observe it.
        let turn = HeadlessTurn::execute(&sandbox, &session, &self.prompt, &self.model).await?;
        let snapshot = sandbox.observe_flushed_transcript(&session)?;
        println!(
            "observed : session {} recovered, {} streamed events, transcript {}",
            snapshot.session_identifier().unwrap_or("<none>"),
            turn.streamed_event_count,
            snapshot
                .project_jsonl_paths()
                .first()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<none>".to_string()),
        );

        // 2) Fold the observation into the pushed session observation. The
        //    launch is Fresh (this witness always mints a new session), the
        //    lifecycle Completed (the turn reached its result), and the streamed
        //    event count is the process runner's tally.
        let observation = ObservedClaudeTurn::new(
            self.harness.clone(),
            TurnLaunch::Fresh,
            ClaudeSessionLifecycle::Completed,
            StreamedEventCount::new(turn.streamed_event_count),
            snapshot,
        )
        .into_session_observation();

        // 3) Drive the real producer plane and prove the push.
        self.prove_push(observation).await
    }

    async fn prove_push(&self, observation: ClaudeSessionObservation) -> Result<(), WitnessError> {
        let manager = TranscriptSubscriptionManager::spawn(TranscriptSubscriptionManager::new());
        manager.wait_for_startup().await;
        let publisher =
            TranscriptDeltaPublisher::spawn(TranscriptDeltaPublisher::new(manager.clone()));
        publisher.wait_for_startup().await;

        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let sink = TranscriptSubscriptionSink::channel(sender);

        // The subscriber opens once; it will park on recv().await from here on.
        let opened = manager
            .ask(OpenTranscriptSubscription {
                harness: self.harness.clone(),
                sink,
            })
            .await
            .expect("manager accepts open");
        match receiver.recv().await {
            Some(TranscriptDeliveryEvent::Snapshot(_)) => {}
            Some(other) => {
                return Err(WitnessError::UnexpectedDelivery {
                    got: format!("{other:?}"),
                });
            }
            None => return Err(WitnessError::SubscriptionClosed { stage: "snapshot" }),
        }
        println!();
        println!(
            "subscriber opened (token subscription #{}); parked on recv().await",
            opened.token.subscription.into_u64()
        );

        // NO-POLL WITNESS: nothing has been published, so a poll finds nothing.
        // The subscriber is not spinning — it is parked until the producer pushes.
        match receiver.try_recv() {
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                println!("stream quiescent before push (try_recv == Empty): nothing polls");
            }
            other => {
                return Err(WitnessError::UnexpectedDelivery {
                    got: format!("premature {other:?}"),
                });
            }
        }

        // The producer PUSHES the observation onto the stream.
        let receipt = publisher
            .ask(PublishStreamEvent {
                event: observation.clone().into(),
            })
            .await
            .expect("publish claude session observation");
        println!(
            "producer pushed ClaudeSessionObservation (published={}, fanned_out={})",
            receipt.published, receipt.fanned_out
        );

        // The park is woken by the push and yields the observation.
        let pushed = match receiver.recv().await {
            Some(TranscriptDeliveryEvent::Delta(HarnessStreamEvent::ClaudeSessionObservation(
                pushed,
            ))) => pushed,
            Some(other) => {
                return Err(WitnessError::UnexpectedDelivery {
                    got: format!("{other:?}"),
                });
            }
            None => {
                return Err(WitnessError::SubscriptionClosed {
                    stage: "observation",
                });
            }
        };

        println!();
        println!("---- PUSHED ClaudeSessionObservation (received via recv().await) ----");
        Self::report_observation(&pushed);
        println!("--------------------------------------------------------------------");

        assert_eq!(
            pushed, observation,
            "subscriber received the exact pushed observation"
        );
        assert_eq!(
            pushed.accumulated_context, None,
            "accumulated_context is deferred (og38.1) and pushed as None"
        );

        // Close the subscription; the final retraction ack is pushed too.
        manager
            .ask(CloseTranscriptSubscription {
                token: opened.token.clone(),
            })
            .await
            .expect("close");
        match receiver.recv().await {
            Some(TranscriptDeliveryEvent::FinalAcknowledgement(ack)) => {
                assert_eq!(ack.token, opened.token);
                println!("subscription retracted (final ack pushed on the same stream)");
            }
            Some(other) => {
                return Err(WitnessError::UnexpectedDelivery {
                    got: format!("{other:?}"),
                });
            }
            None => return Err(WitnessError::SubscriptionClosed { stage: "final ack" }),
        }

        let _ = publisher.stop_gracefully().await;
        publisher.wait_for_shutdown().await;
        let _ = manager.stop_gracefully().await;
        manager.wait_for_shutdown().await;

        println!();
        println!("PROOF: subscriber received a real headless turn's ClaudeSessionObservation");
        println!("       purely by producer push (recv().await), with no poll loop anywhere.");
        Ok(())
    }

    fn report_observation(observation: &ClaudeSessionObservation) {
        println!("harness              : {}", observation.harness.as_str());
        println!(
            "session_identifier   : {}",
            observation
                .session_identifier
                .as_ref()
                .map(|id| id.as_str())
                .unwrap_or("<none>")
        );
        println!(
            "model                : {}",
            observation
                .model
                .as_ref()
                .map(|model| model.as_str())
                .unwrap_or("<none>")
        );
        println!("launch               : {:?}", observation.launch);
        println!("reached_end_of_turn  : {}", observation.reached_end_of_turn);
        println!(
            "streamed_event_count : {}",
            observation.streamed_event_count.into_u64()
        );
        println!(
            "tool_call_count      : {}",
            observation.tool_call_count.into_u64()
        );
        println!(
            "status_transitions   : {}",
            observation.status_transition_count.into_u64()
        );
        println!(
            "transcript_path      : {}",
            observation
                .transcript_path
                .as_ref()
                .map(|path| path.as_str())
                .unwrap_or("<none>")
        );
        println!(
            "response (assistant) : {}",
            observation
                .response
                .as_ref()
                .map(|text| text.as_str())
                .unwrap_or("<none>")
        );
        println!(
            "accumulated_context  : {:?} (DEFERRED — og38.1)",
            observation.accumulated_context
        );
        println!("lifecycle            : {:?}", observation.lifecycle);
    }
}

/// One real headless `claude` turn's captured facts.
struct HeadlessTurn {
    streamed_event_count: u64,
}

impl HeadlessTurn {
    async fn execute(
        sandbox: &Sandbox,
        session: &SessionIdentifier,
        prompt: &str,
        model: &str,
    ) -> Result<Self, WitnessError> {
        let directory = sandbox.path().to_path_buf();
        let session = session.as_str().to_string();
        let prompt = prompt.to_string();
        let model = model.to_string();
        let output = tokio::task::spawn_blocking(move || {
            Command::new("claude")
                .args([
                    "-p",
                    &prompt,
                    "--session-id",
                    &session,
                    "--output-format",
                    "stream-json",
                    "--verbose",
                    "--model",
                    &model,
                    "--allowedTools",
                    "",
                ])
                .current_dir(&directory)
                .output()
        })
        .await
        .expect("claude command task joins")
        .map_err(WitnessError::Spawn)?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let events: Vec<Value> = stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect();
        let has_result = events
            .iter()
            .any(|event| event.get("type").and_then(Value::as_str) == Some("result"));
        if !has_result {
            return Err(WitnessError::MissingResultEvent(format!(
                "{stdout}\n{stderr}"
            )));
        }
        Ok(Self {
            streamed_event_count: events.len() as u64,
        })
    }
}

/// The deterministic per-harness sandbox. Refuses the primary workspace before
/// creating anything.
struct Sandbox {
    directory: PathBuf,
}

impl Sandbox {
    /// Recover the flushed transcript. `claude` has fully exited by the time
    /// `output()` returned, so the JSONL is on disk: a single snapshot normally
    /// sees it. If it does not yet, await the inotify file-event push through
    /// the observer's own event watcher rather than polling.
    fn observe_flushed_transcript(
        &self,
        session: &SessionIdentifier,
    ) -> Result<ClaudeArtifactSnapshot, WitnessError> {
        let observer =
            ClaudeArtifactObserver::new(self.path()).with_session_identifier(session.as_str());
        let snapshot = observer.snapshot().map_err(WitnessError::Observation)?;
        if snapshot.session_identifier() == Some(session.as_str()) {
            return Ok(snapshot);
        }
        // Fall back to the inotify-backed event watcher (a push primitive):
        // each wake is a real filesystem event, not a timed poll.
        let mut watcher = observer
            .event_watcher()
            .map_err(WitnessError::Observation)?;
        for _ in 0..20 {
            let wake = match watcher.wait_for_next_snapshot(Duration::from_secs(2)) {
                Ok(wake) => wake,
                Err(_) => break,
            };
            if wake.snapshot().session_identifier() == Some(session.as_str()) {
                return Ok(wake.snapshot().clone());
            }
        }
        Err(WitnessError::SessionNeverObserved(
            session.as_str().to_string(),
        ))
    }

    fn for_harness(harness: &str) -> Result<Self, WitnessError> {
        let intended = Self::base().join(format!("harness-{harness}"));
        Self::refuse_primary(Self::lexically_absolute(&intended)?)?;
        fs::create_dir_all(&intended).map_err(|source| WitnessError::Filesystem {
            path: intended.clone(),
            source,
        })?;
        let directory = fs::canonicalize(&intended).map_err(|source| WitnessError::Filesystem {
            path: intended.clone(),
            source,
        })?;
        Self::refuse_primary(directory.clone())?;
        Ok(Self { directory })
    }

    fn base() -> PathBuf {
        std::env::var_os("HARNESS_CLAUDE_SESSION_STREAM_BASE")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("harness-claude-session-stream-test"))
    }

    fn lexically_absolute(path: &Path) -> Result<PathBuf, WitnessError> {
        std::path::absolute(path).map_err(|source| WitnessError::Filesystem {
            path: path.to_path_buf(),
            source,
        })
    }

    fn refuse_primary(path: PathBuf) -> Result<(), WitnessError> {
        if path.starts_with(PRIMARY_WORKSPACE) {
            return Err(WitnessError::PrimaryWorkspaceRefused(path));
        }
        Ok(())
    }

    fn path(&self) -> &Path {
        &self.directory
    }
}

/// A `claude` session identifier (a UUID). Domain newtype.
struct SessionIdentifier {
    value: String,
}

impl SessionIdentifier {
    fn generate() -> Result<Self, WitnessError> {
        let raw = fs::read_to_string("/proc/sys/kernel/random/uuid")
            .map_err(WitnessError::SessionIdentifier)?;
        Ok(Self {
            value: raw.trim().to_string(),
        })
    }

    fn as_str(&self) -> &str {
        &self.value
    }
}
