//! Projection from a recovered Claude turn to the pushed session observation.
//!
//! The harness observes a headless Claude turn through the JSONL artifact
//! observer (`claude.rs`) and folds it into the `signal-harness`
//! [`ClaudeSessionObservation`] it pushes on `HarnessTranscriptStream`. This
//! module owns that fold. It exists so the render/store facts a turn carries
//! are sourced from real observation — the recovered turn's own
//! `assistant_text()` getter, the observer's structured metadata, and the
//! process runner's streamed-event tally — never re-derived downstream.

use std::time::{SystemTime, UNIX_EPOCH};

use signal_harness::{
    AssistantResponseText, ClaudeModel, ClaudeSessionIdentifier, ClaudeSessionLifecycle,
    ClaudeSessionObservation, HarnessName, StatusTransitionCount, StreamedEventCount,
    ToolCallCount, TranscriptPath, TurnLaunch,
};
use signal_persona::TimestampNanos;

use crate::ClaudeArtifactSnapshot;

/// The harness-side facts about one observed Claude turn that the JSONL
/// transcript alone does not carry: the per-session harness key, how the turn
/// launched, its lifecycle at the moment of observation, and the count of
/// streamed provider events the process runner tallied while the turn ran.
/// Folded with the [`ClaudeArtifactSnapshot`] the observer recovered, it
/// projects the `signal-harness` [`ClaudeSessionObservation`] the harness
/// pushes on `HarnessTranscriptStream`.
///
/// `launch` and `lifecycle` are closed typed records the observing runtime
/// supplies (a resume that self-heals is `TurnLaunch::SelfHealed`, a finished
/// turn is `ClaudeSessionLifecycle::Completed`), not booleans re-derived from
/// the transcript.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservedClaudeTurn {
    harness: HarnessName,
    launch: TurnLaunch,
    lifecycle: ClaudeSessionLifecycle,
    streamed_event_count: StreamedEventCount,
    snapshot: ClaudeArtifactSnapshot,
}

impl ObservedClaudeTurn {
    pub fn new(
        harness: HarnessName,
        launch: TurnLaunch,
        lifecycle: ClaudeSessionLifecycle,
        streamed_event_count: StreamedEventCount,
        snapshot: ClaudeArtifactSnapshot,
    ) -> Self {
        Self {
            harness,
            launch,
            lifecycle,
            streamed_event_count,
            snapshot,
        }
    }

    pub fn harness(&self) -> &HarnessName {
        &self.harness
    }

    pub fn launch(&self) -> TurnLaunch {
        self.launch
    }

    pub fn lifecycle(&self) -> &ClaudeSessionLifecycle {
        &self.lifecycle
    }

    pub fn streamed_event_count(&self) -> StreamedEventCount {
        self.streamed_event_count
    }

    pub fn snapshot(&self) -> &ClaudeArtifactSnapshot {
        &self.snapshot
    }

    /// Project this observed turn into the pushed session observation.
    ///
    /// The assistant response is sourced from the recovered turn's own
    /// `assistant_text()` getter, never the Claude CLI `result` line. The two
    /// per-turn activity counts the transcript carries — tool calls and status
    /// transitions — come from the recovered turn; the streamed-event count is
    /// the process runner's tally. `session_identifier`, `model`, and
    /// `transcript_path` are `Option` because each is genuinely absent until
    /// the observer sees it.
    ///
    /// `accumulated_context` is deliberately left `None`. Its sourcing is a
    /// pending psyche ruling tracked by bead **primary-og38.1** — between (A)
    /// `/context`-at-rest and (B) summing stream-json `usage` — and this
    /// producer synthesizes no figure to fill the gap until that lands.
    pub fn into_session_observation(self) -> ClaudeSessionObservation {
        let recovered = self.snapshot.recovered_turn();
        let session_identifier = recovered
            .session_identifier()
            .map(ClaudeSessionIdentifier::new);
        let model = recovered.model().map(ClaudeModel::new);
        let reached_end_of_turn = recovered.has_stop_reason_end_turn();
        let tool_call_count = ToolCallCount::new(recovered.tool_calls().len() as u64);
        let status_transition_count =
            StatusTransitionCount::new(recovered.status_transitions().len() as u64);
        let response = recovered.assistant_text().map(AssistantResponseText::new);
        let transcript_path = self
            .snapshot
            .project_jsonl_paths()
            .first()
            .map(|path| TranscriptPath::new(path.display().to_string()));

        ClaudeSessionObservation {
            harness: self.harness,
            session_identifier,
            model,
            launch: self.launch,
            reached_end_of_turn,
            streamed_event_count: self.streamed_event_count,
            tool_call_count,
            status_transition_count,
            transcript_path,
            response,
            // DEFERRED — accumulated_context sourcing (bead primary-og38.1).
            // The spike proved headless emits no statusline; the psyche is
            // ruling between /context-at-rest and summed stream-json usage.
            // Until it lands, push None and synthesize nothing.
            accumulated_context: None,
            last_activity: Self::activity_timestamp(),
            lifecycle: self.lifecycle,
        }
    }

    /// Mint the infrastructure-owned activity timestamp at observation time.
    /// Display/ordering only — never a resume gate.
    fn activity_timestamp() -> TimestampNanos {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0);
        TimestampNanos::new(nanos)
    }
}
