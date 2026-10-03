//! Context use of each thread loaded in a live Codex app-server.
//!
//! Each control socket lists its loaded threads; `thread/read` binds a thread
//! to its rollout file, and the rollout tail's last `token_count` event gives
//! the last request's input tokens and the model's context window. A later
//! user message or compaction marks the figure superseded. A thread whose
//! rollout cannot be bound to its id is reported unbound.

use std::path::Path;

use serde_json::{Value, json};
use usage_contract::{
    ContextBasis, ContextFreshness, ContextUnavailableReason, SessionContext,
    SessionContextObservation, SessionContextUnavailable, UsageProvider,
};

use super::app_server::{AppServerFailure, AppServerSession, AppServerTimeout, JsonRpcExchange};
use super::tail::{RecordTime, TailReader, TranscriptTail, TranscriptTailReading};
use super::{ContextSource, ObservationInstant, UsageHome};

const LOADED_PAGE_LIMIT: usize = 8;

/// The loaded Codex threads across every live app-server under one home.
#[derive(Clone, Debug)]
pub struct CodexLiveThreads {
    home: UsageHome,
    timeout: AppServerTimeout,
    tail: TailReader,
}

/// What a rollout tail says about the last request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CodexRolloutUsage {
    pub tokens: Option<i64>,
    pub window: Option<i64>,
    pub event_time: Option<i64>,
    pub superseded: bool,
}

/// The role of a recorded rollout tail: it yields the last token count.
pub trait CodexUsageRecords {
    fn last_token_count(&self) -> CodexRolloutUsage;
}

/// A thread's own metadata from `thread/read`.
struct ThreadMetadata {
    rollout: Option<String>,
    model: Option<String>,
    name: Option<String>,
}

impl CodexLiveThreads {
    pub fn new(home: UsageHome, timeout: AppServerTimeout) -> Self {
        Self {
            home,
            timeout,
            tail: TailReader::default(),
        }
    }

    fn loaded(session: &mut AppServerSession) -> Result<Vec<String>, AppServerFailure> {
        let mut threads = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..LOADED_PAGE_LIMIT {
            let parameters = match &cursor {
                Some(cursor) => json!({ "cursor": cursor }),
                None => json!({}),
            };
            let page = session.request("thread/loaded/list", Some(parameters))?;
            threads.extend(
                page.get("data")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_owned),
            );
            cursor = page
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        Ok(threads)
    }

    fn metadata(
        session: &mut AppServerSession,
        thread: &str,
    ) -> Result<ThreadMetadata, AppServerFailure> {
        let result = session.request(
            "thread/read",
            Some(json!({ "threadId": thread, "includeTurns": false })),
        )?;
        let record = result.get("thread").ok_or(AppServerFailure::Unreadable)?;
        if record.get("id").and_then(Value::as_str) != Some(thread) {
            return Ok(ThreadMetadata {
                rollout: None,
                model: None,
                name: None,
            });
        }
        let text = |key: &str| record.get(key).and_then(Value::as_str).map(str::to_owned);
        Ok(ThreadMetadata {
            rollout: text("path"),
            model: text("model"),
            name: text("name"),
        })
    }

    /// The flow id a flow title carries: `Aspect.{ Model 1a2b3c }`.
    fn flow_from_name(name: &str) -> Option<String> {
        let inner = name.trim().strip_suffix('}')?.trim_end();
        let candidate = inner.rsplit(char::is_whitespace).next()?;
        (candidate.len() == 6
            && candidate
                .chars()
                .all(|character| matches!(character, '0'..='9' | 'a'..='f')))
        .then(|| candidate.to_owned())
    }

    fn observe_thread(
        &self,
        session: &mut AppServerSession,
        thread: &str,
        instant: ObservationInstant,
    ) -> SessionContextObservation {
        let unavailable = |reason| {
            SessionContextObservation::Unavailable(SessionContextUnavailable {
                usage_provider: UsageProvider::Codex,
                session_identifier: thread.to_owned(),
                observation_time: instant.nanoseconds(),
                context_unavailable_reason: reason,
            })
        };
        let metadata = match Self::metadata(session, thread) {
            Ok(metadata) => metadata,
            Err(AppServerFailure::TimedOut) => {
                return unavailable(ContextUnavailableReason::TransportTimedOut);
            }
            Err(_) => return unavailable(ContextUnavailableReason::TransportFailed),
        };
        let Some(rollout) = metadata
            .rollout
            .filter(|path| path.ends_with(&format!("-{thread}.jsonl")))
        else {
            return unavailable(ContextUnavailableReason::ThreadUnbound);
        };
        let tail = match self.tail.read_tail(Path::new(&rollout)) {
            Ok(tail) => tail,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return unavailable(ContextUnavailableReason::TranscriptAbsent);
            }
            Err(_) => return unavailable(ContextUnavailableReason::TranscriptUnreadable),
        };
        let usage = tail.last_token_count();
        let freshness = match (usage.tokens, usage.superseded) {
            (None, _) => ContextFreshness::Unknown,
            (Some(_), true) => ContextFreshness::Superseded,
            (Some(_), false) => ContextFreshness::Proxy,
        };
        let used = match (usage.tokens, usage.window) {
            (Some(tokens), Some(window)) if window > 0 => Some(tokens * 10_000 / window),
            _ => None,
        };
        SessionContextObservation::Observed(SessionContext {
            usage_provider: UsageProvider::Codex,
            session_identifier: thread.to_owned(),
            flow_identifier_option: metadata.name.as_deref().and_then(Self::flow_from_name),
            session_name_option: metadata.name,
            model_identifier_option: metadata.model,
            observation_time: instant.nanoseconds(),
            event_time_option: usage.event_time,
            context_basis: ContextBasis::CodexRolloutLastTokenCount,
            context_freshness: freshness,
            context_tokens_option: usage.tokens,
            context_window_tokens_option: usage.window,
            context_used_basis_points_option: used,
        })
    }
}

impl ContextSource for CodexLiveThreads {
    fn observe_context(&self, instant: ObservationInstant) -> Vec<SessionContextObservation> {
        let mut seen: Vec<String> = Vec::new();
        let mut observations = Vec::new();
        for home in self.home.codex_homes() {
            for socket in home.control_sockets() {
                let Ok(mut session) = AppServerSession::open(&socket, self.timeout) else {
                    continue;
                };
                let Ok(threads) = Self::loaded(&mut session) else {
                    continue;
                };
                for thread in threads {
                    if seen.contains(&thread) {
                        continue;
                    }
                    observations.push(self.observe_thread(&mut session, &thread, instant));
                    seen.push(thread);
                }
            }
        }
        observations
    }
}

impl CodexUsageRecords for TranscriptTail {
    fn last_token_count(&self) -> CodexRolloutUsage {
        let mut usage = CodexRolloutUsage::default();
        for record in &self.records {
            let kind = record.get("type").and_then(Value::as_str).unwrap_or("");
            let payload = record.get("payload");
            let payload_kind = payload
                .and_then(|payload| payload.get("type"))
                .and_then(Value::as_str)
                .unwrap_or("");
            if kind == "event_msg" && payload_kind == "token_count" {
                let Some(info) = payload
                    .and_then(|payload| payload.get("info"))
                    .filter(|info| !info.is_null())
                else {
                    continue;
                };
                usage.tokens = info
                    .get("last_token_usage")
                    .and_then(|last| last.get("input_tokens"))
                    .and_then(Value::as_i64);
                usage.window = info.get("model_context_window").and_then(Value::as_i64);
                usage.event_time = record.record_nanoseconds();
                usage.superseded = false;
            } else if (kind == "response_item"
                && payload_kind == "message"
                && payload
                    .and_then(|payload| payload.get("role"))
                    .and_then(Value::as_str)
                    == Some("user"))
                || (kind == "response_item"
                    && matches!(payload_kind, "compaction" | "context_compaction"))
                || (kind == "event_msg"
                    && matches!(payload_kind, "context_compacted" | "context_compaction"))
            {
                usage.superseded = true;
            }
        }
        usage
    }
}
