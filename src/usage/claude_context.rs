//! Context use of each live Claude Code session.
//!
//! Live sessions come from Claude Code's own registry,
//! `~/.claude/sessions/<pid>.json`, kept only while the pid is alive. The
//! context figure is the last assistant request's input
//! (`input + cache_creation + cache_read`) in the session transcript's tail:
//! a proxy, superseded by any later user turn or compaction. The window size
//! is not in the transcript, so the percentage stays unknown. A registry that
//! cannot be read, or an entry in it that does not parse, is reported as an
//! unavailable source rather than skipped; only an entry whose pid is dead is
//! passed over, because it names no live session. No Flow is bound: a session
//! identifier's prefix is not a witnessed session-to-Flow binding.

use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::Value;
use signal_harness::{
    ContextBasis, ContextFreshness, ContextSourceFailureReason, ContextSourceUnavailable,
    ContextUnavailableReason, SessionContext, SessionContextObservation, SessionContextUnavailable,
    UsageProvider,
};

use super::tail::{RecordTime, TailReader, TranscriptTailReading};
use super::{ContextSource, ObservationInstant, UsageHome};

const REGISTRY_BYTE_LIMIT: u64 = 64 * 1024;

/// The live Claude sessions under one home.
#[derive(Clone, Debug)]
pub struct ClaudeLiveSessions {
    home: UsageHome,
    tail: TailReader,
}

/// One registry entry of a live session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaudeRegistryEntry {
    session: String,
    name: Option<String>,
    working_directory: String,
}

/// What one read of the session registry found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RegistryRead {
    live: Vec<ClaudeRegistryEntry>,
    unreadable: usize,
}

/// What one registry file holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryEntryRead {
    /// A canonical session whose pid is alive.
    Live(ClaudeRegistryEntry),
    /// A session whose pid has exited: no live session to report.
    Dead,
    /// A file that could not be read or does not name a session.
    Unreadable,
}

/// What the transcript tail says about the last request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClaudeTranscriptUsage {
    pub tokens: Option<i64>,
    pub model: Option<String>,
    pub event_time: Option<i64>,
    pub superseded: bool,
    pub partial: bool,
}

/// The role of a recorded tail: it yields the last request's usage.
pub trait ClaudeUsageRecords {
    fn last_request_usage(&self, session: &str) -> ClaudeTranscriptUsage;
}

impl ClaudeLiveSessions {
    pub fn new(home: UsageHome) -> Self {
        Self {
            home,
            tail: TailReader::default(),
        }
    }

    /// The live registry entries, and how many registry files could not be
    /// read; or why the registry itself could not be read.
    fn entries(&self) -> Result<RegistryRead, ContextSourceFailureReason> {
        let directory =
            std::fs::read_dir(self.home.claude_sessions()).map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => ContextSourceFailureReason::RegistryAbsent,
                _ => ContextSourceFailureReason::RegistryUnreadable,
            })?;
        let mut read = RegistryRead::default();
        for path in directory
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
        {
            match ClaudeRegistryEntry::read(&path) {
                RegistryEntryRead::Live(entry) => read.live.push(entry),
                RegistryEntryRead::Dead => {}
                RegistryEntryRead::Unreadable => read.unreadable += 1,
            }
        }
        read.live
            .sort_by(|left, right| left.session.cmp(&right.session));
        Ok(read)
    }

    fn source_unavailable(
        instant: ObservationInstant,
        reason: ContextSourceFailureReason,
    ) -> SessionContextObservation {
        SessionContextObservation::SourceUnavailable(ContextSourceUnavailable {
            usage_provider: UsageProvider::Claude,
            account_home_option: None,
            observation_time: instant.nanoseconds(),
            context_source_failure_reason: reason,
        })
    }

    fn observe_entry(
        &self,
        entry: &ClaudeRegistryEntry,
        instant: ObservationInstant,
    ) -> SessionContextObservation {
        let unavailable = |reason| {
            SessionContextObservation::Unavailable(SessionContextUnavailable {
                usage_provider: UsageProvider::Claude,
                session_identifier: entry.session.clone(),
                observation_time: instant.nanoseconds(),
                context_unavailable_reason: reason,
            })
        };
        let path = entry.transcript(&self.home.claude_projects());
        let tail = match self.tail.read_tail(&path) {
            Ok(tail) => tail,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return unavailable(ContextUnavailableReason::TranscriptAbsent);
            }
            Err(_) => return unavailable(ContextUnavailableReason::TranscriptUnreadable),
        };
        let usage = tail.last_request_usage(&entry.session);
        let freshness = match (&usage.tokens, usage.superseded, usage.partial) {
            (None, _, _) => ContextFreshness::Unknown,
            (Some(_), true, _) => ContextFreshness::Superseded,
            (Some(_), false, true) => ContextFreshness::Unknown,
            (Some(_), false, false) => ContextFreshness::Proxy,
        };
        SessionContextObservation::Observed(SessionContext {
            usage_provider: UsageProvider::Claude,
            session_identifier: entry.session.clone(),
            flow_identifier_option: None,
            session_name_option: entry.name.clone(),
            model_identifier_option: usage.model,
            observation_time: instant.nanoseconds(),
            event_time_option: usage.event_time,
            context_basis: ContextBasis::ClaudeTranscriptLastRequest,
            context_freshness: freshness,
            context_tokens_option: usage.tokens,
            context_window_tokens_option: None,
            context_used_basis_points_option: None,
        })
    }
}

impl ContextSource for ClaudeLiveSessions {
    fn observe_context(&self, instant: ObservationInstant) -> Vec<SessionContextObservation> {
        let read = match self.entries() {
            Ok(read) => read,
            Err(reason) => return vec![Self::source_unavailable(instant, reason)],
        };
        read.live
            .iter()
            .map(|entry| self.observe_entry(entry, instant))
            .chain((0..read.unreadable).map(|_| {
                Self::source_unavailable(
                    instant,
                    ContextSourceFailureReason::RegistryEntryUnreadable,
                )
            }))
            .collect()
    }
}

impl ClaudeRegistryEntry {
    /// Read one registry file: a live canonical session, a session whose pid
    /// has exited, or an unreadable entry.
    pub fn read(path: &Path) -> RegistryEntryRead {
        let Some(value) = Self::parsed(path) else {
            return RegistryEntryRead::Unreadable;
        };
        let Some(pid) = value.get("pid").and_then(Value::as_u64) else {
            return RegistryEntryRead::Unreadable;
        };
        if !Path::new("/proc").join(pid.to_string()).exists() {
            return RegistryEntryRead::Dead;
        }
        let session = value
            .get("sessionId")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase);
        let working_directory = value.get("cwd").and_then(Value::as_str);
        match (session, working_directory) {
            (Some(session), Some(working_directory)) if Self::canonical_session(&session) => {
                RegistryEntryRead::Live(Self {
                    session,
                    name: value.get("name").and_then(Value::as_str).map(str::to_owned),
                    working_directory: working_directory.to_owned(),
                })
            }
            _ => RegistryEntryRead::Unreadable,
        }
    }

    fn parsed(path: &Path) -> Option<Value> {
        let file = std::fs::File::open(path).ok()?;
        let mut text = String::new();
        file.take(REGISTRY_BYTE_LIMIT)
            .read_to_string(&mut text)
            .ok()?;
        serde_json::from_str(&text).ok()
    }

    fn canonical_session(session: &str) -> bool {
        session.len() == 36
            && session
                .char_indices()
                .all(|(index, character)| match index {
                    8 | 13 | 18 | 23 => character == '-',
                    _ => character.is_ascii_hexdigit(),
                })
    }

    /// `~/.claude/projects/<working directory with every non-alphanumeric
    /// character as '-'>/<session>.jsonl`.
    pub fn transcript(&self, projects: &Path) -> PathBuf {
        let slug: String = self
            .working_directory
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() {
                    character
                } else {
                    '-'
                }
            })
            .collect();
        projects.join(slug).join(format!("{}.jsonl", self.session))
    }
}

impl ClaudeUsageRecords for super::tail::TranscriptTail {
    fn last_request_usage(&self, session: &str) -> ClaudeTranscriptUsage {
        let mut usage = ClaudeTranscriptUsage {
            partial: self.partial,
            ..ClaudeTranscriptUsage::default()
        };
        for record in &self.records {
            if record.get("sessionId").and_then(Value::as_str) != Some(session) {
                continue;
            }
            let kind = record.get("type").and_then(Value::as_str).unwrap_or("");
            let subtype = record.get("subtype").and_then(Value::as_str).unwrap_or("");
            if kind == "assistant" {
                let Some(tokens) = record
                    .get("message")
                    .and_then(|message| message.get("usage"))
                    .and_then(|counts| {
                        [
                            "input_tokens",
                            "cache_creation_input_tokens",
                            "cache_read_input_tokens",
                        ]
                        .iter()
                        .map(|name| counts.get(*name).and_then(Value::as_i64))
                        .sum::<Option<i64>>()
                    })
                else {
                    continue;
                };
                usage.tokens = Some(tokens);
                usage.model = record
                    .get("message")
                    .and_then(|message| message.get("model"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                usage.event_time = record.record_nanoseconds();
                usage.superseded = false;
            } else if kind == "user"
                || kind == "summary"
                || kind == "compact_boundary"
                || (kind == "system" && (subtype.contains("compact") || subtype.contains("clear")))
            {
                usage.superseded = true;
            }
        }
        usage
    }
}
