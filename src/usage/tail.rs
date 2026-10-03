//! Bounded reads of a JSONL transcript or rollout tail.
//!
//! Only the last `limit` bytes are read. A clipped first line is dropped, and
//! a line that does not parse marks the tail partial. Nothing read here is
//! emitted except the usage figures the callers pick out.

use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use serde_json::Value;

/// The default byte bound for a transcript tail.
pub const TAIL_BYTE_LIMIT: u64 = 1024 * 1024;

/// The parsed records of one bounded tail, oldest first.
#[derive(Clone, Debug, PartialEq)]
pub struct TranscriptTail {
    pub records: Vec<Value>,
    pub partial: bool,
}

/// Reads tails under one byte bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TailReader {
    limit: u64,
}

pub trait TranscriptTailReading {
    fn read_tail(&self, path: &Path) -> std::io::Result<TranscriptTail>;
}

impl Default for TailReader {
    fn default() -> Self {
        Self {
            limit: TAIL_BYTE_LIMIT,
        }
    }
}

impl TailReader {
    pub fn new(limit: u64) -> Self {
        Self { limit }
    }
}

impl TranscriptTailReading for TailReader {
    fn read_tail(&self, path: &Path) -> std::io::Result<TranscriptTail> {
        let mut file = std::fs::File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(std::io::Error::other("not a regular file"));
        }
        let length = metadata.len().min(self.limit);
        let start = metadata.len() - length;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = Vec::with_capacity(length as usize);
        file.take(length).read_to_end(&mut bytes)?;
        let text = String::from_utf8_lossy(&bytes);
        let mut lines: Vec<&str> = text.split('\n').collect();
        if start > 0 && !lines.is_empty() {
            lines.remove(0);
        }
        let mut partial = false;
        let records = lines
            .into_iter()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| match serde_json::from_str::<Value>(line) {
                Ok(record) => Some(record),
                Err(_) => {
                    partial = true;
                    None
                }
            })
            .collect();
        Ok(TranscriptTail { records, partial })
    }
}

/// An RFC 3339 record timestamp as Unix-epoch nanoseconds.
pub trait RecordTime {
    fn record_nanoseconds(&self) -> Option<i64>;
}

impl RecordTime for Value {
    fn record_nanoseconds(&self) -> Option<i64> {
        let text = self.get("timestamp")?.as_str()?;
        let parsed =
            time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
                .ok()?;
        i64::try_from(parsed.unix_timestamp_nanos()).ok()
    }
}
