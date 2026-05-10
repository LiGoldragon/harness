use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::{HarnessBinding, TranscriptEvent, TranscriptLine};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HarnessLifecycle {
    Starting,
    Running,
    Paused,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct HarnessState {
    pub binding: HarnessBinding,
    pub lifecycle: HarnessLifecycle,
    pub transcript_event_count: u64,
}

#[derive(Debug)]
pub struct Harness {
    binding: HarnessBinding,
    lifecycle: HarnessLifecycle,
    transcript_event_count: u64,
}

impl Harness {
    pub fn new(binding: HarnessBinding) -> Self {
        Self {
            binding,
            lifecycle: HarnessLifecycle::Starting,
            transcript_event_count: 0,
        }
    }

    pub async fn start(binding: HarnessBinding) -> ActorRef<Self> {
        let reference = Self::spawn(binding);
        reference.wait_for_startup().await;
        reference
    }

    fn state(&self) -> HarnessState {
        HarnessState {
            binding: self.binding.clone(),
            lifecycle: self.lifecycle.clone(),
            transcript_event_count: self.transcript_event_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadState {
    pub minimum_transcript_events: u64,
}

impl ReadState {
    pub fn expecting_at_least(minimum_transcript_events: u64) -> Self {
        Self {
            minimum_transcript_events,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetHarnessLifecycle {
    pub lifecycle: HarnessLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordTranscriptLine {
    pub line: TranscriptLine,
}

impl Actor for Harness {
    type Args = HarnessBinding;
    type Error = Infallible;

    async fn on_start(
        binding: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(binding))
    }
}

impl Message<ReadState> for Harness {
    type Reply = HarnessState;

    async fn handle(
        &mut self,
        message: ReadState,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let _satisfied = self.transcript_event_count >= message.minimum_transcript_events;
        self.state()
    }
}

impl Message<SetHarnessLifecycle> for Harness {
    type Reply = HarnessState;

    async fn handle(
        &mut self,
        message: SetHarnessLifecycle,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.lifecycle = message.lifecycle;
        self.state()
    }
}

impl Message<RecordTranscriptLine> for Harness {
    type Reply = TranscriptEvent;

    async fn handle(
        &mut self,
        message: RecordTranscriptLine,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.transcript_event_count = self.transcript_event_count.saturating_add(1);
        TranscriptEvent::new(self.binding.id().clone(), message.line)
    }
}
