//! Fixture-testable, bounded delivery adapter for a scheduled wake.
//!
//! `Accepted` means only that the harness accepted bytes at its input surface;
//! it is deliberately not a transcript or user-turn receipt.

use std::{collections::HashMap, io::{Read, Write}, path::PathBuf, time::{Duration, Instant}};

use sha2::{Digest, Sha256};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use signal_harness::{DeliveryCompleted, HarnessEvent, HarnessFrame, HarnessFrameBody, HarnessName, HarnessRequest, MessageBody, MessageDelivery, MessageSender, MessageSlot};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeRequest { pub target_flow: String, pub body: String, pub body_sha256: String, pub harness: HarnessName, pub sender: MessageSender, pub message_slot: MessageSlot }
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeDeliveryOutcome { Accepted { target_flow: String, body_sha256: String, message_slot: MessageSlot }, Duplicate { target_flow: String, message_slot: MessageSlot }, Rejected }
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeDeliveryFailure { BodyHashMismatch, Connect, Timeout, Frame, Decode, WrongReply, Conflict }

#[derive(Debug)]
pub struct WakeDeliveryAdapter { socket: PathBuf, timeout: Duration, accepted: HashMap<(String, u64), String> }
impl WakeDeliveryAdapter {
    pub fn new(socket: impl Into<PathBuf>, timeout: Duration) -> Self { Self { socket: socket.into(), timeout, accepted: HashMap::new() } }
    pub fn deliver(&mut self, request: WakeRequest) -> Result<WakeDeliveryOutcome, WakeDeliveryFailure> {
        let digest = format!("{:x}", Sha256::digest(request.body.as_bytes()));
        if digest != request.body_sha256 { return Err(WakeDeliveryFailure::BodyHashMismatch); }
        let key = (request.target_flow.clone(), request.message_slot.into_u64());
        if let Some(first) = self.accepted.get(&key) { return if first == &digest { Ok(WakeDeliveryOutcome::Duplicate { target_flow: request.target_flow, message_slot: request.message_slot }) } else { Err(WakeDeliveryFailure::Conflict) }; }
        let deadline = Instant::now() + self.timeout;
        let runtime = tokio::runtime::Runtime::new().map_err(|_| WakeDeliveryFailure::Connect)?;
        let mut stream = runtime.block_on(async { tokio::time::timeout(self.timeout, tokio::net::UnixStream::connect(&self.socket)).await.map_err(|_| WakeDeliveryFailure::Timeout)?.map_err(|_| WakeDeliveryFailure::Connect) })?.into_std().map_err(|_| WakeDeliveryFailure::Connect)?;
        stream.set_nonblocking(false).map_err(|_| WakeDeliveryFailure::Connect)?;
        let remaining = deadline.checked_duration_since(Instant::now()).ok_or(WakeDeliveryFailure::Timeout)?;
        stream.set_read_timeout(Some(remaining)).map_err(|_| WakeDeliveryFailure::Connect)?;
        stream.set_write_timeout(Some(remaining)).map_err(|_| WakeDeliveryFailure::Connect)?;
        let frame = HarnessFrame::new(HarnessFrameBody::Request { exchange: ExchangeIdentifier::new(SessionEpoch::new(0), ExchangeLane::Connector, LaneSequence::first()), request: signal_frame::Request::from_payload(HarnessRequest::MessageDelivery(MessageDelivery { harness: request.harness.clone(), sender: request.sender, body: MessageBody::new(request.body), message_slot: request.message_slot })) });
        let codec = LengthPrefixedCodec::default();
        codec.write_body(&mut stream, &FrameBody::new(frame.encode().map_err(|_| WakeDeliveryFailure::Frame)?)).map_err(|_| WakeDeliveryFailure::Timeout)?;
        stream.flush().map_err(|_| WakeDeliveryFailure::Timeout)?;
        let remaining = deadline.checked_duration_since(Instant::now()).ok_or(WakeDeliveryFailure::Timeout)?;
        stream.set_read_timeout(Some(remaining)).map_err(|_| WakeDeliveryFailure::Timeout)?;
        let mut prefix = [0_u8; 4]; stream.read_exact(&mut prefix).map_err(|_| WakeDeliveryFailure::Timeout)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > 1024 * 1024 { return Err(WakeDeliveryFailure::Frame); }
        let mut bytes = vec![0; length]; stream.read_exact(&mut bytes).map_err(|_| WakeDeliveryFailure::Timeout)?;
        let event = match HarnessFrame::decode(&bytes).map_err(|_| WakeDeliveryFailure::Decode)?.into_body() { HarnessFrameBody::Reply { reply, .. } => match reply { Reply::Accepted { per_operation, .. } => match per_operation.into_head() { SubReply::Ok(event) => event, _ => return Err(WakeDeliveryFailure::WrongReply) }, _ => return Err(WakeDeliveryFailure::WrongReply) }, _ => return Err(WakeDeliveryFailure::WrongReply) };
        match event { HarnessEvent::DeliveryCompleted(DeliveryCompleted { harness, message_slot }) if harness == request.harness && message_slot == request.message_slot => { self.accepted.insert(key, digest.clone()); Ok(WakeDeliveryOutcome::Accepted { target_flow: request.target_flow, body_sha256: digest, message_slot }) }, _ => Err(WakeDeliveryFailure::WrongReply) }
    }
}
