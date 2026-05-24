# harness skill

Work here when the change concerns harness identity, lifecycle, transcript
events, adapter capabilities, harness actor surfaces, or transcript
subscription delivery.

Rules for work here:

- Keep routing policy in `persona-router`.
- Keep OS/window-manager observations in `persona-system`.
- Keep durable PTY and viewer transport in `persona-terminal`.
- Model harness capabilities as typed values, not strings.
- Project harness identity through typed read views. Do not return full binding
  records to every caller, and do not treat the projection enum as an
  authorization gate.
- Keep live lifecycle and transcript state inside `Harness`; do not add
  alternate runtime wrappers or public handle wrappers.
- Preserve the durable-harness invariant: closing a viewer must not kill the
  harness.
- Transcript subscription delivery follows the canonical five-state
  lifecycle (subscribe → snapshot reply → deltas → retract → final
  ack → end). See this workspace's `skills/subscription-lifecycle.md`.
- Every open transcript subscription is owned by a per-subscription
  Kameo actor (`TranscriptStreamingReplyHandler`); a slow consumer
  holds back its own stream and cannot block siblings. Never
  fan out through a shared `Arc<Mutex<Vec<_>>>`.
- The handler's outbound delta buffer is bounded; on overrun the
  subscription drops with a typed failure reply.
- The kernel grammar at `signal-core/macros/src/validate.rs:303–331`
  enforces close-is-Retract: the daemon must accept
  `HarnessTranscriptRetraction` and emit the final
  `HarnessSubscriptionRetracted` reply before closing the stream.

## See also

- this workspace's `skills/subscription-lifecycle.md` — canonical
  five-state FSM the transcript subscription implements.
- this workspace's `skills/push-not-pull.md` — push-not-poll discipline.
- this workspace's `skills/actor-systems.md` — actor-density rules for
  the producer plane.
- this workspace's `skills/kameo.md` — runtime details for the
  three-actor shape.
