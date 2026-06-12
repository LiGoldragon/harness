# harness skill

Work here when the change concerns harness identity, lifecycle, transcript
events, adapter capabilities, harness actor surfaces, or transcript
subscription delivery.

Rules for work here:

- Keep routing policy in `router`.
- Keep OS/window-manager observations in `system`.
- Keep durable PTY and viewer transport in `terminal`.
- Use the Pi RPC/JSONL adapter for Pi programmatic intake; do not force Pi
  through terminal injection when the daemon has a typed Pi adapter
  configuration.
- Keep the component triad surface split: `harness` is the ordinary
  `signal-harness` CLI, `meta-harness` is the `meta-signal-harness`
  policy CLI, and `harness-daemon` is the managed runtime process.
- The owner-only daemon socket recognizes `meta-signal-harness` before
  falling back to Persona supervision while both management surfaces exist.
- One `harness-daemon` component process may own multiple harness instances.
  Add per-harness boundaries as `HarnessInstanceConfiguration` records and
  in-process actors/adapters; do not spawn one daemon process per harness
  unless a deployment-isolation requirement is explicit.
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
- The kernel grammar at `signal-frame/macros/src/validate.rs`
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
