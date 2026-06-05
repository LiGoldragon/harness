# INTENT — harness

`harness` models interactive AI harnesses as addressable runtime objects. It owns the
reusable abstraction for Codex, Claude, and Pi harnesses: identity, lifecycle state,
typed transcript observations, sequence pointers, adapter capabilities, and terminal
delivery adaptation. It does not own routing policy, OS/window focus observation, or
terminal PTY byte transport. Today's harness is a realization step on the
eventually-self-hosting stack.

`HarnessKind` is a closed four-variant schema — production `Codex`, `Claude`, `Pi`, and
an explicit `Fixture` variant for test harnesses. Later production harnesses become
explicit schema variants, never `Other { name }` string payloads. `HarnessKind` is not
argv state: the daemon takes it from a single typed `HarnessDaemonConfiguration` record
(inline NOTA, `.nota` path, or signal-encoded `.rkyv` path), preserving the closed enum
while keeping startup inside the single-argument rule. The daemon answers supervision
through a canonical `SupervisionPhase` actor, binds `harness.sock` at the managed
spawn-envelope socket mode before accepting traffic, and replies
`HarnessRequestUnimplemented` for valid contract operations not yet built — never a panic
or untyped text.

Key constraints: harnesses are first-class records. Harness identity has an explicit
typed visibility axis (`Full`, `Redacted`, `Hidden`); redaction is typed, not a string
filter, and is a read-path projection, not a runtime authorization gate — runtime
permission lives in filesystem ACLs plus router channel state choreographed by mind.
Transcript and lifecycle observations are pushed typed events, never polled; the internal
event count is a sequencing counter, not the observation surface. Each open transcript
subscription is owned by a per-subscription `TranscriptStreamingReplyHandler` actor with a
bounded outbound buffer — a slow consumer holds back only its own stream and on overrun
drops with a typed failure rather than overrunning the consumer. Live harness lifecycle
and transcript state belong inside Kameo actors, never loose shared mutable objects; the
fanout plane is in-process Kameo mailbox sends, no shared `Arc<Mutex<…>>`. Adapter
capabilities are explicit typed records, not stringly flags. Only the `FixtureOnlyHuman`
endpoint may complete without sending bytes to terminal transport; production delivery
counts an input as delivered only after the terminal accepts the bytes, and reports typed
`DeliveryFailed` when no terminal endpoint is available. The daemon accepts only
length-prefixed `signal-harness` frames. When durable harness history is needed, the
harness actor opens its own `harness.redb` through a harness-owned Sema layer and
sequences its own writes — no shared cross-component database, and no write ownership over
any other component's Sema layer.
