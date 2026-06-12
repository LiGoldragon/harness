# INTENT — harness

`harness` models interactive AI harnesses as addressable runtime objects. It owns the
reusable abstraction for Codex, Claude, and Pi harnesses: identity, lifecycle state,
typed transcript observations, sequence pointers, adapter capabilities, terminal
delivery adaptation, and Pi RPC/JSONL intake. It does not own routing policy,
OS/window focus observation, or terminal PTY byte transport. Today's harness is a
realization step on the eventually-self-hosting stack.

`HarnessKind` is a closed four-variant schema — production `Codex`, `Claude`, `Pi`, and
an explicit `Fixture` variant for test harnesses. Later production harnesses become
explicit schema variants, never `Other { name }` string payloads. `HarnessKind` is not
argv state: the daemon takes it from a typed `HarnessInstanceConfiguration` inside the
single signal-encoded/rkyv `HarnessDaemonConfiguration` startup record, preserving the
closed enum while keeping startup inside the one-argument daemon rule. The daemon does
not decode inline NOTA or `.nota` configuration; authored NOTA belongs to deploy/test
tools that encode the binary startup record before process launch. One `harness-daemon`
component process may own multiple harness instances internally; per-harness boundaries
are actors/adapters, not separate daemon processes. The daemon answers supervision
through a canonical `SupervisionPhase` actor, binds `harness.sock` at the managed
spawn-envelope socket mode before accepting traffic, and replies
`HarnessRequestUnimplemented` for valid contract operations not yet built — never a panic
or untyped text.

Pi harness delivery has a typed RPC/JSONL adapter path. When a
`HarnessInstanceConfiguration` carries a `PiRpcJsonlAdapterConfiguration`, the daemon
owns a long-lived `pi --mode rpc` process for that harness instance, sends routed
messages as the configured `prompt`/`steer`/`follow_up` command, and marks delivery
completed only after Pi's JSONL response accepts the command. This is the programmatic
Pi intake path; the terminal adapter remains for terminal-backed harnesses and fixtures.

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
endpoint may complete without sending bytes to terminal transport; production terminal
delivery counts an input as delivered only after the terminal accepts the bytes, and Pi
RPC delivery counts a message as delivered only after the RPC sidecar accepts the command.
The daemon reports typed `DeliveryFailed` when no adapter endpoint is available. The
daemon accepts only length-prefixed `signal-harness` frames on its working socket.
Every component exposes its working signal and meta policy contracts as two thin CLI
clients. For `harness`, `harness` is the ordinary `signal-harness` client and
`meta-harness` is the `meta-signal-harness` policy client. The daemon's owner-only
meta socket recognizes `meta-signal-harness` first, then falls back to Persona
supervision while the component manager still carries both surfaces. The
message-routing e2e witness must exercise a real request and reply path through real
`message-daemon`, `router-daemon`, and one `harness-daemon` process that owns both
harness instances before it can be described as a round-trip daemon witness; a single
routed delivery into an acceptance socket is only a one-way routing witness. When
durable harness history is needed, the harness actor opens its own `harness.sema`
through a harness-owned Sema layer backed by `sema-engine` and sequences its own writes
— no shared cross-component database, and no write ownership over any other component's
Sema layer.
