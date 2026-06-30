# harness — architecture

*Harness identity, lifecycle, transcript, and adapter contracts.*

`harness` models interactive AI harnesses as addressable runtime
objects. `HarnessKind` is the closed four-variant schema — production
variants `Codex`, `Claude`, `Pi`, and the explicit `Fixture` variant for
test harnesses. Later production harnesses become explicit variants, not
`Other { name }` string payloads. Harnesses carry lifecycle state, typed
transcript observations, sequence pointers, and delivery capabilities.

The Persona-facing terminal contract is `signal-terminal`. The
destination shape for harness → terminal delivery is a typed
`signal-terminal` request/reply exchanged as a length-prefixed
Signal frame on the terminal signal socket. The harness runtime
writes the generated `TerminalFrame` directly; it does not depend on the
retired in-process `terminal` helper crate.

The Pi-facing intake contract is Pi RPC/JSONL over stdio. A Pi-kind
harness instance may be launched with a typed
`PiRpcJsonlAdapterConfiguration` in its
`HarnessInstanceConfiguration`; the daemon then owns a long-lived
`pi --mode rpc` process for that instance and converts routed
`MessageDelivery` records into the configured `prompt`, `steer`, or
`follow_up` JSONL command. Delivery completes only when Pi emits the
matching successful JSONL response.

Transcript and worker-lifecycle observations are pushed as typed events
over the harness observation channel defined by `signal-harness`.
Subscribers receive `TranscriptEvent` and lifecycle-transition frames as
they happen; observation flow is push, never poll. Transitional: the
runtime's internal `transcript_event_count` is a sequencing counter, not
the observation surface; the typed observation stream is.

> **Scope.** Harness does not yet need durable history, but when it does the
> component-owned store is `harness.sema` opened through `sema-engine`; the
> daemon does not touch raw redb or another component's store.

## 0 · TL;DR

This repo owns the harness abstraction. It does not own routing policy,
OS-specific focus observation, or terminal durable PTY transport.

```mermaid
flowchart LR
    "router" -->|"delivery request"| "Harness"
    "Harness" -->|"adapter command"| "HarnessAdapter"
    "HarnessAdapter" -->|"TerminalFrame"| "signal-terminal"
    "HarnessAdapter" -->|"Pi RPC JSONL"| "pi --mode rpc"
    "Harness" -->|"typed observation + sequence pointer"| "router"
    "Harness" -->|"harness-owned state"| "harness Sema"
```

## 1 · Component Surface

`harness` exposes:

- `harness`, the ordinary thin CLI client for `signal-harness`;
- `meta-harness`, the owner/meta thin CLI client for
  `meta-signal-harness`;
- `harness-daemon`, the managed runtime daemon that binds the working
  and owner-only meta sockets from a single binary startup record;
- harness identity records;
- lifecycle state;
- transcript events;
- adapter capability records;
- terminal delivery adapter records;
- Pi RPC/JSONL delivery adapter records;
- a Kameo harness actor surface for the assembled runtime;
- test fixtures for fake harnesses.

The only endpoint that may complete without sending bytes to terminal
transport is `FixtureOnlyHuman`. It is a fixture endpoint, not production
delivery. Production terminal delivery uses the `signal-terminal`
contract and counts an input as delivered only after
`TerminalReply::TerminalInputAccepted`. Pi RPC delivery counts as delivered
only after the configured RPC command is accepted by the Pi JSONL response
stream.

## 1.5 · Lifecycle FSM and supervision-relation reception

The harness daemon first decodes owner-only meta traffic as
`meta-signal-harness`. Its current `Configure` implementation is not built
yet, so it replies with typed `MetaHarnessReply::RequestUnimplemented`.
If a frame is not the meta-harness contract, the same socket falls back to
Persona supervision and answers from a canonical `SupervisionPhase` Kameo
actor. The daemon receives exactly one
startup argument: a `signal_harness::HarnessDaemonConfiguration`
record supplied as a signal-encoded/rkyv file path. Inline NOTA and `.nota`
startup files are rejected before daemon-specific decoding. That record carries
the harness socket path and mode, supervision socket path and mode, owner
identity, and a list of typed
`HarnessInstanceConfiguration` records. Each instance record carries the
harness name, `HarnessKind`, optional terminal socket, and optional
`PiRpcJsonlAdapterConfiguration` that starts the programmatic Pi intake
process for that harness.

`HarnessKind` is not argv state. The daemon takes it from each
`HarnessInstanceConfiguration::harness_kind`, preserving the closed enum
while keeping process startup inside the workspace single-argument rule.
One `harness-daemon` process may own multiple harness instances; those
boundaries are in-process actors/adapters unless a future deployment
requires process isolation.

**Harness lifecycle FSM** (closed enum):

```text
HarnessLifecycle
  | Starting     -- spawned, awaiting first ready signal
  | Running      -- ready to accept MessageDelivery
  | Paused       -- temporarily suspended (no new deliveries; in-flight complete)
  | Stopped      -- exited (clean or crash; distinguishable via exit_code)
```

Readiness mapping for `Operation::Query(ReadinessStatus)`:

- `Running` and `Paused` → `ComponentReady { component_started_at }`
- `Starting` and `Stopped` → `ComponentNotReady { reason }`

Unbuilt domain operations reply
`HarnessEvent::HarnessRequestUnimplemented` rather than panicking or
printing untyped text.

## 1.6 · Transcript-observation subscription delivery

The harness is the destination push primitive for its own transcript
state. The subscription contract is `signal-harness`'s
`HarnessTranscriptStream` (Watch → typed snapshot → typed deltas
→ typed Unwatch → typed final ack → end). The runtime side owns the
producer plane.

Three named actors carry the producer side:

| Actor | Owns |
|---|---|
| `TranscriptSubscriptionManager` | The set of open subscriptions: per-token handler reference, registration metadata, ingress count. Routes `WatchHarnessTranscript` and `UnwatchHarnessTranscript` to handlers. |
| `TranscriptStreamingReplyHandler` | One per open subscription. Holds the connection, the per-stream `HarnessTranscriptToken`, the sequence cursor, the local outbound buffer, and the close-ack flag. Receives `DeliverTranscriptDelta` from the publisher; writes the event onto the wire. |
| `TranscriptDeltaPublisher` | The fanout plane. Receives `TranscriptObservation` records from the `Harness` runtime; sends `DeliverTranscriptDelta { observation }` to every registered handler. |

The publisher fans out by in-process Kameo mailbox sends; the
manager → handler edge is also a mailbox send. No shared
`Arc<Mutex<…>>` carries the subscription set; each handler's mailbox
IS its per-consumer queue, and one slow handler stalls only its own
mailbox.

The full canonical five-state lifecycle (per
`~/primary/skills/subscription-lifecycle.md`):

```mermaid
stateDiagram-v2
    [*] --> Subscribing : WatchHarnessTranscript
    Subscribing --> Streaming : HarnessTranscriptSnapshot (open snapshot)
    Streaming --> Streaming : TranscriptObservation (delta)
    Streaming --> Retracting : UnwatchHarnessTranscript
    Retracting --> Closed : HarnessSubscriptionRetracted (final ack)
    Closed --> [*]
```

## 2 · State and Ownership

The harness component owns live harness identity and lifecycle state.
Transcript and lifecycle events are typed observations. Normal fanout carries
typed observations plus sequence pointers, not broad raw transcript bytes.
`Harness` is the mailbox-backed owner for one live harness binding, its
lifecycle state, and its transcript event count.

Harness identity views are read-path projections: `Full`, `Redacted`, or
`Hidden`. The current code names the local view selector
`HarnessIdentityView`. It is not an authorization gate. Raw transcript
access stays behind explicit later range queries; `HarnessKind` is a
closed enum. Runtime permission lives in filesystem ACLs plus router
channel state choreographed by mind.

When durable harness history is needed, the harness actor opens its **own**
harness Sema file (e.g. `harness.sema`) through a harness-owned Sema layer
backed by `sema-engine`. The harness actor sequences its own writes; no shared
cross-component database.

## 3 · Boundaries

This repo owns:

- harness domain types;
- read-path harness identity projections;
- harness actor lifecycle;
- transcript event shape;
- adapter contracts.
- harness-owned terminal delivery adaptation.
- harness-owned Pi RPC/JSONL delivery adaptation.

This repo does not own:

- routing decisions (`router`);
- OS/window focus backend (`system`);
- PTY byte transport (`terminal`);
- harness wire contract definitions (`signal-harness`);
- terminal wire contract definitions (`signal-terminal`);
- the top-level engine-management contract (`signal-engine-management`);
- Pi's internal model/runtime implementation;
- database write ownership for other components' Sema layers.

## 4 · Invariants

- Harnesses are first-class records.
- Harness identity has an explicit visibility axis; redaction is typed, not a
  string filter.
- A closed viewer does not imply a killed harness.
- Transcript and lifecycle observations are pushed events.
- Transcript observation is push, not poll. Internal event count is not the
  observation surface; the typed observation stream is.
- Live harness lifecycle and transcript state belongs inside Kameo actors.
- Adapter capabilities are explicit typed records, not stringly flags.
- Fixture-only terminal endpoints cannot claim real terminal delivery.
- The daemon accepts length-prefixed `signal-harness` frames.
- The `harness` CLI sends one ordinary `signal-harness` request and prints
  one typed NOTA reply.
- The `meta-harness` CLI sends one privileged `meta-signal-harness` request
  and prints one typed NOTA reply.
- The daemon's meta socket recognizes `meta-signal-harness` before falling
  back to Persona supervision.
- The daemon applies the managed spawn-envelope socket mode to `harness.sock`
  before accepting client traffic.
- The daemon turns `MessageDelivery` into terminal input only when a typed
  terminal endpoint was provided by its spawn envelope or CLI.
- The daemon reports `DeliveryCompleted` only after terminal transport accepts
  the input bytes.
- The daemon reports `DeliveryCompleted` for Pi only after the Pi RPC process
  emits a successful matching JSONL response for the configured delivery
  command.
- The daemon reports typed `DeliveryFailed` when no adapter endpoint is
  available.
- The message-routing e2e witness is a round-trip only when a real first
  `message` CLI call reaches another harness through real `message-daemon`,
  `router-daemon`, and one `harness-daemon` process owning both harness
  instances, the receiving endpoint sends a reply through its own real
  `message` CLI and message daemon, and the first harness receives that
  response.
- The daemon answers `HarnessStatusQuery` with typed health and readiness.
- The daemon returns `HarnessRequestUnimplemented` for valid contract
  operations that are not built yet.
- The daemon does not print untyped text errors for recognized unfinished
  operations.
- The daemon accepts `WatchHarnessTranscript`, replies with a typed
  `HarnessTranscriptSnapshot` carrying the per-stream token and the
  current sequence pointer, then pushes `TranscriptObservation` events
  as transcript lines become visible.
- Each open transcript subscription is owned by a per-subscription
  `TranscriptStreamingReplyHandler` actor; a slow consumer holds back
  its own stream and cannot block siblings.
- The daemon accepts `UnwatchHarnessTranscript` for an open
  subscription, drains the in-flight delta queue, emits the final
  `HarnessSubscriptionRetracted` reply carrying the same token, and
  closes the stream.
- The handler's outbound delta buffer is bounded; on overrun the
  subscription drops with a typed failure reply rather than overrunning
  the consumer.
- Transcript deltas carry a strictly-increasing `HarnessTranscriptSequence`.

## Code Map

```text
src/main.rs               ordinary signal-harness CLI
src/bin/meta_harness.rs   meta-signal-harness CLI
src/bin/harness_daemon.rs managed daemon entrypoint
src/client.rs             ordinary CLI client transport
src/meta.rs               meta CLI client transport
src/configuration.rs      BindingSurface over HarnessDaemonConfiguration
src/daemon.rs             length-prefixed Signal daemon hooks
src/harness.rs            harness identity records
src/runtime.rs            Kameo lifecycle and transcript state owner
src/terminal.rs           terminal delivery adapter records
src/pi.rs                 Pi RPC/JSONL process adapter
src/transcript.rs         transcript event records
tests/                    harness smoke, daemon, CLI, and actor-runtime tests
```

## Constraint Tests

| Constraint | Test |
|---|---|
| Harness identity projection keeps full, redacted, and hidden views distinct. | `nix flake check .#harness-identity-projection-views` |
| Harness identity projection cannot collapse back to one always-full record. | `nix flake check .#harness-identity-projection-source-constraint` |
| Fixture-only human terminal endpoints cannot claim production delivery. | `nix flake check .#terminal-fixture-endpoint-not-production-delivery` |
| `HarnessKind` has exactly four variants and no fifth. | `nix flake check .#harness-kind-includes-all-four-variants` |
| `HarnessKind` has no command-line argument projection table. | `nix flake check .#harness-kind-has-no-command-line-argument-projection` |
| Harness daemon accepts `HarnessKind::Fixture` from a single binary configuration argument. | `cargo test --test daemon harness_daemon_accepts_fixture_kind_from_single_binary_configuration_argument` |
| Harness daemon accepts `HarnessKind::Codex` from a single binary configuration argument. | `cargo test --test daemon harness_daemon_accepts_codex_kind_from_single_binary_configuration_argument` |
| Harness daemon rejects inline NOTA and `.nota` configuration arguments. | `cargo test --test daemon harness_daemon_configuration_rejects` |
| Harness daemon rejects multiple configuration arguments before daemon construction. | `nix flake check .#harness-daemon-configuration-rejects-multiple-arguments` |
| Harness daemon applies the configured working socket mode. | `nix flake check .#harness-daemon-binds-working-socket-with-configured-mode` |
| Harness daemon uses the configured working socket mode while keeping the owner-only meta socket at daemon-shape mode. | `nix flake check .#harness-daemon-applies-configured-working-socket-mode-and-owner-only-supervision` |
| Harness daemon delivers message bytes to a configured terminal endpoint. | `nix flake check .#harness-daemon-delivers-message-to-terminal-endpoint` |
| Harness daemon dispatches two harness instances inside one process by `HarnessName`. | `cargo test --test daemon harness_daemon_dispatches_two_harness_instances_inside_one_process` |
| Harness daemon delivers Pi-kind messages through the Pi RPC/JSONL adapter. | `cargo test --test daemon harness_daemon_delivers_message_to_pi_rpc_endpoint` |
| The Pi RPC adapter can accept a prompt through the low-quant Gemma 4 MoE local model when the live endpoint is available. | `HARNESS_LIVE_PI_RPC=1 HARNESS_LIVE_PI_MODEL=gemma-4-26b-a4b-ud-q4-k-xl cargo test --test pi_rpc_live -- --nocapture` |
| Harness daemon rejects message delivery without a terminal endpoint. | `nix flake check .#harness-daemon-rejects-message-delivery-without-terminal-endpoint` |
| Harness daemon answers status/readiness through its Signal boundary. | `nix flake check .#harness-daemon-answers-status-readiness` |
| Harness daemon returns typed unimplemented for valid unfinished requests. | `nix flake check .#harness-daemon-returns-typed-unimplemented` |
| Harness daemon recognizes the meta-harness policy contract before Persona supervision fallback. | `nix flake check .#harness-daemon-answers-meta-harness-relation` |
| `harness` reaches the ordinary working socket and prints a typed reply. | `nix flake check .#harness-cli-reaches-working-socket` |
| `meta-harness` reaches the owner/meta policy socket and prints a typed reply. | `nix flake check .#meta-harness-cli-reaches-policy-socket` |
| Harness daemon opens a transcript subscription, returns a typed snapshot, and pushes typed deltas plus the final ack on the subscribed stream. | `nix build .#checks.<system>.harness-daemon-watch-transcript-stream-delivers-published-observation-and-final-ack` |
| A subscriber receives the final `HarnessSubscriptionRetracted` ack carrying the same token before the stream ends. | `nix build .#checks.<system>.harness-daemon-unwatch-transcript-returns-final-retraction-ack-on-subscribed-stream` |
| Multiple simultaneous watchers for the same harness receive independent stream frames, and closing one watcher does not close the other. | `nix build .#checks.<system>.harness-daemon-allows-nested-watchers-for-same-harness-without-cross-closing` |
| A slow subscriber does not stall transcript-delta delivery to a sibling subscription. | `cargo test --test subscription_truth slow_subscriber_does_not_block_sibling_subscription` |
| A real `message` CLI call reaches a second Pi-kind harness through real message/router daemons and one multi-instance harness daemon, the receiving endpoint replies through its own real `message` CLI and daemon, and the first harness receives the response. | `cargo test --test message_router_harness_e2e` |

## See Also

- `~/primary/skills/subscription-lifecycle.md` — canonical
  five-state FSM the transcript subscription implements.

- `../router/ARCHITECTURE.md`
- `../system/ARCHITECTURE.md`
- `../terminal/ARCHITECTURE.md`
- `../sema/ARCHITECTURE.md`
- `../signal-harness/ARCHITECTURE.md`
