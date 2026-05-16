# persona-harness — architecture

*Harness identity, lifecycle, transcript, and adapter contracts.*

`persona-harness` models interactive AI harnesses as addressable runtime
objects. `HarnessKind` is the closed schema of harness kinds — production
variants `Codex`, `Claude`, `Pi`, and the explicit `Fixture` variant for
test fixtures. Later production harnesses become explicit variants, not
`Other { name }` string payloads. Harnesses carry lifecycle state, typed
transcript observations, sequence pointers, and delivery capabilities. The
Persona-facing terminal contract is `signal-persona-terminal`; terminal
transport execution is delegated to `persona-terminal`.

> **Scope.** Any "sema" reference here means today's `sema` library
> (rename pending → `sema-db`). The eventual `Sema` is broader;
> today's persona-harness is a realization step. See
> `~/primary/ESSENCE.md` §"Today and eventually".

---

## 0 · TL;DR

This repo owns the harness abstraction. It does not own routing policy,
OS-specific focus observation, or terminal durable PTY transport.

```mermaid
flowchart LR
    "persona-router" -->|"delivery request"| "Harness"
    "Harness" -->|"adapter command"| "HarnessAdapter"
    "HarnessAdapter" -->|"terminal transport"| "persona-terminal"
    "Harness" -->|"typed observation + sequence pointer"| "persona-router"
    "Harness" -->|"harness-owned state"| "harness Sema"
```

## 1 · Component Surface

`persona-harness` exposes:

- a `persona-harness-daemon` skeleton binary for the first-stack engine
  supervision witness;
- harness identity records;
- lifecycle state;
- transcript events;
- adapter capability records;
- terminal delivery adapter records;
- a Kameo harness actor surface for the assembled runtime;
- test fixtures for fake harnesses.

The only endpoint that may complete without sending bytes to terminal
transport is `FixtureOnlyHuman`. It is a fixture endpoint, not production
delivery. Production terminal delivery uses the `persona-terminal` transport
binding and counts an input as delivered only after
`TerminalEvent::TerminalInputAccepted`.

## 1.5 · Lifecycle FSM and supervision-relation reception

The harness daemon answers `signal-persona::SupervisionRequest` from a
canonical `SupervisionPhase` Kameo actor. The daemon reads its
`signal-persona::SpawnEnvelope` at startup, binds `harness.sock` at
mode 0600 by applying the `PERSONA_SOCKET_MODE` value from the Persona spawn
envelope, and proceeds.

The daemon takes its `HarnessKind` from `--kind <codex|claude|pi|fixture>`
on argv. When `--kind` is omitted, the daemon defaults to `Fixture` so a
mis-launched production daemon cannot silently impersonate a real harness
kind. Production callers must pass `--kind` explicitly.

**Harness lifecycle FSM** (closed enum):

```text
HarnessLifecycle
  | Starting     -- spawned, awaiting first ready signal
  | Running      -- ready to accept MessageDelivery
  | Paused       -- temporarily suspended (no new deliveries; in-flight complete)
  | Stopped      -- exited (clean or crash; distinguishable via exit_code)
```

Readiness mapping for `SupervisionRequest::ComponentReadinessQuery`:

- `Running` and `Paused` → `ComponentReady { component_started_at }`
- `Starting` and `Stopped` → `ComponentNotReady { reason }`

Unbuilt domain operations reply
`HarnessEvent::HarnessRequestUnimplemented` rather than panicking or
printing untyped text.

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
redb file (e.g. `harness.redb`) through a harness-owned Sema layer over the
workspace's `sema` database library. The harness actor sequences its own
writes; no shared cross-component database.

## 3 · Boundaries

This repo owns:

- harness domain types;
- read-path harness identity projections;
- harness actor lifecycle;
- transcript event shape;
- adapter contracts.
- harness-owned terminal delivery adaptation.

This repo does not own:

- routing decisions (`persona-router`);
- OS/window focus backend (`persona-system`);
- PTY byte transport (`persona-terminal`);
- harness wire contract definitions (`signal-persona-harness`);
- terminal wire contract definitions (`signal-persona-terminal`);
- the top-level engine-manager contract (`signal-persona`);
- database write ownership for other components' Sema layers.

## 4 · Invariants

- Harnesses are first-class records.
- Harness identity has an explicit visibility axis; redaction is typed, not a
  string filter.
- A closed viewer does not imply a killed harness.
- Transcript and lifecycle observations are pushed events.
- Live harness lifecycle and transcript state belongs inside Kameo actors.
- Adapter capabilities are explicit typed records, not stringly flags.
- Fixture-only terminal endpoints cannot claim real terminal delivery.
- The daemon accepts length-prefixed `signal-persona-harness` frames.
- The daemon applies the managed spawn-envelope socket mode to `harness.sock`
  before accepting client traffic.
- The daemon turns `MessageDelivery` into terminal input only when a typed
  terminal endpoint was provided by its spawn envelope or CLI.
- The daemon reports `DeliveryCompleted` only after terminal transport accepts
  the input bytes.
- The daemon reports typed `DeliveryFailed` when no terminal endpoint is
  available.
- The daemon answers `HarnessStatusQuery` with typed health and readiness.
- The daemon returns `HarnessRequestUnimplemented` for valid contract
  operations that are not built yet.
- The daemon does not print untyped text errors for recognized unfinished
  operations.

## Code Map

```text
src/harness.rs    harness identity records
src/daemon.rs     length-prefixed Signal daemon skeleton
src/runtime.rs    Kameo lifecycle and transcript state owner
src/terminal.rs   terminal delivery adapter records
src/transcript.rs transcript event records
tests/            harness smoke and actor-runtime constraint tests
```

## Constraint Tests

| Constraint | Test |
|---|---|
| Harness identity projection keeps full, redacted, and hidden views distinct. | `nix flake check .#harness-identity-projection-views` |
| Harness identity projection cannot collapse back to one always-full record. | `nix flake check .#harness-identity-projection-source-constraint` |
| Fixture-only human terminal endpoints cannot claim production delivery. | `nix flake check .#terminal-fixture-endpoint-not-production-delivery` |
| `HarnessKind` has exactly four variants and no fifth. | `nix flake check .#harness-kind-includes-all-four-variants` |
| `--kind` argument values round-trip through the typed parser. | `nix flake check .#harness-kind-argument-value-round-trips` |
| Harness daemon accepts `--kind` from CLI as a typed argument. | `nix flake check .#harness-command-line-accepts-typed-kind` |
| Harness daemon defaults to `HarnessKind::Fixture` when `--kind` is omitted. | `nix flake check .#harness-command-line-defaults-kind-to-fixture` |
| Harness daemon rejects unknown `--kind` values with a typed diagnostic. | `nix flake check .#harness-command-line-rejects-unknown-kind` |
| Harness daemon applies the managed spawn-envelope socket mode. | `nix flake check .#harness-daemon-applies-spawn-envelope-socket-mode` |
| Harness daemon flows distinctive socket modes through to both the domain and supervision sockets. | `nix flake check .#harness-daemon-applies-distinctive-spawn-envelope-socket-modes` |
| Harness daemon delivers message bytes to a configured terminal endpoint. | `nix flake check .#harness-daemon-delivers-message-to-terminal-endpoint` |
| Harness daemon rejects message delivery without a terminal endpoint. | `nix flake check .#harness-daemon-rejects-message-delivery-without-terminal-endpoint` |
| Harness daemon answers status/readiness through its Signal boundary. | `nix flake check .#harness-daemon-answers-status-readiness` |
| Harness daemon returns typed unimplemented for valid unfinished requests. | `nix flake check .#harness-daemon-returns-typed-unimplemented` |

## See Also

- `../persona-router/ARCHITECTURE.md`
- `../persona-system/ARCHITECTURE.md`
- `../persona-terminal/ARCHITECTURE.md`
- `../sema/ARCHITECTURE.md`
- `../signal-persona-harness/ARCHITECTURE.md`
