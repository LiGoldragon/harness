# persona-harness — architecture

*Harness identity, lifecycle, transcript, and adapter contracts.*

`persona-harness` models interactive AI harnesses as addressable runtime
objects. Codex, Claude, Pi, and later harnesses become typed records with
lifecycle state, transcript streams, and delivery capabilities.

---

## 0 · TL;DR

This repo owns the harness abstraction. It does not own routing policy,
OS-specific focus observation, or WezTerm's durable PTY transport.

```mermaid
flowchart LR
    "persona-router" -->|"delivery request"| "Harness"
    "Harness" -->|"adapter command"| "HarnessAdapter"
    "HarnessAdapter" -->|"terminal transport"| "persona-wezterm"
    "Harness" -->|"transcript event"| "persona-router"
    "Harness" -->|"harness-owned state"| "persona-sema"
```

## 1 · Component Surface

`persona-harness` exposes:

- harness identity records;
- lifecycle state;
- transcript events;
- adapter capability records;
- a Kameo harness actor surface for the assembled runtime;
- test fixtures for fake harnesses.

## 2 · State and Ownership

The harness component owns live harness identity and lifecycle state.
Transcript and lifecycle events are typed observations. `Harness` is the
mailbox-backed owner for one live harness binding, its lifecycle state, and its
transcript event count.

When durable harness history is needed, the harness actor opens its **own**
redb file (e.g. `harness.redb`) through `persona-sema`, which uses the
workspace's `sema` database library underneath. The harness actor sequences
its own writes; no shared cross-component database. Per
`~/primary/reports/designer/92-sema-as-database-library-architecture-revamp.md`.

## 3 · Boundaries

This repo owns:

- harness domain types;
- harness actor lifecycle;
- transcript event shape;
- adapter contracts.

This repo does not own:

- routing decisions (`persona-router`);
- OS/window focus backend (`persona-system`);
- PTY and WezTerm byte transport (`persona-wezterm`);
- harness wire contract definitions (`signal-persona-harness`);
- umbrella Persona record definitions (`signal-persona`);
- database write ownership for other components (`persona-sema` users).

## 4 · Invariants

- Harnesses are first-class records.
- A closed viewer does not imply a killed harness.
- Transcript and lifecycle observations are pushed events.
- Live harness lifecycle and transcript state belongs inside Kameo actors.
- Adapter capabilities are explicit typed records, not stringly flags.

## Code Map

```text
src/harness.rs    harness identity records
src/runtime.rs    Kameo lifecycle and transcript state owner
src/transcript.rs transcript event records
tests/               harness smoke and actor-runtime constraint tests
```

## See Also

- `../persona-router/ARCHITECTURE.md`
- `../persona-system/ARCHITECTURE.md`
- `../persona-wezterm/ARCHITECTURE.md`
- `../persona-sema/ARCHITECTURE.md`
- `../signal-persona-harness/ARCHITECTURE.md`
