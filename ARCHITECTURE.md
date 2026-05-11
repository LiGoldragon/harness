# persona-harness — architecture

*Harness identity, lifecycle, transcript, and adapter contracts.*

`persona-harness` models interactive AI harnesses as addressable runtime
objects. Codex, Claude, Pi, and later harnesses become typed records with
lifecycle state, transcript streams, and delivery capabilities. The
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
    "Harness" -->|"transcript event"| "persona-router"
    "Harness" -->|"harness-owned state"| "harness Sema"
```

## 1 · Component Surface

`persona-harness` exposes:

- harness identity records;
- lifecycle state;
- transcript events;
- adapter capability records;
- terminal delivery adapter records;
- a Kameo harness actor surface for the assembled runtime;
- test fixtures for fake harnesses.

## 2 · State and Ownership

The harness component owns live harness identity and lifecycle state.
Transcript and lifecycle events are typed observations. `Harness` is the
mailbox-backed owner for one live harness binding, its lifecycle state, and its
transcript event count.

Harness identity views are connection-class aware. `Owner` and
`System(persona)` connections may receive full harness identity records.
`NonOwnerUser` connections receive redacted views by default. `OtherPersona`
connections see harness identity only through an explicit `EngineRoute`
projection; no incidental identity leak is acceptable.

When durable harness history is needed, the harness actor opens its **own**
redb file (e.g. `harness.redb`) through a harness-owned Sema layer over the
workspace's `sema` database library. The harness actor sequences its own
writes; no shared cross-component database. Per
`~/primary/reports/designer/92-sema-as-database-library-architecture-revamp.md`.

## 3 · Boundaries

This repo owns:

- harness domain types;
- class-aware harness identity projections;
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

## Code Map

```text
src/harness.rs    harness identity records
src/runtime.rs    Kameo lifecycle and transcript state owner
src/terminal.rs   terminal delivery adapter records
src/transcript.rs transcript event records
tests/               harness smoke and actor-runtime constraint tests
```

## See Also

- `../persona-router/ARCHITECTURE.md`
- `../persona-system/ARCHITECTURE.md`
- `../persona-terminal/ARCHITECTURE.md`
- `../sema/ARCHITECTURE.md`
- `../signal-persona-harness/ARCHITECTURE.md`
