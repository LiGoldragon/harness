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
    "persona-router" -->|"delivery request"| "HarnessActor"
    "HarnessActor" -->|"adapter command"| "HarnessAdapter"
    "HarnessAdapter" -->|"terminal transport"| "persona-wezterm"
    "HarnessActor" -->|"transcript event"| "persona-router"
    "HarnessActor" -->|"state commit"| "persona-store"
```

## 1 · Component Surface

`persona-harness` exposes:

- harness identity records;
- lifecycle state;
- transcript events;
- adapter capability records;
- a harness actor surface for the assembled runtime;
- test fixtures for fake harnesses.

## 2 · State and Ownership

The harness component owns live harness identity and lifecycle state. Transcript
and lifecycle events are typed observations; durable history is committed
through `persona-store` in the assembled runtime.

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
- shared signal definitions (`persona-signal`);
- database write ownership (`persona-store`).

## 4 · Invariants

- Harnesses are first-class records.
- A closed viewer does not imply a killed harness.
- Transcript and lifecycle observations are pushed events.
- Adapter capabilities are explicit typed records, not stringly flags.

## Code Map

```text
src/harness.rs     harness identity and lifecycle records
src/transcript.rs  transcript event records
tests/             harness smoke tests
```

## See Also

- `../persona-router/ARCHITECTURE.md`
- `../persona-system/ARCHITECTURE.md`
- `../persona-wezterm/ARCHITECTURE.md`
- `../persona-store/ARCHITECTURE.md`
