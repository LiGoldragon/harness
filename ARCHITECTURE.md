# Persona Harness Architecture

`persona-harness` models interactive coding harnesses as addressable runtime
objects.

```mermaid
flowchart LR
  Router[persona-router] --> Actor[HarnessActor]
  Actor --> Adapter[HarnessAdapter]
  Adapter --> PTY[terminal session]
  Adapter --> Transcript[transcript stream]
  Adapter --> Input[buffer observer]
```

The repository is deliberately below routing policy. It should expose what a
harness can do and what it observed, not decide global message flow.
