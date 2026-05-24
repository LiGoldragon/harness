# Persona Harness — Agent Instructions

Read `/home/li/primary/AGENTS.md` first, then `/home/li/primary/lore/AGENTS.md`.
This repository follows the primary workspace orchestration protocol.

## Purpose

`harness` owns typed harness identity, lifecycle, transcript capture,
input observations, and adapter contracts for interactive agent harnesses.

## Local Rules

- Use Jujutsu for version control.
- Keep repositories public unless the human gives a specific reason otherwise.
- Use Nix for build and test entry points.
- Harness adapters are data-bearing objects with methods.
- Harness identity views are typed read-path projections. Keep full, redacted,
  and hidden views distinct; do not expose raw harness bindings to every
  caller. They are not runtime authorization gates.
- Do not inject through the same stream a human is typing into unless an input
  gate proves the buffer is safe. The router decides delivery; harness objects
  perform the adapter-specific action.
- Durable harness state uses `redb + rkyv` when this crate owns it.
