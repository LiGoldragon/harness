# persona-harness

Typed harness abstraction for Persona.

This crate holds the reusable model for Codex, Claude, Pi, and other
interactive harnesses: identity, lifecycle, transcript events, and adapter
capabilities. Live harness lifecycle and transcript counters are owned by a
Kameo `Harness` so assembled runtimes can push state changes through a
mailbox instead of sharing loose mutable objects.

Harness identity is projected through typed visibility levels. Full views keep
identity, kind, and working directory; redacted views expose only the harness
id; hidden views expose no incidental harness identity.
