# harness

Typed harness abstraction for Persona.

This crate holds the reusable model for Codex, Claude, and Pi interactive
harnesses: identity, lifecycle, transcript events, and adapter capabilities.
Future production harness kinds become explicit schema variants, not string
payloads. Live harness lifecycle and transcript counters are owned by a Kameo
`Harness` so assembled runtimes can push state changes through a mailbox
instead of sharing loose mutable objects.

Harness identity is projected through typed read views. Full views keep
identity, kind, and working directory; redacted views expose only the harness
id; hidden views expose no incidental harness identity. These views are not
runtime authorization gates.
