# harness

`flow-id` is the parent-flow identity helper. Run `flow-id codex --flows-root
ABSOLUTE_DIRECTORY` after Codex provides `CODEX_SESSION_ID`, or `flow-id
claude --flows-root ABSOLUTE_DIRECTORY --parent-session UUID` when Claude's
authoritative parent identity is known. Codex normalizes its UUID and claims
from `[23:29]` onward. Claude accepts only a canonical lowercase RFC 4122
UUIDv4 parent session and claims its first six literal hexadecimal characters.
Both extend the candidate by the following eligible hexadecimal character only
when a lane collides. It prints only the claimed hex alias. The parent passes
that alias as `FLOW_ID` and `flows-root/FLOW_ID` as `FLOW_DIRECTORY`; child
threads never invoke it.

Each alias has a private stable claim lock and a private versioned marker. The
helper takes the lock before reading marker contents, writes marker metadata to
a same-directory private temporary file, and publishes only a complete marker.

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

The component surface has two thin CLI clients and one daemon:
`harness` sends ordinary `signal-harness` requests, `meta-harness` sends
privileged `meta-signal-harness` policy requests, and `harness-daemon`
serves the managed runtime sockets from a single binary startup record.
