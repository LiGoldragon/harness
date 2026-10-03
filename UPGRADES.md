# Upgrades

## 0.6.0 — current contract family and the usage snapshot

Breaking wire change. Client, daemon and every peer move together.

- Every socket now speaks plain `signal` frames of exactly one contract
  root: `signal-harness` 8.0.0 on the ordinary socket, `meta-signal-harness`
  1.0.1 on a new owner-only meta socket, the `signal-persona` 4.0.0
  engine-management lifecycle on the supervision socket. The `signal-frame`
  exchange envelope is gone. A peer built against `signal-harness` 0.x
  (the Router's harness delivery, any Persona manager) cannot talk to 0.6.0.
- `HarnessDaemonConfiguration` gains `MetaSocketPath` and `MetaSocketMode`;
  its fields are plain values named after their types. A configuration file
  written by an older producer does not decode.
- `harness` and `meta-harness` take one inline Datom value and print Datom;
  NOTA arguments and `.nota` files are no longer read.
- New `harness-usage` prints the human view of `UsageSnapshotQuery`; the
  typed reply is `harness UsageSnapshotQuery`. The daemon answers it with an
  empty instance set, from the daemon user's own `HOME`.
- Transcript stream events arrive as `Response::HarnessTranscriptEvent`
  carrying their subscription token.

- New `harness-daemon-launch` runs the daemon as a user service: it writes
  the typed configuration for systemd's `RUNTIME_DIRECTORY` and the user's
  uid and becomes `harness-daemon`.

Deploy the CLI and the daemon from one package revision; restart any running
`harness-daemon` with a configuration written for 0.6.0.

## Flow identity helper

Install the Home generation that pins the `harness` revision carrying
`flow-id`. Parent flows claim their lane before writing their first artifact:
`flow-id codex --flows-root /absolute/flows-root` or `flow-id claude
--flows-root /absolute/flows-root --parent-session UUID`. Codex returns six
normalized hexadecimal UUID characters from `[23:29]`; Claude accepts a
canonical lowercase RFC 4122 UUIDv4 or UUIDv5 parent session with an RFC 4122
variant nibble and returns its first six literal hexadecimal characters. Both
extend only for collisions; the result
becomes `FLOW_ID`, and its lane becomes `FLOW_DIRECTORY`. Existing unmarked
lanes are collisions and are never overwritten.

Claude callers must pass the authoritative canonical lowercase UUIDv4 or UUIDv5
parent session; malformed UUIDs, unsupported UUID versions, and non-RFC4122
variants fail closed. No fallback to another environment variable is supported.

The helper now serializes each alias with a persistent private claim-lock file.
It publishes the versioned marker from a private same-directory temporary file
only after metadata is complete, so a concurrent claim cannot read an empty or
partial marker. New Claude markers encode `uuid-version=uuid-v4` or
`uuid-version=uuid-v5`; deployed untyped v4 markers remain readable while
untyped v5 markers fail closed. Existing malformed markers still fail closed.
