# Upgrades

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
