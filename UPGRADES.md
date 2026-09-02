# Upgrades

## Flow identity helper

Install the Home generation that pins the `harness` revision carrying
`flow-id`. Parent flows claim their lane before writing their first artifact:
`flow-id codex --flows-root /absolute/flows-root` or `flow-id claude
--flows-root /absolute/flows-root --parent-session UUID`. Codex returns six
normalized hexadecimal UUID characters from `[23:29]`; Claude accepts only a
canonical lowercase RFC 4122 UUIDv4 parent session and returns its first six
literal hexadecimal characters. Both extend only for collisions; the result
becomes `FLOW_ID`, and its lane becomes `FLOW_DIRECTORY`. Existing unmarked
lanes are collisions and are never overwritten.

Claude callers that supplied a non-v4 or noncanonical parent UUID must pass the
authoritative canonical UUIDv4 parent session instead. No fallback to another
environment variable is supported.

The helper now serializes each alias with a persistent private claim-lock file.
It publishes the versioned marker from a private same-directory temporary file
only after metadata is complete, so a concurrent claim cannot read an empty or
partial marker. Existing malformed markers still fail closed.
