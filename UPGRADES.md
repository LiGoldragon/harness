# Upgrades

## Flow identity helper

Install the Home generation that pins the `harness` revision carrying
`flow-id`. Parent flows claim their lane before writing their first artifact:
`flow-id codex --flows-root /absolute/flows-root`. The returned alias is six
literal normalized hexadecimal UUID characters from `[23:29]`, extended only
for collisions; it becomes `FLOW_ID`, and its lane becomes `FLOW_DIRECTORY`.
Existing unmarked lanes are collisions and are never overwritten.

The helper now serializes each alias with a persistent private claim-lock file.
It publishes the versioned marker from a private same-directory temporary file
only after metadata is complete, so a concurrent claim cannot read an empty or
partial marker. Existing malformed markers still fail closed.
