# Upgrades

## Flow identity helper

Install the Home generation that pins the `harness` revision carrying
`flow-id`. Parent flows claim their lane before writing their first artifact:
`flow-id codex --flows-root /absolute/flows-root`. The returned alias becomes
`FLOW_ID`, and its lane becomes `FLOW_DIRECTORY`. Existing unmarked lanes are
collisions; the helper extends its candidate and never overwrites them.
