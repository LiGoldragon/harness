# persona-harness skill

Work here when the change concerns harness identity, lifecycle, transcript
events, adapter capabilities, or harness actor surfaces.

Rules for work here:

- Keep routing policy in `persona-router`.
- Keep OS/window-manager observations in `persona-system`.
- Keep durable PTY and WezTerm viewer transport in `persona-wezterm`.
- Model harness capabilities as typed values, not strings.
- Keep live lifecycle and transcript state inside `Harness`; do not add
  alternate runtime wrappers or public handle wrappers.
- Preserve the durable-harness invariant: closing a viewer must not kill the
  harness.
