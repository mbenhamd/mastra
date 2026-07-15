---
'mastracode': patch
---

Fixed system notifications and login browser opening so untrusted text and URLs cannot be interpreted by command shells or terminal control sequences. Provider login instructions, prompts, placeholders, and progress messages are sanitized at the TUI boundary, while launcher failures remain best-effort.
