---
'@mastra/core': patch
---

Harness conversations that end silently after tool results now make one bounded, response-only recovery attempt with tools disabled. Aborted in-flight tools produce matching error receipts, and thread subscribers give already-visible tools a bounded chance to emit their authoritative terminal errors before falling back to synthetic abort events.
