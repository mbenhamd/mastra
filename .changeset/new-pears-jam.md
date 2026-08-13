---
'@mastra/core': patch
---

Harness conversations that end silently after tool results now make one bounded, response-only recovery attempt with tools disabled. Aborted in-flight tools also produce matching error receipts alongside their synthetic error terminal events.
