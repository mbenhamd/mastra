---
'@mastra/core': patch
---

Preserved tracing context when API error processors run so their spans remain attached to the active agent trace.
