---
'@mastra/core': patch
---

Fixed delayed workflow writes from a deleted run lifetime leaking stale step output into a reopened run. A stale write is now rejected before it can advance the workflow or publish step events.
