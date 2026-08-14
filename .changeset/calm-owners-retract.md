---
'@mastra/core': patch
'@mastra/pg': patch
---

Fixed message update and deletion retractions so thread-scoped Observational Memory and its managed Working Memory are cleared under the persisted owner resource instead of a stale message resource.
