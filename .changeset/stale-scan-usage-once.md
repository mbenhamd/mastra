---
'@mastra/core': patch
---

Hardened harness session recovery: a process that recovers an interrupted run from an out-of-date scan no longer counts that run's token usage a second time when another owner has already settled the run. Storage already refused to save the doubled count, so saved totals were not affected.
