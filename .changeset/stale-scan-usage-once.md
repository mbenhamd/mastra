---
'@mastra/core': patch
---

A harness session that recovers an interrupted run from a stale scan no longer counts that run's token usage a second time when another owner has already settled the run. For a settled admission, the run's current result evidence now decides whether its staged usage is counted.
