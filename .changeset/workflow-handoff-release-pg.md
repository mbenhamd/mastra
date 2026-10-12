---
'@mastra/pg': patch
---

Added `releaseWorkflowSnapshotHandoff` to the PostgreSQL workflow store, so a run whose snapshot handoff is finished can be deleted again. Release locks rows in the same order as handoff claim, transition, completion and run deletion, so a release that races one of them either sees its result or waits for it. A pending handoff is never released without its fence. The store now reports `releaseVersion: 1`.
