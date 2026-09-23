---
'@mastra/pg': patch
---

Fixed delayed workflow result writes from a deleted run lifetime merging stale step output into a reopened lifetime's snapshot. `updateWorkflowResults` now compares the caller's `executionGeneration` against the stored snapshot's lineage inside the row lock and skips the merge on mismatch, resolving to the `STALE_EXECUTION_RESULT` rejection so the evented processor stops the stale handler instead of advancing with an inline result.
