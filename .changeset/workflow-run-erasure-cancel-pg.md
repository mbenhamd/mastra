---
'@mastra/pg': minor
---

Added `cancelWorkflowRunForErasure` to the PostgreSQL workflow store, with the same rules as the in-memory store (see `@mastra/core`). The store now reports `closureCancelVersion: 1`.

- **One transaction.** The cancel takes the same locks in the same order as `deleteWorkflowRunClosure`: the run's terminal advisory lock, the parent revision, the handoff row, the snapshot and the journal row. A journal claim or a nested child's recovery ancestry that arrives meanwhile waits for it. Once the cancel commits, a new nested child finds its parent terminal and is refused.
- **The write.** It is the snapshot write `persistWorkflowSnapshot` makes, keeping the row's resource, followed by the same parent revision bump, which latches `canceled`.
- **No schema change.**
