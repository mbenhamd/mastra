---
'@mastra/pg': minor
---

Added `cancelWorkflowRunForErasure` to the PostgreSQL workflow store. It follows the same rules as the in-memory store (see `@mastra/core`), and the store now reports `closureCancelVersion: 1`.

```ts
const result = await workflows.cancelWorkflowRunForErasure({ workflowName, runId, expectedResourceId });
if (result.status === 'cancelled') {
  await workflows.deleteWorkflowRunClosure({ workflowName, runId, expectedResourceId });
}
```

The cancel runs in one transaction, so a terminal journal or a nested child that arrives at the same moment cannot slip in between its checks and its write. No schema change is needed.
