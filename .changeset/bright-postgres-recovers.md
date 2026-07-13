---
'@mastra/pg': minor
---

PostgreSQL workflow storage can now durably recover nested and recursive workflows after a crash while preserving their exact terminal result and final state.

```ts
const workflowsStorage = await storage.getStore('workflows')
if (workflowsStorage?.getWorkflowTerminalizationCapabilities().recoveryVersion !== 1) {
  throw new Error('PostgreSQL terminal recovery is unavailable')
}

const admission = await workflowsStorage.admitWorkflowNestedRun({
  workflowName: parentWorkflowName,
  runId: parentRunId,
  stepId,
  nestedWorkflowName: childWorkflowName,
  nestedRunId: childRunId,
  recoveryAncestry,
  result,
  requestContext,
})
```

Safe retries reuse the same stored recovery evidence without duplicating a child workflow or rewriting its parent. Versioned recovery tables can be initialized alongside an earlier draft schema without reusing incompatible stored rows.
