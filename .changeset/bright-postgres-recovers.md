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
  expectedChildGraphFingerprint: createWorkflowTerminalGraphFingerprint(childSerializedStepGraph),
  recoveryAncestry,
  result,
  requestContext,
  initialChildSnapshot: {
    snapshot: childInitialSnapshot,
    resourceId: childResourceId,
  },
})
```

Safe retries keep the original child run and recovery state. A missing initial snapshot is recreated, while running or suspended child progress is never overwritten. A child remains terminal even after its replaceable workflow row is deleted, and conflicting retained child state stops replay without changing parent ownership or recovery ancestry. Nested start delivery remains at-least-once, so consumers must stay idempotent.
