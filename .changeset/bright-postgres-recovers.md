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

Safe retries reuse the same durable child run identity and retained recovery evidence without rewriting the parent. Admission initializes the child snapshot with create-if-absent semantics in the same transaction, so replay repairs a missing initial row but never overwrites running or suspended progress. A nested-step start can still be redispatched for that same identity after broker redelivery until the linked durable child-start outbox follow-up is complete. Recovery uses the canonical v2 tables; incompatible draft table names are not read or migrated.
Terminal child evidence returns `child_terminal` without recreating a missing canonical row. The terminal-status lookup continues to expose the durable marker after that canonical row is deleted, allowing nested replay to fail closed before redispatch. A malformed, mismatched, or graph-drifted retained child returns `child_snapshot_conflict`; neither outcome mutates parent ownership or recovery ancestry.
