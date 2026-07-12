---
'@mastra/pg': minor
---

Added PostgreSQL persistence for authenticated workflow recovery envelopes, recursive ancestry, and atomic nested-run admission.

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

PostgreSQL now validates parent graph bindings under the parent lock, serializes cleanup against ancestry admission, stores exact final state in both persisted state views, and returns matching ownership-and-ancestry replays without rewriting or advancing the parent revision.

Recovery effects, envelopes, and dependent continuation plans use versioned evidence tables, so initializing this release over a database created by the earlier producer-outbox contract does not reuse incompatible row shapes or foreign keys.
