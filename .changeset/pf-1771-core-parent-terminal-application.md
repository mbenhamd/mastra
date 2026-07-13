---
'@mastra/core': minor
---

Workflow storage can now recover a nested workflow's terminal outcome and apply it to the parent exactly once, including after a process restart:

```ts
const parent = await workflowsStorage.getWorkflowTerminalParentContext({
  workflowName,
  runId,
  ownerId,
  claimToken,
  claimGeneration,
});

if (parent.status !== 'found') throw new Error('Parent run is unavailable');

const applied = await workflowsStorage.applyWorkflowTerminalParentEffect({
  workflowName,
  runId,
  ownerId,
  claimToken,
  claimGeneration,
  contract,
});
```

Retries return the same stored result without applying the outcome twice. Conflicting or stale requests return clear results without leaving partial state. Storage does not run workflow conditions or user callbacks while applying the outcome.
