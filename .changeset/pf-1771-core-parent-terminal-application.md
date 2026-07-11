---
'@mastra/core': minor
---

Workflow storage now exposes version 1 graph-bound atomic parent terminal application. A fenced runtime reads the effect-derived parent context, creates a PF-1781 continuation contract, atomically applies its pure parent patch with the canonical receipt and committed contract, and recovers the exact stored action after restart:

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

The API uses the fixed `mastra.parent-application.v1` consumer, rejects raw PubSub targets and redundant parent identifiers, returns explicit CAS and redacted contract conflicts without orphan evidence, and never executes workflow conditions or user callbacks inside storage. In-memory storage uses monotonic tombstone revisions so delete/recreate cannot revive an old planning context.
