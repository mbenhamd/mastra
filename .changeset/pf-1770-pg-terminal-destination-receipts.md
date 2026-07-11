---
'@mastra/pg': minor
---

Added persistent terminal destination receipt reservation to PostgreSQL workflow storage:

```ts
const reservation = await workflowsStorage.reserveWorkflowTerminalDestinationReceipt({
  workflowName,
  runId,
  ownerId,
  claimToken,
  claimGeneration,
  effectKind: 'parent-workflow-step-end',
  consumerId: 'parent-application',
});
```

Repeating the call for the same effect and consumer returns the same receipt. A different consumer receives its own receipt. Reserved receipts survive normal workflow-run deletion until completed terminal records are cleaned up. Version 1 does not mark destination work as applied.
