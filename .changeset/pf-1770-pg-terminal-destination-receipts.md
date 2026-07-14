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

Repeating the call for the same effect and consumer returns the same receipt. PostgreSQL atomically permits at most eight distinct consumers per effect and returns `consumer_limit_reached` for another distinct consumer, including under concurrent reservations. Reserved receipts survive normal workflow-run deletion until completed terminal records are cleaned up. A reserved receipt records retry identity only; it does not confirm that destination work was delivered or applied.
