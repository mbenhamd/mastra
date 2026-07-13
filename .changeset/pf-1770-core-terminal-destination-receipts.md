---
'@mastra/core': minor
---

Added terminal destination receipt reservation for durable workflow storage. Check support before using the API:

```ts
const capabilities = workflowsStorage.getWorkflowTerminalizationCapabilities();

if (capabilities.destinationReceiptVersion !== 1) {
  throw new Error('Destination receipts are not supported by this workflow store');
}

const reservation = await workflowsStorage.reserveWorkflowTerminalDestinationReceipt({
  workflowName,
  runId,
  ownerId,
  claimToken,
  claimGeneration,
  effectKind: 'workflow-finish',
  consumerId: 'finish-dispatcher',
});
```

Repeating the call for the same effect and consumer returns the same receipt. Version 1 permits at most eight distinct consumers per effect and returns `consumer_limit_reached` for another distinct consumer, while idempotent retries remain available at the limit. A reserved receipt records retry identity only; it does not confirm destination delivery or application, schedule follow-up work, or acknowledge transport.
