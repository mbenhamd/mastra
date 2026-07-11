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

Repeating the call for the same effect and consumer returns the same receipt. A different consumer receives its own receipt. Version 1 reserves receipt identity only; it does not apply destination work, schedule follow-up work, or acknowledge transport.
