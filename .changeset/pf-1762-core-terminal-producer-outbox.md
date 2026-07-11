---
'@mastra/core': patch
---

Terminal workflow delivery intents can now be prepared once and recovered after a worker failure or restart. Check the exact capability before using the durable producer APIs:

```ts
const workflows = await storage.getStore('workflows')
const capabilities = workflows?.getWorkflowTerminalizationCapabilities()

if (capabilities?.journalVersion !== 1 || capabilities.producerOutboxVersion !== 1) {
  throw new Error('The workflow store does not support durable terminal producer intents')
}
```

Supported stores preserve the terminal result needed for retry and return the same parent-workflow or finish delivery intent across repeated preparation attempts. Normal workflow-run deletion does not discard incomplete delivery evidence.
