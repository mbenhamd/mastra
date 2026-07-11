---
'@mastra/pg': patch
---

PostgreSQL storage can now preserve terminal workflow delivery evidence after normal workflow-run deletion, so an interrupted parent-workflow or finish delivery can be resumed after restart.

Check for the exact producer capability before enabling durable terminal delivery:

```ts
const workflows = await storage.getStore('workflows')
const capabilities = workflows?.getWorkflowTerminalizationCapabilities()

if (capabilities?.journalVersion !== 1 || capabilities.producerOutboxVersion !== 1) {
  throw new Error('PostgreSQL storage does not support durable terminal delivery')
}
```
