---
'@mastra/core': patch
'@mastra/pg': patch
---

Developers building durable workflow runtimes can now coordinate one terminal outcome across workers without storing claim state inside replaceable workflow snapshots. In-memory and PostgreSQL storage support the new journal; other adapters report `unsupported` explicitly.

```ts
const workflows = await storage.getStore('workflows')

if (!workflows?.supportsWorkflowTerminalizationJournal()) {
  throw new Error('Durable workflow terminalization requires a supported store')
}

const claim = await workflows.claimWorkflowTerminalization({
  workflowName: 'research',
  runId: 'run-123',
  eventKey: 'event-456',
  terminalStatus: 'failed',
  ownerId: 'worker-a',
  leaseMs: 30_000,
})
```

Successful claims return a token and generation that fence stale workers during renewal, phase advancement, and release. This makes terminalization state durable; external effects still require a retained source event or durable outbox plus stable idempotency keys.
