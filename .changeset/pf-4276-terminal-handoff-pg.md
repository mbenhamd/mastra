---
'@mastra/pg': patch
---

Added native Harness terminal handoff support to `PostgresStore`. Terminal chat results are committed durably, delivered through a claimable intent queue, and fenced when a session is deleted or recreated. Bounded seed, payload, and claim-lease limits are configurable on the store.

```ts
const storage = new PostgresStore({
  connectionString: process.env.DATABASE_URL,
  terminalHandoff: { enabled: true, maxPendingIntents: 1000, claimLeaseMs: 30_000 },
});
```
