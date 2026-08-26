---
'@mastra/pg': minor
---

Added domain-scoped initialization to `PostgresStore`. Pass a non-empty list of unique storage domain names through `enabledDomains` to construct and initialize only those domains. Omitting the option preserves initialization of every domain.

Because `PostgresStoreVNext` always owns its required observability domain, its domain selection must include `observability` when provided.

A `PostgresStoreVNext` that enables only `observability` no longer needs a reachable primary database during `init()`.

```typescript
const storage = new PostgresStore({
  id: 'memory-storage',
  connectionString: process.env.DATABASE_URL,
  enabledDomains: ['memory'],
});
```
