---
'@mastra/pg': minor
---

Added PostgreSQL support for revision-aware owner corrections and filtering inactive threads before pagination. Observational-memory edits and failed thread clones now preserve unrelated or concurrently changed data, and clones capture source ownership in the same transaction as their thread and message snapshot.

**Before**

```ts
const memoryStore = await storage.getStore('memory');
const firstPage = await memoryStore?.listThreads({ perPage: 100, page: 0 });
```

**After**

```ts
const memoryStore = await storage.getStore('memory');
if (!memoryStore?.supportsThreadUpdatedBeforeFilter) {
  throw new Error('Inactive-thread filtering is unavailable');
}

const firstInactivePage = await memoryStore.listThreads({
  perPage: 100,
  page: 0,
  filter: { updatedBefore: new Date('2026-01-01T00:00:00.000Z') },
});
```

`@mastra/pg` imports these governance contracts from `@mastra/core`, so its peer range starts at `1.58.0-alpha.11`, the first coordinated Core prerelease containing them. Any release plan must version Core and PG atomically; do not publish PG independently.
