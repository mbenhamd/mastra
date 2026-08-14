---
'@mastra/pg': minor
---

Added PostgreSQL support for revision-aware owner corrections and filtering inactive threads before pagination. Observational-memory edits and failed thread clones now preserve unrelated or concurrently changed data.

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
