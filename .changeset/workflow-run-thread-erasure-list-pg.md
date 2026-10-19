---
'@mastra/pg': minor
---

Added `listWorkflowRunsByThreadForErasure` to the PostgreSQL workflow store. It follows the same rules as the in-memory store (see `@mastra/core`), and the store now reports `threadErasureListVersion: 1`.

```ts
const page = await workflows.listWorkflowRunsByThreadForErasure({ resourceId, threadId, limit: 100 });
```

It also works with older tables whose snapshot column is `json` or `text`. The `threadId` filter of `listWorkflowRuns` is unchanged. No schema change is needed.
