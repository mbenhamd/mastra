---
'@mastra/pg': minor
---

Added `listWorkflowRunsByThreadForErasure` to the PostgreSQL workflow store. It follows the same rules as the in-memory store (see `@mastra/core`), and the store now reports `threadErasureListVersion: 1`.

```ts
const page = await workflows.listWorkflowRunsByThreadForErasure({ resourceId, threadId, limit: 100 });
```

The query scans the resource's snapshots in the order of the existing `("resourceId", workflow_name, run_id)` index. It matches each snapshot with one `jsonb_path_exists` call that binds the thread id as a jsonpath variable. Legacy `json` and `text` snapshot columns are read through the same escape sanitizing as the status filter. The `threadId` filter of `listWorkflowRuns` and its expression index are unchanged. No schema change is needed.
