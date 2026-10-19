---
'@mastra/core': minor
---

Added `listWorkflowRunsByThreadForErasure` to workflow storage. It lists every run of a resource whose snapshot records a thread id, in any run status, so erasing a thread or a session doesn't leave runs behind. The `threadId` filter of `listWorkflowRuns` reads a single value: the first suspended step's thread, or else the durable loop's input thread. It misses an agent loop snapshot that is still `pending` or `running`, and a run whose first suspended step names another thread.

```ts
if (workflows.getWorkflowSnapshotHandoffCapabilities().threadErasureListVersion === 1) {
  const page = await workflows.listWorkflowRunsByThreadForErasure({ resourceId, threadId, limit: 100 });
  // page.runs: [{ workflowName, runId, attribution: 'thread' }], page.hasMore, page.nextCursor
}
```

- **Where it looks.** Any `memoryInfo.threadId` at any depth (agent loop suspend payloads in any step, the durable loop's `messageListState`), the durable loop's `context.input.state.threadId`, and the request context of a started run (`MastraMemory.thread.id` and `mastra__threadId`).
- **Unattributed runs.** The `pending` snapshot `createRun` stores before a run starts records no thread. With `includeUnattributed: true`, the listing also returns the resource's snapshots that record no thread id at all, with their `status`, the store's `createdAt` and `updatedAt`, and `contentFree`. `contentFree` is true when `context` is empty, or holds only an empty `input`, and the request context is empty. The listing never erases anything.
- **Capability.** Adapters report `threadErasureListVersion: 1`. The in-memory store supports it, and the base class answers `unsupported`.
