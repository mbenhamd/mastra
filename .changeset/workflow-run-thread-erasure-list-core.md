---
'@mastra/core': minor
---

Added `listWorkflowRunsByThreadForErasure` to workflow storage. It lists every run of a resource that holds a copy of a thread's data, in any run status, so erasing a thread or a session doesn't leave runs behind. The `threadId` filter of `listWorkflowRuns` reads a single value: the first suspended step's thread, or else the durable loop's input thread. It misses an agent loop snapshot that is still `pending` or `running`, and a run whose first suspended step names another thread.

```ts
if (workflows.getWorkflowSnapshotHandoffCapabilities().threadErasureListVersion === 1) {
  const page = await workflows.listWorkflowRunsByThreadForErasure({ resourceId, threadId, limit: 100 });
  // page.runs: [{ workflowName, runId, attribution: 'thread' }], page.hasMore, page.nextCursor
}
```

- **Which rows.** The canonical snapshot, the handoff snapshot (kept after a `deleteRun` release) and the terminal recovery envelope. Each entry's `rows` says, for each, whether it is `absent` or records this `thread`, an `other` thread or none (`unattributed`). Don't delete a run with an `other` row when erasing one thread: a released handoff lets a newer run reuse the run ID.
- **Where it looks.** Only where the framework writes a thread id: an agent loop suspension's `__streamState`, a durable agent run's `messageListState` and `state.threadId` (input, step outputs, result), and the request context of a started run (`MastraMemory.thread.id`, `mastra__threadId`). Application data that only looks like thread metadata doesn't match.
- **Unattributed runs.** The `pending` snapshot `createRun` stores before a run starts records no thread. With `includeUnattributed: true`, the listing also returns the resource's runs that record no thread id in any row, with their `status`, the store's `createdAt` and `updatedAt`, and `contentFree`. `contentFree` is true when the run has no handoff or terminal row, its `context` is empty or holds only an empty `input`, and its request context is empty. The listing never erases anything.
- **Capability.** Adapters report `threadErasureListVersion: 1`. The in-memory store supports it, and the base class answers `unsupported`.
