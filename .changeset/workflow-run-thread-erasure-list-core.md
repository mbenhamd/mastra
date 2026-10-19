---
'@mastra/core': minor
---

Added `listWorkflowRunsByThreadForErasure` to workflow storage. It lists every run of a resource that holds a copy of a thread's data, in any run status, so erasing a thread or a session doesn't leave runs behind. The `threadId` filter of `listWorkflowRuns` reads a single value: the first suspended step's thread, or else the durable loop's input thread. It misses a durable loop snapshot that is still `running`, a suspension kept in a foreach iteration or nested payload, and a run whose first suspended step names another thread.

```ts
if (workflows.getWorkflowSnapshotHandoffCapabilities().threadErasureListVersion === 1) {
  const page = await workflows.listWorkflowRunsByThreadForErasure({ resourceId, threadId, limit: 100 });
  // page.runs: [{ workflowName, runId, attribution: 'thread', rows: { snapshot, handoff, terminalSnapshot } }],
  // page.hasMore, page.nextCursor
}
```

- **Which rows.** The canonical snapshot, the handoff snapshot (kept after a `deleteRun` release) and the terminal recovery envelope. Each entry's `rows` says, for each, whether it is `absent` or records this `thread`, an `other` thread or none (`unattributed`). Don't delete a run with an `other` row when erasing one thread: a released handoff lets a newer run reuse the run ID.
- **Where it looks.** Only where the framework writes a thread id: agent loop suspensions (`__streamState`, including foreach and nested suspensions), a durable agent run's `messageListState` and `state.threadId` (input, step payloads and outputs, result, terminal envelope), an agent network run's input, and the request context of a started run (`MastraMemory.thread.id`, `mastra__threadId`). Application data that only looks like thread metadata doesn't match.
- **Unattributed runs.** The `pending` snapshot `createRun` stores before a run starts records no thread. With `includeUnattributed: true`, the listing also returns the resource's runs that record no thread id in any row, with their `status`, the store's `createdAt` and `updatedAt`, and `contentFree`. `contentFree` is true only for the shell `createRun` stores: no handoff or terminal row, a JSON object with only the shell's keys, empty `value`, `requestContext` and `lifecycleStepStates`, no `result` or `error`, and a `context` that is empty or holds only an empty `input`. The listing never erases anything.
- **Capability.** Adapters report `threadErasureListVersion: 1`. The in-memory store supports it, and the base class answers `unsupported`.
