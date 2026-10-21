---
'@mastra/core': minor
'@mastra/pg': minor
---

Added a `retainHandoffFence` option to `deleteWorkflowRunClosure`. Use it to keep a deleted run's id blocked even if a late, stale delete arrives after you blocked it. The run's completed handoff is kept when it was recorded for the same resource under exactly that `mutationFence`. Its snapshot is emptied in the same step, so no run content survives, and every later write for that run id is refused. The delete reports this with `retainedFence: true`. Stores that support the option advertise `closureDeleteRetainFenceVersion: 1`.

```ts
await workflows.deleteWorkflowRunClosure({
  workflowName,
  runId,
  expectedResourceId,
  retainHandoffFence: ABSENCE_FENCE,
});
// → { status: 'deleted', removed: { ... }, retainedFence: true }
//   or { status: 'missing', retainedFence: true } once only the empty handoff remains
```

Calls without the option, and handoffs under another fence or still pending, behave as before.
