---
'@mastra/core': minor
'@mastra/pg': minor
---

Added a `retainHandoffFence` option to `deleteWorkflowRunClosure`, so a run you erased stays blocked even when an older delete arrives late. Pass the same marker you used when you blocked the run. That block survives the delete, keeps no run content, and still refuses every later write for that run id. The result includes `retainedFence: true` when this happens. Stores that support the option report `closureDeleteRetainFenceVersion: 1`.

```ts
await workflows.deleteWorkflowRunClosure({
  workflowName,
  runId,
  expectedResourceId,
  retainHandoffFence: ABSENCE_FENCE,
});
// → { status: 'deleted', removed: { ... }, retainedFence: true }
//   or { status: 'missing', retainedFence: true } when only the block itself is left
```

Deletes without the option, or with a marker other than the one that blocked the run, behave as before.
