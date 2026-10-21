---
'@mastra/core': minor
'@mastra/pg': minor
---

An erasure can now fence a deleted run id against a late, stale delete. `deleteWorkflowRunClosure` accepts `retainHandoffFence`. When the run's handoff is completed, unreleased, records the same resource and carries exactly that `mutationFence`, the delete erases the rest of the closure and keeps the handoff, with its snapshot emptied in the same atomic step. Stores that support it report `closureDeleteRetainFenceVersion: 1`.

**Before**

```ts
// Worker B erased the run and fenced its absence with a completed handoff.
await workflows.deleteWorkflowRunClosure({ workflowName, runId, expectedResourceId });
// Worker A's stale delete lands later: it deletes B's completed fence handoff,
// so a later write for this run id succeeds.
```

**After**

```ts
await workflows.deleteWorkflowRunClosure({
  workflowName,
  runId,
  expectedResourceId,
  retainHandoffFence: ABSENCE_FENCE,
});
// The completed handoff under ABSENCE_FENCE survives, holding no run content,
// and every later snapshot write for the run id is refused.
```

Handoffs under another fence, pending or released handoffs, and calls without the option behave as before.

A resumed, restarted or time-traveled workflow run no longer recreates its run when erasure deleted it while a step was executing, even in a fresh process and for a run that persists only suspensions. Only a run's first admission (`run.start()`) may create its row from a step update.
