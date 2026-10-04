---
'@mastra/core': minor
---

Added `run.requestCancel()` to stop a workflow run from any process without canceling a newer execution of the same run.

**Why:** `run.cancel()` cancels whatever execution the run holds when it is called, so a late call can stop a run that was restarted or resumed in the meantime, and it cannot stop a run that another process is executing. `requestCancel()` names the execution it targets and is ignored by any later execution.

```ts
const outcome = await run.requestCancel({
  requestId: 'abort-1',
  expectedExecutionGeneration,
  expectedLifecycleResumeAttempt,
});
if (outcome.status === 'lineage_moved') {
  // The run restarted or resumed. Send the request again for the new execution.
}
```

- A running execution stops at its next step, in whichever process runs it. The process that sent the request also aborts its own execution at once.
- `restart()`, the startup recovery of active runs, and `resume()` mark a run with a pending request as canceled instead of running it again.
- A pending, suspended or paused run that no process is executing is canceled immediately.
- Works on the default workflow engine with in-memory, PostgreSQL and LibSQL storage. Other engines and storage adapters reject the call.
