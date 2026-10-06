---
'@mastra/core': minor
---

Added a `noActiveExecution` option to `Run.requestCancel()`. When the caller knows no process is executing the named execution, for example because it holds the run's execution lease after the owner's lease expired, a `running` or `waiting` run is canceled immediately. The write uses the same compare-and-set on that execution, so a restart that has replaced it is never canceled.

```typescript
const outcome = await run.requestCancel({
  requestId: 'abort-1',
  expectedExecutionGeneration: state.executionGeneration,
  expectedLifecycleResumeAttempt: state.lifecycleResumeAttempt ?? 0,
  noActiveExecution: true,
});
```
