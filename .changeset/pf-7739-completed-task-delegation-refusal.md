---
'@mastra/core': patch
---

`task_delegate` now refuses a task whose delegation already completed with `harness.delegation_already_completed`, naming the completed child session and its outcome. The model can read that child's result instead of starting a new subagent on finished work.

The refusal applies to every call, including a retried call after another run completed the task. A task whose earlier delegation failed, was blocked, or was cancelled can still be delegated again, and so can a completed task that the plan has reopened (for example with `task_update`). `Session` callers get the exported `HarnessDelegationAlreadyCompletedError`. The tool returns:

```ts
{
  isError: true,
  errorName: 'HarnessDelegationAlreadyCompletedError',
  code: 'harness.delegation_already_completed',
  taskId: 'task-1',
  subagentSessionId: 'sess-123',
  outcome: 'completed',
  message:
    'task "task-1" was already delegated to subagent session "sess-123", which settled with outcome "completed"; ' +
    "read that delegation's result with plan_task_check instead of delegating the task again",
}
```
