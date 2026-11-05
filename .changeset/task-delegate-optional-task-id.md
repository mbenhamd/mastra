---
'@mastra/core': patch
---

`task_delegate` no longer requires `taskId`. When the model omits it, the call adds a top-level plan task titled with the first line of `task` and delegates it.

- A call that cannot start (closed session, unknown agent type, depth limit) is refused before any task is added.
- An empty `task` without a `taskId` is refused.
- A retry of the same tool call with the same full `task` resolves to the task that call added. If that call already delegated it, the retry returns the same result instead of an error. Overlapping executions of the same call return the same result.
- A retry of the same tool call with a changed `task` is new work: it adds and delegates a new task instead of returning the earlier child.
- At the concurrency limit the added task stays pending, and the error names its id. The model is told to retry with that `taskId` and the same `task`, so the subagent receives the full text rather than only the title.
