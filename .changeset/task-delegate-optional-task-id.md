---
'@mastra/core': patch
---

`task_delegate` no longer requires `taskId`. When the model omits it, the call adds a top-level plan task titled with the first line of `task` and delegates it.

- A call that cannot start (closed session, unknown agent type, depth limit) is refused before any task is added.
- An empty `task` without a `taskId` is refused.
- A retry of the same tool call resolves to the task that call added. If that call already delegated it, the retry returns the same result instead of an error.
- At the concurrency limit the added task stays pending, and the error names its id so the model can retry with `taskId` once a subagent settles.
