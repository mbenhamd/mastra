---
'@mastra/core': patch
---

`task_delegate` no longer requires `taskId`. When the model omits it, the call adds a top-level plan task titled with the first line of `task` and delegates it. A retried call reuses that task instead of adding a second one. An empty `task` without a `taskId` is refused, and if the delegation fails after the task was added, the error names the task id so the model can retry with it.
