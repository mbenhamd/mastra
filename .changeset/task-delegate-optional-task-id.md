---
'@mastra/core': patch
---

`task_delegate` can now delegate new work without a `taskId`. It adds a top-level plan task titled with the first line of `task` and delegates it.

```ts
await tools.task_delegate.execute({
  agentType: 'researcher',
  task: 'Screen the abstracts\nInclude randomized trials only.',
});
```

- A call that cannot start is refused before a task is added: no `task`, a blank `task`, a `task` over the byte limit, an unknown agent type, or the depth limit.
- A retry of the same tool call with the same `task` returns the original result.
- A retry with a changed `task` adds and delegates a new task.
- At the concurrency limit, the added task stays pending and the error names its id.
- A Stop that completes while the task is being added refuses the delegation.
