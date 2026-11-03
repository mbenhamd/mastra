---
'@mastra/core': minor
---

Added `subagents.onDelegatedSettlement` to Harness v1 so a parent session hears when a detached `task_delegate` child finishes, instead of having to poll `plan_task_check`.

- `'none'` (default): unchanged behaviour.
- `'remind'`: the parent's running turn receives a system reminder; an idle parent keeps it for its next turn.
- `'wake'`: an idle parent also starts a follow-up turn.

Each settlement is announced at most once, survives a process restart, and is not sent when the parent's own Stop or cancel ended the child.

```ts
const harness = new Harness({
  // ...
  subagents: {
    onDelegatedSettlement: 'wake',
    types: { researcher: { agentId: 'researcher', description: 'Background research' } },
  },
});
```
