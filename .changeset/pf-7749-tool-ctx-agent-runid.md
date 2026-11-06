---
'@mastra/core': patch
---

Fixed `ctx.agent.runId` being empty in tools run by an agent. Tools now receive the run id there. Before, only the top-level `ctx.runId` had it.

```ts
const tool = createTool({
  id: 'note-run',
  description: 'Records which run called it',
  inputSchema: z.object({}),
  execute: async (_input, ctx) => ({ runId: ctx.agent?.runId }),
});
```

Inside a Harness, `ask_user`, `submit_plan` and `task_delegate` now use the same run id that the Harness reports on `agent_start`.
