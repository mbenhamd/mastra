---
'@mastra/core': minor
---

Added a built-in `subagent` tool to Harness v1 so the parent model can manage the subagents it started with `spawn_subagent` or `task_delegate`.

**Actions**

- `status`: list your subagents, or inspect one, with its status and (once settled) its result.
- `wait`: wait up to a bounded timeout for one or more subagents to settle. Returns the settled results and the ones still running.
- `send`: add an instruction to a running subagent's current run.
- `cancel`: stop one subagent. The result says whether the request was `acknowledged` (cancelled in this process) or `intent_recorded` (another process holds the subagent; the cancel is stored on the subagent's record and applied there or after a restart), and whether the subagent has `settled`.

A session can act only on its own subagents. The tool is present only when `subagents` types are configured and the session is below `subagents.maxDepth`. It goes through the normal permission policy, so a host can deny it by name:

```ts
const harness = new Harness({
  // ...
  subagents: { maxDepth: 2, types: { worker: { agentId: 'worker', description: 'Background worker' } } },
  modes: [{ id: 'default', agentId: 'lead', permissions: { categories: {}, tools: { subagent: 'deny' } } }],
});
```
