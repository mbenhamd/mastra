---
'@mastra/core': minor
---

Added a built-in `subagent` tool to Harness v1 so the parent model can manage the subagents it started with `spawn_subagent` or `task_delegate`.

**Actions**

- `status`: list your subagents, or inspect one, with its status. A settled `task_delegate` subagent includes its result.
- `wait`: wait up to a bounded timeout for one or more subagents to settle. Returns the settled results and the ones still running.
- `send`: add an instruction to a running subagent's current run. A subagent that is idle or waiting for human input is never woken, and a message the run ends without reading is dropped.
- `cancel`: stop one subagent. The cancel is stored on the subagent's own record, so it survives restarts. It needs the subagent to be free or held by this process; otherwise it returns a retryable `harness.subagent_cancel_unavailable` error and changes nothing. The result says whether the cancel was `acknowledged` and whether the subagent has `settled`.

A session can act only on its own subagents. The tool is available whenever `subagents` types are configured and the session is below `subagents.maxDepth`:

```ts
const harness = new Harness({
  // ...
  subagents: { maxDepth: 2, types: { worker: { agentId: 'worker', description: 'Background worker' } } },
});
// The lead agent can now call, for example:
// { "action": "wait", "subagentSessionId": "<id from task_delegate>", "timeoutMs": 60000 }
```

It goes through the normal permission policy, so a host can deny it by name with `permissions: { categories: {}, tools: { subagent: 'deny' } }`.
