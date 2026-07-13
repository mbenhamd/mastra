---
'@mastra/core': minor
---

Added enforceable per-execution toolset replacement and explicit Harness built-in control.

Set `toolsetsMode: 'replace'` on an agent execution to expose only the supplied toolsets. Replacement toolsets remain restricted across processors, durable execution, and same-instance Harness suspend/resume. Resumes fail closed when the original replacement tools are unavailable.

Harness modes that use `tools` now apply replacement semantics as documented. Set `harnessBuiltins: 'exclude'` to remove Harness-owned plan-task and subagent built-ins, including when configuring an explicitly tool-free mode with `tools: {}`.

```ts
await agent.generate('Run the approved operation', {
  toolsets: { approved: { approvedTool } },
  toolsetsMode: 'replace',
})
```
