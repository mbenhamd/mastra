---
'@mastra/client-js': major
---

Require both `runId` and `toolCallId` when responding to AgentController tool approvals so a stale decision cannot resolve a different run.

**Before**

```ts
await session.approveTool(toolCallId, true);
```

**After**

```ts
await session.approveTool(runId, toolCallId, true);
```
