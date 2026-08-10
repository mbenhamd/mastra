---
'@mastra/server': major
---

Require both `runId` and `toolCallId` in AgentController tool approval requests so approval decisions bind to one exact pending run.

**Before**

```ts
await fetch(toolApprovalUrl, {
  method: 'POST',
  body: JSON.stringify({ toolCallId, approved: true }),
});
```

**After**

```ts
await fetch(toolApprovalUrl, {
  method: 'POST',
  body: JSON.stringify({ runId, toolCallId, approved: true }),
});
```
