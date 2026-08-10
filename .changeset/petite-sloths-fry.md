---
'@mastra/core': major
---

Require both `runId` and `toolCallId` for AgentController approval events, pending display state, and responses so approvals cannot cross run boundaries.

**Before**

```ts
await session.respondToToolApproval({ toolCallId, decision: 'approve' });
```

**After**

```ts
await session.respondToToolApproval({ runId, toolCallId, decision: 'approve' });
```
