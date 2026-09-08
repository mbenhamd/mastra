---
'@mastra/core': minor
---

Added optional `editedArgs` patches to regular tool-gate approvals, with input validation and replay-safe approved arguments.

Before:

```typescript
await session.respondToToolApproval({ approved: true })
```

After:

```typescript
await session.respondToToolApproval({ approved: true, editedArgs: { query: "reviewed query" } })
```

Durable and in-tool approvals reject edits. [PF-4112]
