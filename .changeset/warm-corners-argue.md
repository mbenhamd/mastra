---
'@mastra/core': minor
---

Users can now revise pending regular tool input with optional `editedArgs`. Invalid edits do not execute, and retries reuse the approved input.

Before:

```typescript
await session.respondToToolApproval({ approved: true })
```

After:

```typescript
await session.respondToToolApproval({ approved: true, editedArgs: { query: "reviewed query" } })
```

Durable and in-tool approvals reject edits. [PF-4112]
