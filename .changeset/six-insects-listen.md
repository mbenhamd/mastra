---
'@mastra/client-js': minor
---

Added typed `editedArgs` input to Harness tool-approval helpers and generated route types.

Before:

```typescript
await session.respondToToolApproval({ approved: true, ...generation })
```

After:

```typescript
await session.respondToToolApproval({ approved: true, editedArgs: { query: "reviewed query" }, ...generation })
```

[PF-4112]
