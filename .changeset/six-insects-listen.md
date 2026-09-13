---
'@mastra/client-js': minor
---

Harness clients can now revise pending tool input with typed `editedArgs`. Invalid edits do not execute.

Before:

```typescript
await session.respondToToolApproval({ approved: true, ...generation })
```

After:

```typescript
await session.respondToToolApproval({ approved: true, editedArgs: { query: "reviewed query" }, ...generation })
```

[PF-4112]
