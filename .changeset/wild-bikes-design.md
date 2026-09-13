---
'@mastra/server': minor
---

Added `editedArgs` to Harness inbox tool-approval responses.

Before:

```typescript
{ kind: "tool-approval", approved: true, ...generation }
```

After:

```typescript
{ kind: "tool-approval", approved: true, editedArgs: { query: "reviewed query" }, ...generation }
```

Denied approvals reject edits. [PF-4112]
