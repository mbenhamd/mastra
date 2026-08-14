---
'@mastra/memory': minor
---

Improved Working Memory updates so owner corrections win over stale observations, protected JSON fields remain unchanged during later observation, and configured byte limits are enforced before persistence.

**Before**

```ts
await memory.updateWorkingMemory({
  threadId: 'thread-1',
  resourceId: 'user-1',
  workingMemory: JSON.stringify({ profile: { name: 'Ada' } }),
});
```

**After**

```ts
const memory = new Memory({
  storage,
  options: {
    workingMemory: {
      enabled: true,
      scope: 'resource',
      maxDataBytes: 8_192,
    },
  },
});

// Authorize the caller for user-1 before applying an owner correction.
const snapshot = await memory.getWorkingMemorySnapshot({ resourceId: 'user-1' });
await memory.updateWorkingMemoryByOwner({
  resourceId: 'user-1',
  workingMemory: JSON.stringify({ profile: { name: 'Ada Lovelace' } }),
  expectedRevision: snapshot.revision,
  protectPaths: ['/profile/name'],
});
```
