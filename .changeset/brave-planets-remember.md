---
'@mastra/core': minor
---

Added revision-aware Working Memory APIs so authorized applications can correct or forget saved facts without later observer runs restoring protected values. Applications can also detect whether storage supports safe inactive-thread filtering and failed-clone rollback.

**Before**

```ts
const memoryStore = await storage.getStore('memory');
await memoryStore?.updateResource({
  resourceId: 'user-1',
  workingMemory: JSON.stringify({ profile: { name: 'Ada' } }),
});
```

**After**

```ts
const memoryStore = await storage.getStore('memory');
if (!memoryStore?.supportsRevisionedWorkingMemory) {
  throw new Error('Revisioned Working Memory is unavailable');
}

const snapshot = await memoryStore.getWorkingMemorySnapshot({
  scope: 'resource',
  resourceId: 'user-1',
});

await memoryStore.applyWorkingMemoryUpdate({
  scope: 'resource',
  resourceId: 'user-1',
  value: JSON.stringify({ profile: { name: 'Ada Lovelace' } }),
  expectedRevision: snapshot.revision,
  source: 'owner',
  protectPaths: ['/profile/name'],
});
```
