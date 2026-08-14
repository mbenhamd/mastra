---
'@mastra/core': minor
---

Added revision-aware Working Memory APIs so authorized applications can correct or forget saved facts without later observer runs restoring protected values. Provenance remains exact for up to 256 protected RFC 6901 paths of at most 1,024 characters, while high-cardinality changes collapse to a coarse root marker and total provenance is capped at 257 entries with 64-character timestamps. `maxDataBytes` continues to bound stored value bytes independently from these fixed control-metadata limits, and stored controls above the fixed limits now fail closed. Applications can also detect whether storage supports safe inactive-thread filtering, failed-clone rollback, and atomic clone source-ownership snapshots.

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
