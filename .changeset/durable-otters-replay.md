---
'@mastra/core': minor
---

Added `durableLocalOnly: 'passthrough'` so local-only events can publish
without retained history when durable exact replay is enabled.

```typescript
new CachingPubSub(inner, durableCache, {
  indexedReplay: { retentionMs: 60_000, maxEvents: 100 },
  durableLocalOnly: 'passthrough',
});
```
