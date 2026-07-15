---
'@mastra/convex': minor
---

Added durable atomic indexed-log cache support for restart-safe PubSub replay with Convex.

```ts
const cache = new ConvexServerCache({ client: convexCacheClient });
const pubsub = new CachingPubSub(innerPubsub, cache, {
  indexedReplay: { retentionMs: 60_000, maxEvents: 1_000 },
});
```
