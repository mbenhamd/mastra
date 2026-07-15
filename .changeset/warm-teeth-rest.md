---
'@mastra/redis': minor
---

Added durable atomic indexed-log cache support for restart-safe PubSub replay with Redis.

```ts
const cache = new RedisServerCache({ client: redis });
const pubsub = new CachingPubSub(innerPubsub, cache, {
  indexedReplay: { retentionMs: 60_000, maxEvents: 1_000 },
});
```
