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

Existing custom clients remain source-compatible without `eval` for ordinary cache operations. Atomic indexed-log calls require either the client's `eval` method or an `evalScript` adapter and fail with an explicit capability error when neither is available.
