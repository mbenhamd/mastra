---
'@mastra/core': patch
---

Thread-stream run ids and lease owners now come from `globalThis.crypto.randomUUID()`, like the runtime's other ids, instead of `node:crypto`. The thread-stream replay tests now assert the fork's lease-owner contract: replayed events from an origin without a live lease are dropped, not deferred.
