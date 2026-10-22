---
'@mastra/core': patch
---

Fixed `createEventedAgent()` ignoring `durableRequestContextKeys` and `cleanupTimeoutMs`. An evented agent made with the factory now keeps only the allowlisted request context in storage, as `createDurableAgent()` and `new EventedAgent()` do.
