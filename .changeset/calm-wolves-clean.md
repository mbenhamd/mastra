---
'@mastra/core': patch
'@mastra/inngest': patch
'@mastra/redis-streams': patch
---

Fence durable abort delivery, listeners, cleanup, and stale result handles to the run's immutable runtime binding. Awaitable Core and Inngest abort handles now reject when remote dispatch cannot be confirmed. Inngest rejects overlapping run-ID registration without replacing active runtime state and retries terminal ERROR publication, abort-replay subscription, and retained-topic deletion before committing finalization. Redis Streams exposes strict cleanup failures to retryable durable operations while preserving the existing best-effort `clearTopic()` contract.
