---
'@mastra/fastify': patch
---

Security hardening: the Fastify adapter now enforces body size limits on multipart aggregates. Multipart requests previously bypassed both Fastify's `bodyLimit` and the per-route cap (only a per-file busboy limit applied), so many small parts or fields were unbounded. `route.maxBodySize`, the adapter-wide `bodyLimitOptions.maxSize`, or the server `bodyLimit` is now enforced as an aggregate cap over the raw multipart stream and rejects breaches with the same `FST_ERR_CTP_BODY_TOO_LARGE` 413 as the non-multipart lane. Routes that combine `skipBodyParse` with a declared `maxBodySize` get the same aggregate cap through a passive raw-stream guard that leaves the unparsed stream byte-exact for the handler.
