---
'@mastra/server': patch
---

Stream chunk serialization now takes a plain `JSON.stringify` fast path and only falls back to the replacer-based safe serializer for chunks it cannot handle (BigInt, circular references). Output is byte-identical; SSE and datastream responses serialize each chunk 2-4x faster. `getServerRoutes()` also returns a cached frozen snapshot instead of copying the route table on every request.
