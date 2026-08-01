---
'@mastra/server': patch
---

Faster streamed responses. Stream chunks now serialize through a plain `JSON.stringify` fast path — 2.3-3.9x faster per chunk in our benchmarks — and fall back to the safe serializer only for chunks plain serialization cannot handle (BigInt values, circular references). Output stays byte-identical. The server also reuses one frozen snapshot of its route table instead of copying the table on every request.
