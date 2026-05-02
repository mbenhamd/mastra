---
'@mastra/core': patch
'@mastra/observability': patch
---

Improve agent response latency by making post-finish persistence non-blocking, deduplicating concurrent finish handling by run id, and reducing model tracing stream overhead.
