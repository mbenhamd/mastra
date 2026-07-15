---
'@mastra/core': patch
'@mastra/pg': patch
---

Preserved accessor-backed tool invocation options while injecting request context, so canonical MCP context reaches tools without evaluating deprecated compatibility getters.

Preserved the documented PostgreSQL pagination validation messages while continuing to redact unknown and accessor-backed errors.
