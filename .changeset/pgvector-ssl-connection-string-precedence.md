---
'@mastra/pg': patch
---

Fixed `PgVector` SSL configuration when both a connection string and explicit `ssl` option are provided.
Explicit SSL settings now take priority, matching `PostgresStore` behavior.
