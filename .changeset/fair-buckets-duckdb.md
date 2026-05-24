---
'@mastra/duckdb': patch
---

Fixed DuckDB observability capability reporting before initialization.

Studio now waits for DuckDB observability storage to be ready before enabling metrics queries.
