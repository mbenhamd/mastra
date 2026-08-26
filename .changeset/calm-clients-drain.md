---
'@mastra/pg': patch
---

Fixed `PostgresStore.init()` so it finishes all table setup before returning the database connection to the pool. This prevents intermittent startup errors when multiple parts of an app call `init()` at the same time.

Concurrent calls now share one initialization through completion. If initialization fails, a later call retries only after work from the failed attempt has stopped.
