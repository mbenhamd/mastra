---
'@mastra/pg': patch
---

Fixed delayed workflow writes from a deleted run lifetime leaking stale step output into a reopened run when using the Postgres store. Stale writes are rejected inside the store's row lock before merging.
