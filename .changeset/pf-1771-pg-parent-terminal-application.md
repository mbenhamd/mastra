---
'@mastra/pg': minor
---

PostgreSQL workflow storage can now apply a nested workflow's terminal outcome to its parent atomically and only once. Racing application instances stay consistent, safe retries do not duplicate the update, and committed outcomes can be recovered after a process restart.

Conflicting or stale requests leave no partial parent update. Cleanup, schema export, and restart recovery include the stored continuation state.
