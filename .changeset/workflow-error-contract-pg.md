---
'@mastra/pg': minor
---

PostgreSQL workflow storage errors now say whether the failed operation applied. Each workflow storage `MastraError` carries `details.persistenceFailure`: `transient` for lock timeouts, conflicts, timeouts and lost connections before a write committed, `permanent` for rejected statements and contract errors, and `commit_unknown` when a write's COMMIT may have succeeded although the connection failed. Read it with `getStoragePersistenceFailure` from `@mastra/core/storage`. The store never retries a `commit_unknown` write itself.
