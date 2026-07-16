---
'@mastra/core': major
'@mastra/libsql': major
'@mastra/pg': minor
---

Add resource-scoped, atomic thread-state mutations and a durable PostgreSQL thread-state store for task and goal signals.

Thread state is now keyed by resource as well as thread, so a thread's task and goal signals cannot be read or written across a resource boundary. Objective writes go through the same atomic `mutateState` path as task writes, deriving the run-count increment and status transition inside the store's lock instead of from a pre-judge read.

**Breaking:** existing `@mastra/libsql` thread-state rows are orphaned by this release. `ThreadStateLibSQL` shipped in 1.13.0 keyed on the raw `threadId`; every read and write now binds a resource-scoped key in the same table under the same primary key, with no fallback read and no backfill. After upgrading, a thread with an `active` goal objective resolves to `undefined`, so the agent silently stops working toward it and task lists reset mid-thread; the legacy rows linger until retention prunes them. **Before upgrading, drain or migrate existing thread-state rows.** Deployments that have never persisted thread state are unaffected.

**Breaking:** `resourceId` is now required on `Agent.getObjective`, `Agent.clearObjective`, `Agent.updateObjectiveOptions`, and `Agent.setObjective`. The first three previously accepted `{ threadId }` alone and the fourth accepted `resourceId` as optional, so existing callers no longer typecheck. The exported `ThreadStateStorage` and `ThreadStateKey` contracts also require `resourceId`, which breaks third-party thread-state adapters. Pass the resource that owns the thread — the same value already supplied to `stream`/`generate`.
