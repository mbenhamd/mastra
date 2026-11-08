---
'@mastra/core': patch
---

Fixed `run.restart()` losing a resume answer after a crash. A restart now continues an accepted `run.resume()` with its answer, so the step no longer suspends and asks again. The boot-time recovery sweep does the same, and each answer is continued at most once.

- **Store:** this needs a workflows store with concurrent updates, such as `@mastra/pg` or `@mastra/libsql`.
- **When the answer is not stored:** `shouldPersistSnapshot` excludes `running` snapshots, or `pruneSnapshot` removes or changes the answer. Restart then behaves as before.
- **Nested runs:** a nested run that its parent drives is no longer restarted on its own. `run.restart()` rejects with `WORKFLOW_RESTART_DEFERRED_TO_PARENT`, so restart the parent instead. The recovery sweep leaves the nested run to its parent.
- **Declined settle:** if `shouldPersistSnapshot` declines the snapshot a resume settles with, the run returns to its suspended snapshot instead of staying `running`, and the recovery sweep no longer runs it again. After a restart, a declined settle still leaves the row `running`.
- **Default engine:** `run.resume()` takes the run's recovery ownership lease, so a recovery sweep on another process skips the run while it resumes. A nested run resumed directly takes its parent's lease. If another live process holds the parent's lease, the answer is not stored.
- **Evented engine:** evented runs take no recovery lease. `run.resume()` before `mastra.startWorkers()` now starts the execution workers.
