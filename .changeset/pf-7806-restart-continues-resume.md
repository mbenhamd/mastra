---
'@mastra/core': patch
---

Fixed `run.restart()` losing a resume answer after a crash. When a process died after `run.resume()` was accepted but before the resumed step finished, a restart re-ran that step without its resume data, so it suspended again and asked for the same answer. The accepted resume data is now stored with the resume claim, and `run.restart()` (including the boot-time recovery sweep) continues the resume with it once. This needs a workflows store with concurrent updates, such as `@mastra/pg` or `@mastra/libsql`. The answer is stored only where the resumed step's `running` write would be persisted, so not when `shouldPersistSnapshot` excludes `running` snapshots. The stored answer goes through the workflow's `pruneSnapshot` hook the same way the resumed step's own write does; if the hook removes or changes it, restart behaves as before. A restart checks the stored answer again against its own host's options. A nested run resumed directly stores the answer on both its own row and its parent's row, or on neither.

A nested workflow run is no longer restarted on its own while its parent run drives it. The same applies when this Mastra's recovery sweep will restart the parent. `run.restart()` on such a nested run now rejects with `WORKFLOW_RESTART_DEFERRED_TO_PARENT`. Restart the parent run instead. The boot-time recovery sweep leaves the nested run to its parent.

A run whose `shouldPersistSnapshot` declines the snapshot a resume settles with (for example a policy that persists only `suspended` snapshots) is no longer left `running` after that resume; its row returns to the suspended snapshot it had before the resume, without the stored answer, so the recovery sweep no longer runs it again. The shared storage test "should use shouldPersistSnapshot option" now passes for the default engine.

On the default engine, `run.resume()` now takes the run's recovery ownership lease before its claim, so a recovery sweep on another process skips a run that is being resumed. On the evented engine, a `run.resume()` issued before `mastra.startWorkers()` now starts the execution workers.
