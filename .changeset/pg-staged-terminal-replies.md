---
'@mastra/pg': patch
---

Add `stageTerminalCompletion` to preserve completed native terminal output on the pending message-result row before terminal finalization. The row stays pending until the atomic terminal commit. Recovery refuses to replace staged completed output with an interrupted result and rediscovers committed completed output until its run summary acknowledges publication. Session usage fences persist atomically with the counters in their existing JSON column. These changes require no schema change.
