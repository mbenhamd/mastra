---
'@mastra/pg': patch
---

Add `stageTerminalCompletion` to preserve completed native terminal output on the pending message-result row before terminal finalization. The row stays pending until the atomic terminal commit. Recovery refuses to replace staged completed output with an interrupted result. This uses the existing result column and requires no schema change.
