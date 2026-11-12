---
'@mastra/core': patch
---

Moved the harness session's queue-head state (the queued item whose turn is running and the pending queue() promises) into its own internal module. There is no change to the public API or to runtime behavior.
