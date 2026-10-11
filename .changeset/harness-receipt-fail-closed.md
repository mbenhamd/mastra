---
'@mastra/core': patch
---

Harness v1 no longer records a failed tool call as successful when a later result arrives for the same call. A `tool-result` now replaces only an earlier `tool-result` receipt for that call, and never turns an error into success. A tool error, an aborted tool and a receipt restored after resume are kept, so contradictory results still leave the call unbindable as subagent evidence.
