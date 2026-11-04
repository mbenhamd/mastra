---
'@mastra/core': patch
---

Fixed the Harness `subagent` control tool for cancelled suspended children and stalled storage. A child that was cancelled while waiting for input keeps its suspension record as audit evidence; `status` no longer reports it as `awaiting_input`, and `wait` treats it as settled instead of timing out. `wait` now also stops at its timeout, or as soon as the call is aborted, when a storage read it depends on stalls, returning its usual `timedOut` or `aborted` result.
