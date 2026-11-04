---
'@mastra/core': patch
---

Fixed the Harness `subagent` control tool for cancelled suspended children and stalled storage. A child that was cancelled while waiting for input keeps its suspension record as audit evidence; `status` no longer reports it as `awaiting_input`, and `wait` treats it as settled instead of timing out. When a storage read stalls, `wait` now returns at most about a second after its timeout, or as soon as the call is aborted. If the first read stalls, it returns a retryable `harness.storage` error; otherwise it returns `timedOut` with the statuses from the last completed read.
