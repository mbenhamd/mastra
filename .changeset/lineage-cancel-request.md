---
'@mastra/core': minor
'@mastra/pg': patch
---

Added `run.requestCancel({ requestId, expectedExecutionGeneration, expectedLifecycleResumeAttempt })` to stop a workflow run from another process without canceling a newer execution of the same run. The request names the execution it targets and is stored on the run only while that execution still owns it, so a request that arrives after a restart or a later resume is rejected with `lineage_moved`.

A running execution ends as `canceled` at its next step boundary, in whichever process runs it; the requesting process also aborts its own execution at once. `restart()`, the startup recovery sweep and `resume()` mark a requested run `canceled` instead of running it again. A pending, suspended or paused run is canceled immediately. The PostgreSQL store returns the request in its lifecycle state reads.
