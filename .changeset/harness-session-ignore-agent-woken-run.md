---
'@mastra/core': patch
---

Stop a Harness session from reporting a run it did not start as its current run. A run woken directly through the agent on the session's thread while the session is idle no longer sets `currentRunId` or `currentRun` in `getDisplayState()`, so the session no longer reports that run as running after it has ended.
