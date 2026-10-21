---
'@mastra/core': patch
---

Fixed a deleted workflow run coming back when a resume, restart or time travel of it was already running. Its next saved step used to recreate the run, together with its output. Now the run stays deleted, including in a fresh process and for workflows that save only some states. Only `run.start()` can still create a run's row.
