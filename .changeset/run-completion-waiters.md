---
'@mastra/core': patch
---

Fixed a harness session that could leave a caller waiting forever on a run. This happened when the run's result failed to load just as another caller started waiting on the same run, after the run's earlier outcome had already been dropped from the session's cache. That caller now receives the run's failure. In the same rare situation, a caller always receives the failure the session recorded for the run.
