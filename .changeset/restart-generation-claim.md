---
'@mastra/core': patch
---

Fixed `restart()` and `timeTravel()` running steps whose results were thrown away when two callers restarted the same workflow run at the same time. Only one caller can now take over the run. This also applies to runs created by older versions of Mastra. The other caller gets a `WORKFLOW_RESTART_NOT_CLAIMED` error and runs no steps. Before, the run could wrongly end as canceled.
