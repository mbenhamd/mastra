---
'@mastra/core': patch
---

Fixed `restart()` and `timeTravel()` running steps whose results were thrown away when two callers restarted the same workflow run at the same time. On stores with atomic concurrent updates (`supportsConcurrentUpdates()`), only one caller can now take over the run — including pre-upgrade snapshots that carry no execution generation, where the first caller to install one wins. The other caller gets a `WORKFLOW_RESTART_NOT_CLAIMED` error and runs no steps. Before, the run could wrongly end as canceled.
