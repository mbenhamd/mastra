---
'@mastra/core': patch
---

Fixed delegated plan tasks losing their status while a subagent works on them. A delegated task now stays `in_progress` until its delegation settles. Before, a failed child or a reopened dependency could change it, and retrying a failed subtree could leave it unrecoverable after a restart.
