---
'@mastra/core': patch
---

Fixed a restart on the evented engine bringing back a workflow run that was deleted while the restart was starting. The deleted run now stays deleted, and `run.restart()` returns a canceled result instead of waiting forever. A restart or resume whose run was canceled, or taken over by a newer restart, before its start was handled also returns canceled now, and a late duplicate of an earlier resume no longer cancels the resume that is running. A workflow's first start still saves its run as before. This needs a store that supports fenced step updates, such as the in-memory and PostgreSQL stores.
