---
'@mastra/core': patch
---

Fixed evented workflow restarts that could bring back a deleted run.

- A run deleted while its restart was starting now stays deleted, and `run.restart()` returns a canceled result instead of waiting forever. This needs the in-memory or PostgreSQL store.
- A restart or resume now returns canceled when its run was canceled, or taken over by a newer restart, before it started.
- A late duplicate of an earlier resume no longer cancels the resume that is running.
