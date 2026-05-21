---
'@mastra/core': patch
---

Fixed task status updates to automatically move only one task to "in progress" at a time. When moving a task to in-progress status, any previously in-progress task is now automatically demoted instead of returning an error.
