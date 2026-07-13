---
'@mastra/core': patch
---

Fixed evented parallel workflow restarts that could resume the wrong branch. Pending nested workflows now preserve all branch names, and invalid saved restart paths are rejected before any branch runs.
