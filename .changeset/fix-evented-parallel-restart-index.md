---
'@mastra/core': patch
---

Fix evented parallel workflow restarts that could resume the wrong branch, preserve magic-key branch IDs for pending nested workflows, and reject corrupt persisted branch paths before dispatch.
