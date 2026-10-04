---
'@mastra/pg': patch
---

Workflow lifecycle state reads now include a pending cancel request, so `run.requestCancel()` works with PostgreSQL storage.
