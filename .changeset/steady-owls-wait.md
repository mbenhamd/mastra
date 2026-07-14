---
'@mastra/core': patch
---

Prevent workflow events from being redelivered after their retries are exhausted, and keep nested workflow runs available while the final failure is recorded.
