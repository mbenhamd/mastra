---
'@mastra/core': patch
'@mastra/libsql': patch
---

Fixed message admission retries so exact duplicate write races reuse the stored evidence without dispatching a second signal.
