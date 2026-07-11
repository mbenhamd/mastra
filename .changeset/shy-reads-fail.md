---
'@mastra/pg': patch
---

Changed PostgreSQL thread and message list reads to throw safe typed storage errors on backend failures instead of returning empty results.
