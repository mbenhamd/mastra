---
'@mastra/memory': patch
'@mastra/core': patch
'@mastra/pg': patch
---

Fixed failed thread clones so rollback preserves concurrent changes and unrelated observational memory.
