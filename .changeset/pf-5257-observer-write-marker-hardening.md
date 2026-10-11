---
'@mastra/core': patch
'@mastra/pg': patch
---

[PF-5257] An in-memory guarded observer working-memory write and its marker stamp land together, PostgreSQL locks the guarded Observational Memory row before the write's target row, and `updateObservationalMemoryConfig` on PostgreSQL merges under a row lock so it cannot drop a concurrent marker stamp.
