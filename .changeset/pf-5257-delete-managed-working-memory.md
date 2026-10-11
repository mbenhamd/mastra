---
'@mastra/core': patch
'@mastra/pg': patch
---

[PF-5257] Message deletion with Observational Memory retraction can clear observer-written working memory of an unmarked record in the same storage transaction (`managedWorkingMemoryScope`). The PostgreSQL guarded observer working-memory write no longer fails with inconsistent timestamp parameter types.
