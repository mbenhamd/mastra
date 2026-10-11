---
'@mastra/core': patch
'@mastra/memory': patch
'@mastra/pg': patch
---

[PF-5257] `Memory` forwards its observer working-memory policy to atomic message edits, deletions and retractions, and `updateMessages` accepts `managedWorkingMemoryScope`. Observational Memory records whose observer does not manage working memory carry an explicit `null` marker, so the policy fallback reaches only records written before the marker existed.

A guarded observer working-memory write stamps a `null`-marked record managed in its own transaction, and `Memory.deleteThread` forwards the policy too.
