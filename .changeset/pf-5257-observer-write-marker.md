---
'@mastra/core': patch
'@mastra/pg': patch
---

[PF-5257] A guarded observer working-memory write stamps a `null`-marked Observational Memory record only after the write succeeds, and only when it changed working memory; OM thread-metadata updates no longer stamp it.
