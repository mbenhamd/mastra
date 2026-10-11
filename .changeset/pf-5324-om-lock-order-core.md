---
'@mastra/core': patch
---

[PF-5324] The in-memory store's guarded resource working-memory write now goes through `updateResource`, so overrides see it, and no longer copies the whole store on every write.
