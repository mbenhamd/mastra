---
'@mastra/memory': patch
---

Prevent cross-resource thread clones from inheriting Working Memory owner authority or copying Observational and Working Memory from a stale source owner. Memory-bearing clones now fail before mutation when storage cannot atomically snapshot source ownership.
