---
'@mastra/core': patch
---

Improved transient workflow performance by avoiding unnecessary storage checks for new runs. Runs with caller-provided or custom-generated IDs still consult storage.
