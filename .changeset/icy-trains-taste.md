---
'@mastra/memory': minor
---

Added `Memory.deleteResource()` to delete a resource and its working memory without deleting threads or messages. Use `await memory.deleteResource('resource-123');` to invoke the API.
