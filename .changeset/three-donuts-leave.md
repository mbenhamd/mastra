---
'@mastra/core': minor
---

Added `MemoryStorage.deleteResource()` so storage adapters can delete a resource and its working memory without deleting threads or messages. Use `await memoryStore.deleteResource({ resourceId: 'resource-123' });` to invoke the API.
