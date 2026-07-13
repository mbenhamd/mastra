---
'@mastra/pg': minor
---

Added PostgreSQL support for deleting a resource and its working memory without deleting threads or messages. Call `await memoryStore.deleteResource({ resourceId: 'resource-123' });`; repeated calls are safe.
