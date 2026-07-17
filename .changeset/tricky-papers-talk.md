---
'@mastra/core': patch
---

Fixed durable agent callbacks so replayed events and competing terminal deliveries do not invoke user callbacks more than once.
