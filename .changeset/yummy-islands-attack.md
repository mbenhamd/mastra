---
'@mastra/redis-streams': patch
---

Fixed Redis Streams delivery to preserve caller-assigned event identity, cursors, and log generations across local, brokered, and retried events.
