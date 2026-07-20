---
'@mastra/core': patch
---

Fixed durable goal and completion scorers so warm runs receive their full live request context while cold recovery remains limited to explicitly persisted keys.
