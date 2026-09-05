---
'@mastra/core': patch
---

Fixed dynamic workflow admission to reject scheduled inputs, state, and request context that violate their schemas before any member of a bundle is saved.
