---
'@mastra/core': patch
---

Fixed custom route registration when using a custom API prefix. Routes starting with `/api/` are no longer rejected when the server is configured with a different API prefix.
