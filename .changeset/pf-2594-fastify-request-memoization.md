---
'@mastra/fastify': patch
---

Improved Fastify request handling. The adapter now creates a web-standard Request object only when authentication or a route handler actually needs one, instead of two to three times on every request. Header changes made by authentication callbacks do not affect the `ctx.request` seen by route handlers.
