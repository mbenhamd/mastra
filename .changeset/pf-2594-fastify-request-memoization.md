---
'@mastra/fastify': patch
---

The Fastify adapter now constructs the WHATWG `Request` lazily and at most once per lane per request instead of two to three times eagerly. Requests with no auth configured and handlers that never read `ctx.request` construct zero `Request` objects. Auth callbacks (`authenticateToken` / legacy `authorize`) share one memoized instance, and the handler-visible `ctx.request` is a separate instance, so header mutations made by auth callbacks can never leak into route handlers.
