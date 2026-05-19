---
"@mastra/core": patch
"@mastra/server": patch
"@mastra/express": patch
"@mastra/fastify": patch
"@mastra/hono": patch
"@mastra/koa": patch
"@mastra/nestjs": patch
---

Added Harness v1 remote session mutation primitives and HTTP routes.

Remote clients can now admit message and queued turns by idempotency key, mutate session state with an ETag validator, switch mode or model, update permissions, respond to pending inbox items, and manage session goals through the server route layer.
