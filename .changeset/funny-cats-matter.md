---
'@mastra/hono': minor
---

Populated `MASTRA_FRAMEWORK_PUBLIC_KEY` on the Hono context inside `registerContextMiddleware()`.

Framework authentication can inspect this request-scoped signal to skip only its own authentication check for routes declared public via `createPublicRoute()` or `requiresAuth: false`. General host middleware, including security, audit, rate-limit, and tenancy middleware, continues to run.
```
