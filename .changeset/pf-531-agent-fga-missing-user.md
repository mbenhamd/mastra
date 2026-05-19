---
'@mastra/core': patch
---

Tightened agent execution authorization when a Mastra server FGA provider is configured. Fresh `agent.generate(...)` and `agent.stream(...)` calls now fail closed unless the effective `requestContext` includes a `user`, matching the existing resume API boundary. Existing callers that execute agents against an FGA-enabled server without a request-context user now receive an authorization error before the model runs.

```ts
const requestContext = new RequestContext();
requestContext.set('user', { id: 'user-1' });
await agent.generate('Summarize this', { requestContext });
```

Local agent calls without a configured FGA provider continue to run without requiring a `user` in request context.
