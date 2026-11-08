---
'@mastra/editor': minor
---

Added three Composio tool provider options:

- `requireToolApproval` asks for approval before a tool runs. Set it to `true` for every tool, or pass a function that decides per tool from the tags Composio publishes, such as `readOnlyHint`.
- `allowTracking` is passed to both Composio clients. Set it to `false` to turn off the Composio SDK's usage analytics.
- `resolveCatalogTools` builds one tool catalog that many users share, for example a tool search catalog. Each call runs as the user who made it, and a call with no resolved user is refused before it reaches Composio.

```ts
const provider = new ComposioToolProvider({
  apiKey: process.env.COMPOSIO_API_KEY!,
  allowTracking: false,
  requireToolApproval: tool => !tool.tags.includes('readOnlyHint'),
});

const tools = await provider.resolveCatalogTools({
  toolSlugs: ['GMAIL_FETCH_EMAILS', 'GMAIL_SEND_EMAIL'],
  resolveIdentity: ({ requestContext }) => lookUpAccount(requestContext),
});
```
