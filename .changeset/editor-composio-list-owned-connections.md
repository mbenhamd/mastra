---
'@mastra/editor': minor
---

Added `listOwnedConnections` to the Composio tool provider. It lists every account one user owns across several toolkits in a single listing and follows Composio's pages to the end, so checking which apps a user has connected takes one request instead of one per toolkit. SHARED accounts owned by other users are left out. If Composio fails or repeats a page, the call throws instead of returning a partial list.

```ts
const connections = await provider.listOwnedConnections({
  userId: 'workspace:user_42',
  toolkits: ['gmail', 'googlecalendar'],
});
const connected = new Set(connections.filter(c => c.status === 'active').map(c => c.toolkit));
```
