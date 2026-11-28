---
'@mastra/editor': minor
---

Added `listOwnedConnections` to the Composio tool provider. It lists every account one user owns across several toolkits in a single listing and follows Composio's pages to the end, so checking which apps a user has connected takes one request instead of one per toolkit. Like `listConnections` with `ownedOnly`, it relies on Composio's owner filter, so SHARED accounts owned by other users are not included. If Composio fails, returns a page without items, an account without an id or toolkit, a cursor that is not a string, a repeated cursor or more than 50 pages, the call throws instead of returning a partial list.

```ts
const connections = await provider.listOwnedConnections({
  userId: 'workspace:user_42',
  toolkits: ['gmail', 'googlecalendar'],
});
const connected = new Set(connections.filter(c => c.status === 'active').map(c => c.toolkit));
```
