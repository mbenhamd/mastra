---
'@mastra/editor': minor
---

Added an `ownedOnly` option to the Composio tool provider's `listConnections`. With it set, the listing returns only accounts the requested users own. SHARED accounts owned by other users are left out, even when their ACL admits a requested user.

```ts
const { items } = await provider.listConnections({
  toolkit: 'gmail',
  userId: 'workspace:user_42',
  ownedOnly: true,
});
```
