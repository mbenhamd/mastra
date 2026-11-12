---
'@mastra/editor': patch
---

Added `owned` and, when Composio reports it, `accountType` to each Composio connection so callers can tell their own accounts from shared accounts they can access. Set `authorId` for an owned account when one user was requested.

Previously, the same listing did not identify ownership. The new fields let callers distinguish connections without treating an accessible shared account as their own:

```ts
import { ComposioToolProvider } from '@mastra/editor/composio';

const provider = new ComposioToolProvider({ apiKey: process.env.COMPOSIO_API_KEY! });
const { items } = await provider.listConnections({ toolkit: 'gmail', userId: 'user_42' });
const ownedConnections = items.filter(connection => connection.owned);
const sharedConnections = items.filter(connection => connection.accountType === 'SHARED');
```

Fixed incomplete ownership lookups returning a connection as not owned when Composio repeats a pagination cursor. These lookups now reject so callers can handle the failure.
