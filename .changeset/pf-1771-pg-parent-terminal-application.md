---
'@mastra/pg': minor
---

PostgreSQL workflow storage can now apply a nested workflow's terminal outcome to its parent atomically and only once. Racing application instances stay consistent, safe retries do not duplicate the update, and committed outcomes can be recovered after a process restart.

```ts
import { PostgresStore } from '@mastra/pg';

const storage = new PostgresStore({ id: 'app-storage', connectionString: process.env.DATABASE_URL! });
const workflows = await storage.getStore('workflows');
if (!workflows) throw new Error('Workflow storage is unavailable');

const parent = await workflows.getWorkflowTerminalParentContext(fence);
if (parent.status !== 'found') throw new Error('Parent run is unavailable');

const applied = await workflows.applyWorkflowTerminalParentEffect({ ...fence, contract });
```

Conflicting or stale requests leave no partial parent update. Cleanup, schema export, and restart recovery include the stored continuation state.
