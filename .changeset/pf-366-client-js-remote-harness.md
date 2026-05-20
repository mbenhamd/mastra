---
"@mastra/client-js": minor
---

Added first-party client-js RemoteHarness and RemoteSession resources for Harness v1 server routes.

`MastraClient#getHarness(name)` now exposes remote session listing, creation/loading, snapshots, state/mode/model updates, permissions, inbox responses, goals, close, durable message admission, queued work admission, SSE subscription with Last-Event-ID replay, and result lookup settlement for interrupted message/queue operations. Remote skill APIs fail explicitly until matching server routes exist.

**Usage**

```ts
import { MastraClient } from '@mastra/client-js';

const client = new MastraClient({ baseUrl: 'http://localhost:4111' });
const session = await client.getHarness('default').session({ threadId: { fresh: true } });

const unsubscribe = session.subscribe(event => {
  console.log(event.type);
});

const result = await session.message({
  content: 'Summarize the workspace status',
  admissionId: 'workspace-summary-1',
});

unsubscribe();
```
