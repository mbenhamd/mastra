---
'@mastra/pg': minor
---

Added `exportExecutionClosure` and `importExecutionClosure` to `PostgresStore`. Use them to move a harness session, its child sessions, and their memory and workflow state to another database, or to restore them after an outage.

Import checks the payload before it writes any rows. It writes everything in one transaction, so a retry is safe after a lost acknowledgement. It rejects corrupt payloads and rows that conflict with unrelated destination data. Imported sessions start without an owner, so leases and callbacks from the source cannot act on them.

```ts
const closure = await source.exportExecutionClosure({
  harnessName: 'default',
  sessionId: 'sess-123',
});

const result = await destination.importExecutionClosure(closure);
// result.status: 'imported' | 'pinned'
// result.incarnations: sessionId -> destination session incarnation token
```
