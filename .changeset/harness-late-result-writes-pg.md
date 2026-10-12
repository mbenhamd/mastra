---
'@mastra/pg': patch
---

**Fixed:** A turn that finishes after its Harness session was deleted no longer saves its result in PostgreSQL.

**Added:** `deleteMessages` supports `sessionLeaseFence`. Messages are deleted only while the caller still owns the Harness session, so a process that lost the session cannot delete another owner's messages. Keep Harness and memory in the same schema to use it.

```ts
const memoryStore = await store.getStore('memory');
await memoryStore.deleteMessages(messageIds, {
  sessionLeaseFence: { harnessName: 'default', sessionId, ownerId },
});
// Throws HarnessStorageLeaseConflictError, and deletes nothing, if another process owns the session.
```

**Improved:** A store with session-record projections turned off can now safely delete sessions that another store projects. The delete still cleans up those projection records.
