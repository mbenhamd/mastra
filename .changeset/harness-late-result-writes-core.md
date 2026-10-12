---
'@mastra/core': patch
---

**Fixed:** A turn that finishes after its Harness session was deleted no longer saves its result. The late result is dropped, and the caller gets `HarnessSessionDeletedError`.

**Fixed:** A deleted session's kept turn record now says "The error message is not retained" instead of an empty message, so it stays a valid record.

**Added:** `deleteMessages` can now refuse to run when the caller no longer owns the chat session. Pass `sessionLeaseFence`, and check `supportsSessionLeaseFencedMessageDeletion` first. If another process took the session over, the call throws `HarnessStorageLeaseConflictError` and deletes nothing.

```ts
import { HarnessStorageLeaseConflictError } from '@mastra/core/storage';

const memoryStore = await storage.getStore('memory');
if (memoryStore?.supportsSessionLeaseFencedMessageDeletion) {
  try {
    await memoryStore.deleteMessages(messageIds, {
      sessionLeaseFence: { harnessName: 'default', sessionId, ownerId },
    });
  } catch (error) {
    if (!(error instanceof HarnessStorageLeaseConflictError)) throw error;
    // Another process owns the session now: nothing was deleted.
  }
}
```
