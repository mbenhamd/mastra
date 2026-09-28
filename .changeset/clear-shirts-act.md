---
'@mastra/core': minor
---

Fixed admitted Harness `message()` turns staying pending forever when the process running them stopped mid-run.

When `harness.session()` adopts a session whose previous owner died, an admitted `message()` turn with no live run and no still-valid dispatch claim is now settled as failed with the error code `harness.run_interrupted`. The adopter emits `run_completed` with `status: 'interrupted'` and `reconstructed: true`, and commits an aborted terminal intent when native terminal handoff owns the turn. The turn is never sent to the model again. To retry it, send a new message with a new `admissionId`.

Storage adapters that support this recovery (`supportsDispatchRecovery`) can also list sessions for a recovery worker to adopt:

```ts
const sessions = store.stores.harness!;
let cursor;
do {
  const page = await sessions.listRecoverableSessions({ limit: 100, cursor });
  for (const found of page.items) {
    if (found.closing) continue;
    await harness.session({ sessionId: found.sessionId, resourceId: found.resourceId });
  }
  cursor = page.nextCursor;
} while (cursor);
```
