---
'@mastra/core': minor
---

Fixed admitted Harness `message()` turns staying pending forever when the process running them stopped mid-run.

When `harness.session()` adopts a session whose previous owner died, an admitted `message()` turn with no live run and no still-valid dispatch claim is now settled as failed with the error code `harness.run_interrupted`. The adopter emits `run_completed` with `status: 'interrupted'` and `reconstructed: true`, and commits an aborted terminal intent when native terminal handoff owns the turn. The turn is never sent to the model again. To retry it, send a new message with a new `admissionId`.

Storage adapters that support this recovery (`supportsDispatchRecovery`) can also list sessions for a recovery worker. A listed session always has work the worker can finish: an open session is adopted with `harness.session()`, and a session whose close never finished is completed with `harness.closeSession()`, which also interrupts its orphaned turns. Turns that are still claimed, waiting for a user response, or already finished do not cause a session to be listed.

```ts
const sessions = store.stores.harness!;
let cursor;
do {
  const page = await sessions.listRecoverableSessions({ limit: 100, cursor });
  for (const found of page.items) {
    const target = { sessionId: found.sessionId, resourceId: found.resourceId };
    if (found.closing) await harness.closeSession(target);
    else await harness.session(target);
  }
  cursor = page.nextCursor;
} while (cursor);
```
