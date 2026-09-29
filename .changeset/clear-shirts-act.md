---
'@mastra/core': minor
---

Fixed admitted Harness `message()` turns staying pending forever when the process running them stopped mid-run.

When `harness.session()` adopts a session whose previous owner died, an admitted `message()` turn with no live run and no still-valid dispatch claim is now settled as failed with the error code `harness.run_interrupted`. The adopter emits `run_completed` with `status: 'interrupted'` and `reconstructed: true`, and commits an aborted terminal intent when native terminal handoff owns the turn. The turn is never sent to the model again. To retry it, send a new message with a new `admissionId`.

The settlement only commits while the adopter still holds the session lease and the turn's dispatch is exactly what it observed, so a stalled owner that resumes is never overwritten. In turn, a stalled owner whose turn was interrupted can no longer dispatch or admit it when it resumes. If the settlement commits but the adopter fails before reporting it, the next recovery reports it. A turn that another process still claims is recovered once the claim expires, the next time the session is resolved with `harness.session()`. `harness.closeSession()` settles such turns before closing, and throws `HarnessSessionLockedError` while one of them is still pending or waiting for a user response. A refused close leaves the session open, so a turn waiting for a response can still be answered.

Storage adapters that support this recovery (`supportsDispatchRecovery`) can also list sessions for a recovery worker. A listed session always has work the worker can finish: open sessions are adopted with `harness.session()`, and sessions whose close never finished are completed with `harness.closeSession()`.

```ts
const sessions = store.stores.harness!;
let cursor;
do {
  const page = await sessions.listRecoverableSessions({ now: Date.now(), limit: 100, cursor });
  for (const found of page.items) {
    const target = { sessionId: found.sessionId, resourceId: found.resourceId };
    if (found.closing) await harness.closeSession(target);
    else await harness.session(target);
  }
  cursor = page.nextCursor;
} while (cursor);
```
