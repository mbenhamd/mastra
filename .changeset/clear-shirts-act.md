---
'@mastra/core': minor
---

Fixed admitted Harness `message()` turns staying pending forever when the process running them stopped mid-run.

When `harness.session()` adopts a session whose previous owner died, an admitted `message()` turn with no live run and no still-valid dispatch claim is now settled as failed with the error code `harness.run_interrupted`. The adopter emits `run_completed` with `status: 'interrupted'` and `reconstructed: true`. When the turn has a pending native terminal admission, the adopter also commits an aborted terminal intent for it. A turn that died before its terminal admission was written gets no intent, because there is no admission to commit against. The turn is never sent to the model again. To retry it, send a new message with a new `admissionId`.

The settlement only commits while the adopter still holds the session lease and the turn's dispatch is exactly what it observed, so a stalled owner that resumes is never overwritten. In turn, a stalled owner whose turn was interrupted can no longer dispatch or admit it when it resumes. If the settlement commits but the adopter fails before reporting it, the next recovery reports it. A turn that another process still claims is recovered once the claim expires, the next time the session is resolved with `harness.session()`. `harness.closeSession()` settles such turns before closing, and throws `HarnessSessionLockedError` while one of them is still pending or waiting for a user response. A close refused over a turn waiting for a response leaves the session open, so the turn can still be answered. Once that interaction is due, close expires it and closes. When an interaction expires, a native terminal turn waiting on it is now settled as failed with `harness.terminal_cancelled`, so it no longer blocks closing the session.

Storage adapters that support this recovery (`supportsDispatchRecovery`) can also list sessions for a recovery worker. A listed session has work the worker can finish: open sessions are adopted with `harness.session()`, and sessions whose close never finished are completed with `harness.closeSession()`. One deploy limitation applies. A turn whose pending terminal admission names a finalizer id and version that the recovering process doesn't register stays pending, and its session is listed again each time its lease lapses. Keep every finalizer version that may still have pending admissions registered on the processes that run recovery.

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
