---
'@mastra/core': minor
---

Added a way to cancel a Harness `message()` turn before it starts, and fixed two cases where a turn could run or end in the wrong state.

**Cancel a turn before it starts.** Call `revokeTerminalGrant()` on the harness storage. A later `message()` with that grant fails with `HarnessTerminalHandoffCancelledError` and never calls the model. If the turn already started, nothing changes and you get `{ status: 'admitted' }` back. Check `supportsTerminalGrantRevocation` first.

```ts
const harnessStorage = store.stores.harness!;
if (harnessStorage.supportsTerminalGrantRevocation) {
  const result = await harnessStorage.revokeTerminalGrant({
    harnessName: 'default',
    sessionId,
    admissionId,
    executionGrant: { key: grantKey, generation: 1 },
    reason: { code: 'turn_released', message: 'released before it started' },
  });
  if (result.status === 'admitted') {
    // The turn already started: handle it when it finishes.
  }
}
```

**Fixed: a process that lost its session could still start a turn.** When another process takes over a session, the old process can no longer start that session's pending turn. `message()` fails with `HarnessSessionLockedError` or `HarnessSessionClosedError`, and the model is not called. One case remains: a process that freezes for longer than the rest of its 30-second start window, right before the model call, can still call the model once.

**Fixed: a turn whose model call failed stayed pending.** When the model or agent reports an error, the turn now ends as `failed`, and `onTerminalCommit` receives the result. A retry of the same turn does not call the model again. After a restart, the turn is no longer reported as interrupted.

Custom harness storage adapters that support terminal handoff or dispatch recovery must honor the new `leaseOwner` option of `compareAndSwapSignalDispatch`.
