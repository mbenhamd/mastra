---
'@mastra/core': minor
---

Added pre-admission grant revocation and closed two gaps in native terminal handoff for Harness `message()` turns.

**Revoke a grant before it is admitted.** `HarnessStorage.revokeTerminalGrant()` fences a grant without knowing how its turn would be admitted. When the grant is already admitted, it returns `{ status: 'admitted', admission }` and changes nothing. Otherwise it writes a durable tombstone and returns `revoked` (or `duplicate` when one exists). A later `message()` with that grant then rejects with `HarnessTerminalHandoffCancelledError` before the provider is called. While the session's lease is live, the refusal settles the turn's reservation with `harness.terminal_cancelled` in the same step, so the turn neither blocks closing the session nor is later reported as interrupted. Check `supportsTerminalGrantRevocation` before calling it.

```ts
const sessions = store.stores.harness!;
if (sessions.supportsTerminalGrantRevocation) {
  const receipt = await sessions.revokeTerminalGrant({
    harnessName: 'default',
    sessionId,
    admissionId,
    executionGrant: { key: grantKey, generation: 1 },
    reason: { code: 'turn_released', message: 'released before admission' },
  });
  if (receipt.status === 'admitted') {
    // The turn was admitted: settle it through its terminal intent instead.
  }
}
```

**Dispatch is fenced on the session lease.** `compareAndSwapSignalDispatch` accepts an optional `leaseOwner`. Harness passes it when it marks a `message()` turn as dispatching, so a process whose session another process adopted can no longer dispatch that turn: `message()` rejects with `HarnessSessionLockedError` (or `HarnessSessionClosedError`) and the provider is not called. The last check of the dispatch claim now runs immediately before the provider is called. One window remains: a process that pauses between that check and the provider request for longer than what remains of the dispatch claim (at most 30 seconds) can still call the provider after another process interrupted the turn.

**A rejected provider run settles as failed.** When the run of a native terminal turn ends with a provider or agent error, Harness now commits a durable `failed` terminal result and its delivery intent instead of leaving the turn pending. `onTerminalCommit` receives the receipt, and `message()` rejects with the run's error, redacted as for any failed run. A same-admission retry does not call the provider again and receives the same receipt, and adopting the session after a restart no longer reports the turn as interrupted. A failure whose outcome is unknown, such as a stream that ended without reporting an error or a terminal commit that failed, stays pending as before; a same-admission retry in the same process can still commit it. Aborting or deleting the session while that commit is stalled releases `message()` as it does for a completed run.

Custom storage adapters that support terminal handoff or dispatch recovery must honor the `leaseOwner` precondition of `compareAndSwapSignalDispatch`.
