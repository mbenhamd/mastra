---
'@mastra/pg': patch
---

Added support for cancelling a Harness turn before it starts (`revokeTerminalGrant`) and for the `leaseOwner` option of `compareAndSwapSignalDispatch`.

**Database change.** Two columns of `mastra_harness_terminal_tombstones` now accept `NULL`. `init()` applies this automatically when the database role owns the table, and logs a warning otherwise. If you run with `disableInit: true` or `MASTRA_DISABLE_STORAGE_INIT=true`, or your runtime role does not own the table, apply this migration before cancelling turns:

```sql
ALTER TABLE mastra_harness_terminal_tombstones
  ALTER COLUMN session_incarnation DROP NOT NULL,
  ALTER COLUMN admission_hash DROP NOT NULL;
```

**Exported sessions.** Cancel a turn in the store its session was imported into. Cancelling it in the store that exported the session fails with `HarnessTerminalHandoffFencedError`.
