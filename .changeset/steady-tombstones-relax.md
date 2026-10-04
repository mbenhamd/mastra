---
'@mastra/pg': patch
---

The Postgres store supports pre-admission grant revocation (`revokeTerminalGrant`) and the `leaseOwner` precondition of `compareAndSwapSignalDispatch`.

- A revocation is serialized with admission, commit, and cancellation of the same grant. It also writes the session row, so an execution-closure export that started before it fails and has to be retried, and the retried export carries the revocation. Revoking a grant of a session that an export already handed off throws `HarnessTerminalHandoffFencedError`: revoke it in the store the session was imported into.
- The `session_incarnation` and `admission_hash` columns of `mastra_harness_terminal_tombstones` are now nullable, because a revocation has no admission identity. `init()` and the first terminal handoff operation relax existing tables when the role owns them (otherwise they log a warning). With `disableInit: true` or `MASTRA_DISABLE_STORAGE_INIT=true`, or when the runtime role does not own the table, apply this migration before revoking grants:

```sql
ALTER TABLE mastra_harness_terminal_tombstones
  ALTER COLUMN session_incarnation DROP NOT NULL,
  ALTER COLUMN admission_hash DROP NOT NULL;
```

- A refused admission of a revoked or cancelled grant settles the undispatched reservation of an owner whose lease is live in the same transaction.
- A dispatch stamp with `leaseOwner` locks the session row before the evidence row and is refused when another owner holds the lease or the session is closed.
