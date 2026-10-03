---
'@mastra/pg': patch
---

The Postgres store supports pre-admission grant revocation (`revokeTerminalGrant`) and the `leaseOwner` precondition of `compareAndSwapSignalDispatch`.

- A revocation is serialized with admission, commit, and cancellation of the same grant. It also writes the session row, so an execution-closure export that started before it fails and has to be retried, and the retried export carries the revocation. Revoking a grant of a session that an export already handed off throws `HarnessTerminalHandoffFencedError`: revoke it in the store the session was imported into.
- The `session_incarnation` and `admission_hash` columns of `mastra_harness_terminal_tombstones` are now nullable, because a revocation has no admission identity. `init()` and the first terminal handoff operation relax existing tables. With `disableInit: true`, apply this migration before revoking grants:

```sql
ALTER TABLE mastra_harness_terminal_tombstones
  ALTER COLUMN session_incarnation DROP NOT NULL,
  ALTER COLUMN admission_hash DROP NOT NULL;
```

- A refused admission of a revoked or cancelled grant settles the lease holder's undispatched reservation in the same transaction.
- A dispatch stamp with `leaseOwner` locks the session row before the evidence row and is refused when another owner holds the lease or the session is closed.
