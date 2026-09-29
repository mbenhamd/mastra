---
'@mastra/pg': patch
---

The Postgres store now supports recovering admitted Harness `message()` turns whose process stopped mid-run. It can list recoverable sessions and turns. It settles an interrupted turn only while the recovering process still holds the session lease, the turn's dispatch is unchanged, and no terminal admission for it is pending, even when this store does not enable terminal handoff. It refuses a message reservation on a closed session with `HarnessStorageSessionClosedError`, and on a session whose lease another owner holds with `HarnessStorageLeaseConflictError`. It also refuses a terminal admission from a process that no longer holds the lease, or whose turn was already settled.
