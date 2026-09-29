---
'@mastra/pg': patch
---

The Postgres store now supports recovering admitted Harness `message()` turns whose process stopped mid-run: it can list recoverable sessions and turns, settles an interrupted turn only while the recovering process still holds the session lease, the turn's dispatch is unchanged, and no terminal admission for it is pending (even when that store does not enable terminal handoff), and refuses a message reservation or terminal admission from a process that no longer holds the lease of the open session, or whose turn was already settled.
