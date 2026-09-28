---
'@mastra/pg': patch
---

The Postgres store now supports recovering admitted Harness `message()` turns whose process stopped mid-run: it can list recoverable sessions and turns, settles an interrupted turn only while the recovering process still holds the session lease and the turn's dispatch is unchanged, and refuses a terminal admission from a process that no longer holds the lease or whose turn was already settled.
