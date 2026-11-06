---
'@mastra/core': patch
---

Fixed duplicate and late parent wakes in Harness v1 when `subagents.onDelegatedSettlement` is `'wake'` and a storage write fails.

- The parent wakes once after a delegated task settles, even when one storage write fails. The Harness retries the write in the same process and logs each failure.
- A failed write no longer delays the wake. The Harness retries it, waiting longer after each failure.
- If a write keeps failing, or the session closes or loses ownership first, the Harness logs an error. The parent can then wake twice.
- After a Stop, the notice stays a reminder and does not wake the parent.
- If a run loads the notice between a failed attempt and its retry, that run counts as the delivery. No extra wake follows.
- A failure to record the loaded history no longer fails the run. The Harness logs an error. A duplicate wake can follow.
