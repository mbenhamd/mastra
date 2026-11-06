---
'@mastra/core': patch
---

Fixed duplicate and late parent wakes in Harness v1 when `subagents.onDelegatedSettlement` is `'wake'` and a storage write fails.

- The parent wakes once after a delegated task settles, even when one storage write fails. The Harness retries the write in the same process and logs each failure.
- A failed claim write no longer delays the wake until a later reconcile pass. The Harness retries it in the same process, with backoff.
- If a write keeps failing, or the session closes or loses its lease first, the Harness logs an error. The parent can then wake a second time.
- After a Stop, the notice stays a reminder and does not wake the parent.
- A run that loads the notice between a failed attempt and its retry counts as the delivery. No extra wake follows.
- Recording which history a run loaded can no longer fail the run. On failure the Harness logs an error, and a duplicate wake can follow.
