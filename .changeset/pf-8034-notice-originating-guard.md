---
'@mastra/core': patch
---

Fixed delegated-settlement notices (`subagents.onDelegatedSettlement: 'remind'` or `'wake'`) for parents whose Observational Memory uses `sourceWriteFencing: 'required'`. The notice is now written with the source-write guard of the parent run that delegated the task, and a wake run reuses that guard. Parent turns no longer need to pass `observationalMemorySourceWriteGuard` to receive reminders. A notice whose conversation transcript was deleted since the delegation is dropped and logged at warn instead of being retried. Under required fencing, a notice from a delegation that recorded no guard (for example one still owed from before this release) is dropped the same way.
