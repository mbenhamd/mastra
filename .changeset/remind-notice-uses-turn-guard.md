---
'@mastra/core': patch
---

Fixed `subagents.onDelegatedSettlement: 'remind'` for parents whose Observational Memory uses `sourceWriteFencing: 'required'`. The reminder used to fail on every save. It is now saved at the start of the parent's next `message()` call that passes `observationalMemorySourceWriteGuard`, so pass that option on parent turns to receive reminders.
