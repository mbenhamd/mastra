---
'@mastra/core': patch
---

Fixed delegated-settlement reminders in Harness:

- `shutdown()` now waits for reminder writes to finish, or reports an error at its deadline.
- `subagents.onDelegatedSettlement: 'remind'` is rejected when a mode that can delegate has no memory.
- A parent that switched to a memory-less mode after delegating still gets the reminder, in the memory of the mode it delegated from.
