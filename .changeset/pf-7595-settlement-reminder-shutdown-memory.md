---
'@mastra/core': patch
---

Harness `shutdown()` now waits for delegated-settlement reminder writes to finish, or reports an error at its deadline. Harness rejects `subagents.onDelegatedSettlement: 'remind'` when an agent that can delegate has no memory. A parent that switched to a memory-less mode after delegating still receives the reminder, in the memory of the mode it delegated from.
