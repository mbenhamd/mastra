---
'@mastra/core': patch
---

Fixed a delegated-task reminder that could be lost when a new parent turn started while the reminder was still being saved. With `subagents.onDelegatedSettlement: 'remind'`, a turn now waits briefly for reminders being saved before it reads the conversation history, so the model sees the reminder exactly once.
