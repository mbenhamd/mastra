---
'@mastra/core': patch
---

Fixed two gaps in Harness `subagents.onDelegatedSettlement` notices. `Harness.shutdown()` no longer reports success while a delegated-settlement reminder is still being written to the parent's memory: shutdown's bounded settle now waits for running notice deliveries and fails at its deadline naming the outstanding session storage work, and a delivery does not start or begin its reminder write once shutdown has begun (the notice stays owed for the next reconcile). `onDelegatedSettlement: 'remind'` is now rejected with a `HarnessConfigError` when a mode that can delegate (Harness built-ins not excluded) uses an agent without memory, instead of silently acknowledging notices that were never persisted.
