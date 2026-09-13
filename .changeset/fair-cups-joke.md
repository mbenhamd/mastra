---
'@mastra/inngest': patch
---

Fixed nested workflow run IDs so separate invocations, loop iterations, and time-travel generations execute independently while replay and suspended resume retain their child identities. Nested time travel reads the original child snapshot before starting the new generation. Nested writer events now use the canonical watch envelope expected by PubSub subscribers.
