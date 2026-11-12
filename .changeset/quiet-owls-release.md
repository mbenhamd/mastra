---
'@mastra/core': patch
---

Harness: when a question or approval is declined or expires while no turn is running, the agent run waiting on it is now released. The user's next message starts right away, instead of being refused until the suspended-run timeout.
