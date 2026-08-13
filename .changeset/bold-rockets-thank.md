---
'@mastra/inngest': patch
---

Inngest durable agents now reject response-only recovery configuration before dispatch because it requires live worker state.
