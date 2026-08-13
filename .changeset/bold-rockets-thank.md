---
'@mastra/inngest': major
---

Inngest durable agents now reject response-only recovery configuration before dispatch because it requires live worker state.

**Operational migration:** The durable-agent workflow identity is now v3. Drain active v2 runs and complete suspended v2 runs on a v2 worker before deploying v3, or keep v2 workers available until those runs finish.
