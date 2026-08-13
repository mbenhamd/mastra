---
'@mastra/inngest': patch
---

Inngest durable agents now reject response-only recovery configuration before dispatch because it requires live worker state. This uses the v3 durable workflow identity; keep v2 workers available until existing v2 runs have drained.
