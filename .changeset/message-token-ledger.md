---
'@mastra/core': patch
---

Harness sessions keep counting each run's message token usage once, with the bookkeeping behind that guarantee now held in one place and covered by its own tests. There is no change to the public API or to runtime behavior.
