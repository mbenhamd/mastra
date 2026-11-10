---
'@mastra/core': patch
---

A signal that joins another signal's run no longer takes over that run's origin, so a question asked in the run settles the signal that started it.
