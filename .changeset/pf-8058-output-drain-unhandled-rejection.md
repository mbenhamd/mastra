---
'@mastra/core': patch
---

Fixed an unhandled promise rejection when a thread subscription closed while an agent run's output was still registering or finishing, for example when a Harness session was evicted. Waiting code still receives the rejection.
