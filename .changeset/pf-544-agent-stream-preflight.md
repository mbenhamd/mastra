---
'@mastra/core': patch
---

Fixed: `Agent.stream()` now avoids unnecessary thread allocations when request validation denies a run.
