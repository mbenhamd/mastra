---
'@mastra/core': patch
---

Added an internal contract for safely continuing a parent after a nested workflow completes, fails, or is canceled. It defines bounded validation and consistent request-context handling for future storage and runtime integration, including suspended, loop, branch, and foreach continuations. The contract is not wired into the live continuation path and adds no public API.
