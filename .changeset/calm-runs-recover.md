---
'@mastra/core': major
---

- Added `listSuspendedRuns()` for discovering and resuming suspended standard or durable agent runs after a process restart.
- Durable recovery now validates the persisted run owner and runtime version before continuing.
- Built-in `Run.startAsync()` results now expose an optional `execution` promise for observing background execution failures.

**Breaking change:** `DurableAgent.prepare()` no longer returns `registryEntry`.

- Before: read `registryEntry` from the prepared result.
- After: reuse the returned IDs and `workflowInput` with `stream()`, and call `cleanup()` when abandoning the preparation.
