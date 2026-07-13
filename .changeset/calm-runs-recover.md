---
'@mastra/core': major
---

Added `listSuspendedRuns()` so applications can discover and resume suspended standard or durable agent runs after a process restart.

```typescript
const { runs } = await agent.listSuspendedRuns()
```

Results include the exact tool-call identifier needed to approve, decline, or resume the intended call. Durable recovery validates the persisted run owner and runtime version before continuing. Resume inputs are snapshotted before asynchronous work, non-suspended snapshots are rejected, and standard agent authorization is rechecked against the verified owner before execution. `DurableAgent.prepare()` now keeps live runtime bindings private and returns an idempotent `cleanup()` function for abandoned preparations.

Built-in `Run.startAsync()` results now also expose an optional `execution` promise. Fire-and-forget callers can ignore it; runtime owners such as `EventedAgent` can observe background setup/execution rejection instead of relying on log-only failures.

**Breaking change:** `DurableAgent.prepare()` no longer returns `registryEntry`. Use the returned IDs and serialized `workflowInput`, pass the same request to `stream()` to consume the one-time preparation, and call `cleanup()` when abandoning it.

```typescript
const prepared = await durableAgent.prepare(messages, { runId })

try {
  await durableAgent.stream(messages, { runId })
} catch (error) {
  prepared.cleanup()
  throw error
}
```
