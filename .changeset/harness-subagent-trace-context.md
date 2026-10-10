---
'@mastra/core': patch
---

Harness v1 subagent runs now join the parent run's trace instead of starting their own.

An inline `spawn_subagent` child's agent run is a child span of the spawning tool call. A durable `task_delegate` child runs under a `subagent delegation` child span of the `task_delegate` tool call. That span has a detached lifetime: the parent's span tree can end (including on suspension, abort or error) without closing the still-running delegation's spans. The delegation span ends once its drive has settled the delegation. Its output records the child's outcome and the settlement, and it ends as an error when the drive could not settle the delegation.

Subagent spans follow the parent trace's sampling decision and trace settings (`hideInput`, `hideOutput`, request-context keys) and inherit the parent run's span metadata, as ordinary child spans do. A turn's tracing metadata, for example `tracingOptions.metadata.requestId`, therefore reaches the subagent's model generations. The child's own run identity (`runId`, `threadId`) still takes precedence. Only spans change: subagent request contexts still receive only the keys in `subagents.inheritRequestContextAppKeys`.

```typescript
const rootAgent = new Agent({
  id: 'root-agent',
  name: 'root-agent',
  instructions: 'Delegate focused work to subagents.',
  model,
  // Inherited by every span of the turn, subagent model generations included.
  defaultOptions: { tracingOptions: { metadata: { requestId: 'turn-123' } } },
});
```

A delegated child re-driven after a process restart has no live parent span, so it starts its own trace.
