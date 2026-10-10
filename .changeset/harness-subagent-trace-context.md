---
'@mastra/core': patch
---

Harness v1 subagent runs now join the parent run's trace instead of starting their own.

An inline `spawn_subagent` child's agent run is a child span of the spawning tool call. A durable `task_delegate` child runs under a `subagent delegation` span that joins the parent trace below the `task_delegate` tool call. Its lifetime is independent of the parent turn, so the parent's span tree can end without closing the child's spans.

Subagent spans now inherit the parent run's span metadata, as ordinary child spans do. A turn's tracing metadata, for example `tracingOptions.metadata.requestId`, therefore reaches the subagent's model generations. The child's own run identity (`runId`, `threadId`) still takes precedence. Only spans change: subagent request contexts still receive only the keys in `subagents.inheritRequestContextAppKeys`.

A delegated child re-driven after a process restart has no live parent span, so it starts its own trace.
