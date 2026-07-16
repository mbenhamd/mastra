---
'@mastra/inngest': major
---

Added canonical replayable workflow lifecycle routing with stable semantic event IDs and ordered retained cursor envelopes to the Inngest workflow adapter, preserving execution lineage across durable replay and nested resume while assigning fresh lineage to new starts and time travel.

```ts
const inngestWorkflow = createWorkflow({
  // ...workflow schemas and steps
  pubsubFactory: defaultPubsub => customWorkflowTransport(defaultPubsub),
});

// Use a shared atomic server cache (for example Redis) on every replica for
// durable cross-process replay. The adapter adds exact indexed replay around
// the workflow-level transport.
const mastra = new Mastra({ cache: durableCache, workflows: { inngestWorkflow } });
const run = await inngestWorkflow.createRun();
const stop = await run.watchLifecycle(event => {
  // Project event.eventId idempotently; delivery is at least once.
  console.log(event.cursor, event.logGeneration, event.event);
});
```

Reconnect with the saved `executionGeneration` as well as its cursor and log generation so a later runId takeover cannot redirect the consumer to another execution.

Per-run `createRun({ pubsub })` configuration is rejected because a local object cannot be reproduced by remote Inngest function replicas; use `pubsubFactory` on the workflow instead.

Inngest durable-agent observers and workflow publishers now share both the retained cache and the caller-provided live transport path, preventing workflow-emitted agent chunks from being stranded until observer reconnect. Numeric durable-agent `observe({ offset })` remains scoped to the current retained log; generation-bound cursor resume is provided by workflow lifecycle watching.

**Breaking:** `createInngestAgent` now derives distinct parent-loop and nested-execution workflow IDs from a stable hash of the public durable-agent ID. This prevents multiple durable agents on one Mastra instance from collapsing onto the first agent's served Inngest functions, live transport, and replay cache. Before upgrading, drain or cancel every in-flight Inngest durable-agent run created with the former shared workflow IDs; its queued events and stored snapshots cannot be resumed under the new per-agent IDs. Direct `createInngestDurableAgenticWorkflow({ inngest })` callers keep the historical shared IDs unless they explicitly pass `workflowIds`; use `createInngestDurableAgenticWorkflowIds(ownerId)` to derive the same collision-safe pair.

**Breaking:** `createInngestAgent` execution now fails closed unless the durable-agent wrapper is registered on a Mastra instance with workflow storage. Passing the optional `mastra` reference alone is not registration. Register the wrapper through `new Mastra({ storage, agents: { durableAgent } })` (or `mastra.addAgent(durableAgent)`) before calling `stream`, `generate`, or resume APIs; this lets dispatch use the canonical persisted `InngestRun` lifecycle path instead of an untracked direct event send.

Custom `CachingPubSub` transports must expose an indexed replay backend; the adapter now fails closed instead of silently weakening replay guarantees.

Competing create, start, resume, cancel, and terminal-completion operations for one explicit `runId` must still be serialized by the caller until the shared workflow-storage contract supports atomic admission and terminal claims (tracked in PF-2013).
