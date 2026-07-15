---
'@mastra/core': minor
---

Added canonical replayable workflow lifecycle events with execution-scoped identity, deterministic semantic event IDs, and ordered retained cursors across start, retry, suspend, resume, cancellation, and completion.

```ts
const run = await workflow.createRun({ pubsub: exactReplayPubsub });
const identity = await run.getLifecycleExecutionIdentity();
const stop = await run.watchLifecycle(
  async envelope => {
    // Apply eventId idempotently and commit cursor + logGeneration in the
    // same durable projection transaction.
    await persistProjection(envelope);
  },
  lastCommitted
    ? {
        executionGeneration: lastCommitted.executionGeneration,
        afterCursor: lastCommitted.cursor,
        afterLogGeneration: lastCommitted.logGeneration,
      }
    : undefined,
);

console.log(identity.executionGeneration);
```

Lifecycle delivery is at least once: a semantic event keeps the same `eventId` when it is republished, while each retained delivery may have a later cursor. Lifecycle watchers require an exact indexed-replay transport. Durable consumers must apply `eventId` idempotently, commit their projection before the callback resolves, and resume with the execution generation, committed cursor, and retained-log generation from that checkpoint.

Competing create, start, resume, cancel, and terminal-completion operations for the same explicit `runId` still depend on a storage-atomic admission/terminal claim that is not yet available across adapters (tracked in PF-2013). Callers must serialize control operations for one run until that cross-adapter contract lands; generation checks prevent stale lineages from silently taking over but cannot close the final read/write race without storage compare-and-set.
