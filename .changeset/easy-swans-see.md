---
'@mastra/core': major
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

**Breaking:** every workflow run suspended by an earlier `@mastra/core` becomes unresumable after upgrading. Lifecycle restore requires a reserved `executionGeneration` on the run snapshot, and snapshots written before this release do not carry one, so `run.resume()` fails closed rather than replaying a run whose lifecycle lineage cannot be established. Snapshots are not versioned and there is no backfill. **Before upgrading, drain or cancel every in-flight run** — most importantly runs parked on a suspend such as human approval, which are the ones most likely to be waiting when a deploy lands. Runs started after the upgrade reserve their generation at creation and are unaffected.

**Breaking:** `UpdateWorkflowStateOptions.status` (exported from `@mastra/core/storage`) is now optional, so lifecycle metadata can be patched without restating a status. Storage adapters that read `opts.status` into a required field no longer typecheck, and an adapter that assigns `snapshot.status = opts.status` unconditionally would write `undefined` on a metadata-only patch. Adapters that spread (`{ ...snapshot, ...stateOptions }`) or merge server-side need no change. Update custom adapters to treat `status` as optional and preserve the existing status when it is absent.

Competing create, start, resume, cancel, and terminal-completion operations for the same explicit `runId` still depend on a storage-atomic admission/terminal claim that is not yet available across adapters (tracked in PF-2013). Callers must serialize control operations for one run until that cross-adapter contract lands; generation checks prevent stale lineages from silently taking over but cannot close the final read/write race without storage compare-and-set.
