---
'@mastra/pg': patch
---

Added `releaseWorkflowSnapshotHandoff` to the PostgreSQL workflow store, so a run whose snapshot handoff is done can be deleted or pruned again. It follows the same rules as the in-memory store (see `@mastra/core`).

```ts
await workflows.releaseWorkflowSnapshotHandoff({
  workflowName: 'research-flow',
  runId: 'run-123',
  expectedClaimGeneration: 1,
  deleteRun: true,
});
```

- **Concurrent calls.** Release, claim, transition and completion running at the same time on one run always give consistent results.
- **Retried calls.** A retried or late release never removes a newer claim of the run.
- **Upgrading.** Existing handoff tables gain two columns (`claim_generation` and `released_at`) on the next `init()`. Existing handoffs keep working unchanged.
- **Capability.** The store now reports `releaseVersion: 1`.
