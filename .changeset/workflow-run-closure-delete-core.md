---
'@mastra/core': patch
---

Added `deleteWorkflowRunClosure` and `listWorkflowRunClosuresByResource` to workflow storage, so an application can erase everything storage keeps for a user's settled workflow runs. Before this, a released handoff kept its snapshot copy and delivered payload forever, and terminal rows had no delete path outside completed-journal cleanup.

```ts
if (!workflows.getWorkflowSnapshotHandoffCapabilities().closureDeleteVersion) {
  throw new Error('This store cannot erase workflow run closures');
}
let after;
do {
  const page = await workflows.listWorkflowRunClosuresByResource({ resourceId, after, limit: 100 });
  if (page.status !== 'listed') throw new Error('unsupported');
  for (const { workflowName, runId } of page.closures) {
    const result = await workflows.deleteWorkflowRunClosure({ workflowName, runId, expectedResourceId: resourceId });
    // 'deleted' | 'missing' are done; 'fenced' | 'active' are retried later; 'resource_mismatch' is refused.
  }
  after = page.nextCursor;
} while (after);
```

- **What is deleted.** In one atomic step: the handoff row (released or not), the run snapshot, and every terminal row whatever the journal phase (journal, effects, terminal snapshot, destination receipts, continuation plans, recovery ancestry).
- **Run ids.** Erasure drops the handoff's claim history, as removing a released handoff row by hand does: a later claim of the same run id starts again at `claimGeneration` 1. Don't reuse an erased run's id while a release from its erased lifetime could still arrive.
- **What is kept.** The content-free parent revision stays as the run-id reuse fence and is bumped, as `deleteWorkflowRunById` does.
- **Results**, checked in this order, and nothing changes unless the result is `deleted`:
  - `missing`: nothing is stored for the run (an idempotent no-op);
  - `resource_mismatch`: the snapshot, handoff or terminal snapshot records another resource, or none;
  - `fenced`: the handoff is pending and unreleased (a completed handoff does not fence erasure, released or not);
  - `active`: the snapshot status is not settled (`success`, `failed`, `canceled`, `tripwire`, `bailed`, `skipped`), or a terminalization owner holds a live lease;
  - `deleted`, with `removed: { snapshot, handoff, terminalRows }`;
  - `unsupported`, from adapters without the capability.
- **Listing.** `listWorkflowRunClosuresByResource` pages runs by `(workflowName, runId)` in code-point order and returns each run once, with flags for which rows matched: `snapshot`, `terminalSnapshot`, and `handoff: { status, released }`. Released handoffs are included, so it finds a delivered handoff whose snapshot was already deleted. `limit` is required (1 to 100).
- **Capability.** Adapters that implement both report `closureDeleteVersion: 1` from `getWorkflowSnapshotHandoffCapabilities()`. The in-memory store supports it; the base class answers `unsupported` for both methods and never an empty page.
