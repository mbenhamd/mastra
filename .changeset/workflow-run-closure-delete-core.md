---
'@mastra/core': minor
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
- **Run ids.** Erasure keeps the handoff's highest claim generation on the parent revision, and a later claim of the same run id starts above it. A delayed release from the erased lifetime therefore answers `conflict` and cannot delete the new run.
- **What is kept.** The content-free parent revision stays as the run-id reuse fence and is bumped, as `deleteWorkflowRunById` does.
- **Results**, checked in this order, and nothing changes unless the result is `deleted`:
  - `missing`: nothing is stored for the run (an idempotent no-op);
  - `resource_mismatch`: the snapshot, handoff or terminal snapshot records another resource, or none;
  - `fenced`: the handoff is pending and unreleased (a completed handoff does not fence erasure, released or not);
  - `active`: the snapshot status is not settled (`success`, `failed`, `canceled`, `tripwire`, `bailed`, `skipped`), a terminalization owner holds a live lease, or a nested child run has not completed its terminal journal (erase child runs first);
  - `deleted`, with `removed: { snapshot, handoff, terminalRows }`;
  - `unsupported`, from adapters without the capability.
- **Listing.** `listWorkflowRunClosuresByResource` pages runs by `(workflowName, runId)` in code-point order and returns each run once, with flags for which rows matched: `snapshot`, `terminalSnapshot`, and `handoff: { status, released }`. Released handoffs are included, so it finds a delivered handoff whose snapshot was already deleted. `limit` is required (1 to 100).
- **Late writers.** A delayed `releaseWorkflowSnapshotHandoff` from an erased run answers `missing` while the run has no handoff, and `conflict` once a new claim of a reused run id holds the row; either way nothing changes. A delayed recovery-ancestry write for an erased run answers `missing_run` and writes nothing.
- **Run-id reuse.** The erased run's terminal status is cleared, so a reused run id starts running again.
- **Docs.** See the new *Workflow run closure* storage reference page.
- **Capability.** Adapters that implement both report `closureDeleteVersion: 1` from `getWorkflowSnapshotHandoffCapabilities()`. The in-memory store supports it; the base class answers `unsupported` for both methods and never an empty page.
