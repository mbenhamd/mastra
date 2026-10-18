---
'@mastra/core': minor
---

Added `cancelWorkflowRunForErasure` to workflow storage. It settles a run that will never settle on its own, so `deleteWorkflowRunClosure` can then erase it. Examples are an erased account's agent loop suspended on a question, or a run whose execution stopped. It overwrites the unsettled snapshot in place with an empty `canceled` snapshot under the same resource.

```ts
if (workflows.getWorkflowSnapshotHandoffCapabilities().closureCancelVersion === 1) {
  const result = await workflows.cancelWorkflowRunForErasure({ workflowName, runId, expectedResourceId: resourceId });
  // 'cancelled': erase the closure next. 'journaled' | 'active' | 'fenced': leave it to its owner and retry later.
}
```

- **Atomic.** Every check and the write happen in one step. An application that reads the journal and the run's nested children first and then calls `persistWorkflowSnapshot` could overwrite a parent while a child's terminal journal goes live. This call refuses that case.
- **Results**, checked in this order. Snapshot content changes only when the result is `cancelled`:
  - `missing`: no snapshot is stored for the run;
  - `resource_mismatch`: the snapshot, handoff or terminal snapshot records another resource, or the snapshot records none;
  - `fenced`: an unreleased handoff fences the run's snapshot writes, as for `persistWorkflowSnapshot`;
  - `journaled`: the run has a terminalization journal in any phase, so its owner settles it;
  - `active`: a nested child run (linked by a parent effect or recovery ancestry, whatever its resource) has not completed its terminal journal, or the run's parent revision latched a terminal status its snapshot does not show yet;
  - `settled`: the snapshot is already settled;
  - `cancelled`;
  - `unsupported`, from adapters without the capability.
- **Capability.** Adapters report `closureCancelVersion: 1` from `getWorkflowSnapshotHandoffCapabilities()`. The in-memory store supports it, and the base class answers `unsupported`.
- **In-memory erasure fence.** After `cancelled` or `settled`, a write that would reopen the run throws `WorkflowStaleSnapshotPersistError`. `persistWorkflowStepUpdate` returns `finalized` instead. Writes with the same status still work, and deleting the closure frees the run ID for reuse.
