---
'@mastra/core': patch
---

Added `releaseWorkflowSnapshotHandoff` to workflow storage. Before this, a run with a snapshot handoff could never be deleted, because the handoff blocks deletion and there was no way to lift it.

```ts
const result = await workflows.releaseWorkflowSnapshotHandoff({
  workflowName,
  runId,
  expectedClaimGeneration: handoff.claimGeneration,
  deleteRun: true,
});
if (result.status === 'conflict') {
  // The run was claimed again since `handoff` was read; nothing changed.
}
```

- **Releases only the claim you saw.** Handoff records now carry `claimGeneration`. A release that names an older generation, such as a retried release after the run was claimed again, answers `conflict` and changes nothing.
- **Fence rules.** A pending handoff needs its exact `mutationFence`. A completed one can be released without a fence, but a fence you pass must match.
- **What a released handoff does.** It no longer blocks deletion, retention or native writes, and `listWorkflowSnapshotHandoffs` no longer returns it.
  - It keeps its record (`releasedAt` is set). Claiming it again with the same fence after it completed still answers `completed`, so the outcome of a delivery is not lost.
  - Any other claim starts a new pending claim on the run, with the next `claimGeneration`.
- **Results:**
  - `released`;
  - `already_released`, for a repeat of the same release;
  - `missing`, when the run has no handoff;
  - `conflict`;
  - `unsupported`.
- **`deleteRun: true`** deletes the run in the same step, so no new claim can block it in between.
- **Capability.** Adapters that support release report `releaseVersion: 1` from `getWorkflowSnapshotHandoffCapabilities()`. The in-memory store supports it.
