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
- **What a released handoff does.** It no longer blocks deletion, retention or native writes. It is no longer listed by `listWorkflowSnapshotHandoffs`, and it is not exported or restored by execution-closure export and import.
- **What a released handoff keeps.** It keeps its record (`releasedAt` is set), so claiming it again with the same fence after it completed still answers `completed`. A claim under another fence starts a new pending claim with the next `claimGeneration`, and only that latest claim's record is kept.
- **Cleanup.** Storage never removes released handoffs itself. Delete old ones yourself if you need to; once deleted, a later claim of the run starts fresh.
- **Results:**
  - `released`;
  - `already_released`, for a repeat of the same release;
  - `missing`, when the run has no handoff;
  - `conflict`;
  - `unsupported`.
- **`deleteRun: true`** deletes the run in the same step as the release, so no new claim can block it in between.
  - Only the call that actually releases deletes; a repeated release never deletes data written after the first one.
  - To delete a run whose handoff you released earlier, call `deleteWorkflowRunById`.
- **Capability.** Adapters that support release report `releaseVersion: 1` from `getWorkflowSnapshotHandoffCapabilities()`. The in-memory store supports it.
