---
'@mastra/core': patch
---

Added `releaseWorkflowSnapshotHandoff` to workflow storage. Before this, a run with a snapshot handoff could never be deleted, because the handoff row blocks deletion and there was no way to remove it.

- A completed handoff can be released by any caller. A handoff that is still pending can only be released by the caller holding its exact `mutationFence`. If a caller passes a fence, it must match, even for a completed handoff.
- The result is `released` (with the removed record), `missing` (nothing to release, so repeating a release is safe), or `conflict` (the row is kept and returned). Adapters without support return `unsupported`.
- Adapters that support it report `releaseVersion: 1` from `getWorkflowSnapshotHandoffCapabilities()`. The in-memory store supports it.
