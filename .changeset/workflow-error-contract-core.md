---
'@mastra/core': minor
---

Added stable error ids for workflow lifecycle preconditions and a storage persistence failure contract.

**Stable ids.** `resume()`, `restart()` and `timeTravel()` now throw `MastraError`s with stable ids instead of plain errors, so callers no longer have to match on message text. Messages are unchanged.

- `WORKFLOW_RUN_NOT_SUSPENDED`: `resume()` found a run that is no longer suspended.
- `WORKFLOW_RUN_NOT_ACTIVE`: `restart()` found a run that is not running or waiting.
- `WORKFLOW_SNAPSHOT_NOT_FOUND`: the run has no stored snapshot.
- `WORKFLOW_RUN_STILL_RUNNING`: `timeTravel()` found a run that is still running.

The stored status is reported in `details.actualStatus`.

**Persistence failures.** Storage adapters can now say what a failed operation left behind: `transient` (it did not apply, and a later attempt may succeed), `permanent` (it did not apply, and repeating it fails the same way) or `commit_unknown` (the write may have committed although the caller saw a failure). Today only the PostgreSQL workflow storage (`@mastra/pg`) reports it; other adapters, including the in-memory store, return `undefined`. Read it with `getStoragePersistenceFailure`. Step retries never repeat a step that failed with `commit_unknown` (for example a nested workflow whose final write may have committed); the failure reaches the caller instead, and no failed outcome is recorded over it:

```ts
import { getStoragePersistenceFailure } from '@mastra/core/storage';

try {
  await run.resume({ step: 'approval', resumeData });
} catch (error) {
  if (getStoragePersistenceFailure(error) === 'commit_unknown') {
    // Read the run back before deciding anything; do not repeat the resume blindly.
    const state = await workflow.getWorkflowRunById(run.runId);
  }
}
```
