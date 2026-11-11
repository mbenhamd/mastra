---
'@mastra/core': patch
---

Made three existing workflow and harness symbols importable, so you no longer have to copy or string-match them.

- `@mastra/core/workflows` now exports `getRestartableWorkflowResumeIntent`, which returns the stored resume input that `restart()` continues for a run, or `undefined` when a restart re-executes the run from its snapshot.
- `@mastra/core/workflows` now exports the `WorkflowClaimErrorId` type, which lists the error ids thrown when a run claim is lost: `WORKFLOW_RESUME_ALREADY_CLAIMED`, `WORKFLOW_RESTART_NOT_CLAIMED` and `WORKFLOW_RESTART_RUN_OWNED`.
- `@mastra/core/harness/v1` now exports `HarnessExecutionError`, so you can check a redacted harness failure with `instanceof`.

```ts
import { MastraError } from '@mastra/core/error';
import type { WorkflowClaimErrorId } from '@mastra/core/workflows';

const alreadyClaimed: WorkflowClaimErrorId = 'WORKFLOW_RESUME_ALREADY_CLAIMED';

try {
  await run.resume({ step: 'approval', resumeData: { approved: true } });
} catch (error) {
  if (error instanceof MastraError && error.id === alreadyClaimed) {
    // Another caller resumed this run first.
  }
}
```

Error ids, messages and classes are unchanged.
