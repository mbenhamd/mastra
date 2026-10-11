---
'@mastra/core': patch
---

Tools with a `terminalResult` can now end the run after an approval. Set `afterApproval: true` when your success check verifies the applied outcome. An approved call, or a call resumed from a suspension, then ends the run with its result instead of returning to the model once more. The success check receives `approvalGranted` and `resumedFromSuspension` in its context.

Declined, denied and failed calls still return to the model, as do mixed batches and runs whose output processors could change the result.

```ts
const applyEdit = createTool({
  id: 'apply_edit',
  requireApproval: true,
  // ...
  terminalResult: {
    afterApproval: true,
    isSuccess: output => output.applied && output.verified,
    project: output => ({ summary: output.summary }),
    outputSchema: z.object({ summary: z.string() }),
  },
});
```
