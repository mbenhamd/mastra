---
'@mastra/core': minor
---

Added `session.discardPendingInteraction()` to Harness v1. It resolves the session's current pending interaction without resuming its run or aborting any turn, for example an approval that was restored as answerable after its resume failed although the work it gated has already settled.

The call is fenced to the exact pending generation, the same `itemId`, `runId`, `toolCallId` and `pendingRequestedAt` that receipt responses bind to. It only acts when that generation is still current. The resolution is stored in the durable inbox response receipts, so repeating the call, or retrying an answer with the same `responseId`, replays the same outcome without new writes.

```ts
const pending = session.getRecord().pendingResume!
const result = await session.discardPendingInteraction({
  kind: pending.kind,
  itemId: pending.itemId ?? pending.toolCallId,
  runId: pending.runId,
  toolCallId: pending.toolCallId,
  pendingRequestedAt: pending.requestedAt,
  responseId: crypto.randomUUID(),
  reason: 'run_settled',
})
// result.status: 'discarded' | 'already_resolved' | 'not_current'
```

A storage failure rejects with `HarnessStorageError`. An interaction whose resume is already running rejects with `HarnessBusyError` and is left untouched. Later answers for a discarded generation reject with `HarnessPendingInteractionDiscardedError`. Existing answer, expiry and `abortActiveWork()` behavior is unchanged.
