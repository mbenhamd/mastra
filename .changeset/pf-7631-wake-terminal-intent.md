---
'@mastra/core': minor
---

Harness v1: a settlement wake turn (`subagents.onDelegatedSettlement: 'wake'`) can now stage a terminal intent. Register `sessions.terminalHandoff.wakeFinalizer` (its id must differ from the message finalizer's). Each wake turn is then admitted under a Harness-minted grant, `harness-wake:<wakeNoticeId>` at generation 1, and committed under the wake finalizer. The stored payload is a `HarnessWakeTerminalPayload`: `origin: 'wake'`, `wakeNoticeId`, `runId`, `taskId`, `subagentSessionId`, `outcome`, and `reply` (your finalizer's payload). If the process dies after the wake run ends but before the intent is written, the next owner of the session settles it from the staged output without re-running the model.

`message()`, `admitMessage()`, `signal()`, `queue()` and `admitQueue()` now reject an `admissionId` (and `message()`/`admitMessage()` an `executionAuthorityGrant.key`) in the reserved `harness-wake:` namespace. If a wake's terminal commit fails, the Harness emits an `error` event and retries with bounded backoff; if it still fails, `close()` is refused until the wake settles, and the admission stays durable for the next owner. A wake run that fails by its own output first emits an `error` event with its `runId` and error code; if its commit never lands, the next owner settles it as `aborted` with `harness.run_interrupted`, so that event is the only record of the run's error. A wake turn that suspends and is resumed commits through the same retry; if that still fails, the response call rejects and the interaction stays parked.

`claimTerminalIntents` accepts `finalizerIds` to claim only intents of those finalizers. Per-session delivery order now holds within one finalizer id, so an unsettled intent of one kind never holds back another kind. This also applies without the filter: if you rename your finalizer and drain the old id through `settleOnly`, intents under the old and new ids in one session are no longer ordered against each other. Keep one finalizer id across versions to keep their order.

```ts
const wakes = await storage.claimTerminalIntents({
  consumerId: 'wake-consumer',
  limit: 10,
  finalizerIds: ['my-app.chat-wake'],
});
```
