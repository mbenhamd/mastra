---
'@mastra/core': minor
---

Harness v1: a settlement wake turn (`subagents.onDelegatedSettlement: 'wake'`) can now stage a terminal intent. Register `sessions.terminalHandoff.wakeFinalizer` (its id must differ from the message finalizer's). Each wake turn is then admitted under a Harness-minted grant, `harness-wake:<wakeNoticeId>` at generation 1, and committed under the wake finalizer. The stored payload is a `HarnessWakeTerminalPayload`: `origin: 'wake'`, `wakeNoticeId`, `runId`, `taskId`, `subagentSessionId`, `outcome`, and `reply` (your finalizer's payload). If the process dies after the wake run ends but before the intent is written, the next owner of the session settles it from the staged output without re-running the model.

`message()` now rejects an `admissionId` or `executionAuthorityGrant.key` in the reserved `harness-wake:` namespace.

`claimTerminalIntents` accepts `finalizerIds` to claim only intents of those finalizers. Per-session delivery order now holds within one finalizer id, so an unsettled intent of one kind never holds back another kind.

```ts
const wakes = await storage.claimTerminalIntents({
  consumerId: 'wake-consumer',
  limit: 10,
  finalizerIds: ['my-app.chat-wake'],
});
```
