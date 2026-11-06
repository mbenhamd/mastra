---
'@mastra/core': minor
---

Harness v1: a settlement wake turn (`subagents.onDelegatedSettlement: 'wake'`) can now stage a terminal intent.

- **Registration:** register `sessions.terminalHandoff.wakeFinalizer`; its id must differ from the message finalizer's.
- **When a wake gets a grant:** the wake turn must still hold its idle slot at dispatch (no caller turn began, no run is active), and its reminder row must have been saved by the dispatching agent's own fixed memory instance with no mode change since. It then runs as the reserved wake run under a Harness-minted grant, `harness-wake:<wakeNoticeId>` at generation 1, and is committed under the wake finalizer.
- **When it does not:** any other wake (a memory-less parent, a row saved to another mode's memory, function or inherited memory, a mode switch, or a retry after a grant for the same notice was given up or orphaned) runs without a grant and stages no intent. A caller turn that takes the slot never receives the wake: it reads the reminder row from its history, or the wake runs after it (see the settlement-notice change in this release), and that later wake can get a grant and stage an intent when it meets the conditions above.
- **Given-up grants:** a grant given up after its admission (a caller turn or a mode switch during the admission, a Stop, edit or eviction before the dispatch, or a dispatch the runtime refused before any run started) is cancelled: no terminal result and no intent; its `harness-wake:` reservation is settled failed with `harness.terminal_cancelled`. If storage refuses the cancel, recovery settles the wake as interrupted (an aborted wake intent is possible). A hard delete does not cancel: it fences the session's terminal handoffs first, so the pending reservation becomes `fenced` and no intent can commit under it.
- **Dispatch bound:** if the runtime does not acknowledge a wake dispatch within 30 seconds, the wake turn is aborted and recovery settles it as interrupted.
- **Stored payload:** a `HarnessWakeTerminalPayload`: `origin: 'wake'`, `wakeNoticeId`, `runId`, `taskId`, `subagentSessionId`, `outcome`, and `reply` (your finalizer's payload). If the process dies after the wake run ends but before the intent is written, the next owner of the session settles it from the staged output without re-running the model.
- **Reserved namespace:** `message()`, `admitMessage()`, `signal()`, `queue()` and `admitQueue()` now reject an `admissionId` (and `message()`/`admitMessage()` an `executionAuthorityGrant.key`) in the reserved `harness-wake:` namespace.
- **Commit failures:** if a wake's terminal commit fails, the Harness emits an `error` event and retries with bounded backoff; if it still fails, `close()` is refused until the wake settles, and the admission stays durable for the next owner.
- **Run errors:** a wake run that fails by its own output first emits an `error` event with its `runId` and error code; if its commit never lands, the next owner settles it as `aborted` with `harness.run_interrupted`, so that event is the only record of the run's error. A wake turn whose terminal commit did not land emits `error` events but no `agent_end`, as `message()` does; its settlement publishes the outcome.
- **Suspended wakes:** a wake turn that suspends and is resumed commits through the same retry; if that still fails, the response call rejects and the interaction stays parked. A retried response settles it; once the interaction is due, `close()` settles it from the staged output. If only that commit fails, `close()` is refused with `HarnessSessionLockedError` and the interaction stays with its owner, which settles it once storage recovers.
- **Close with storage down:** if storage is also unavailable for close's read of the wake admission, `close()` rejects with the raw storage or terminal-handoff error (for example `harness.terminal_pending`).

```ts
const harness = new Harness({
  // agents, modes, ...
  subagents: { onDelegatedSettlement: 'wake' /* , types */ },
  sessions: {
    storage,
    terminalHandoff: {
      finalizer: chatFinalizer, // required alongside wakeFinalizer
      wakeFinalizer: {
        id: 'my-app.chat-wake', // must differ from chatFinalizer.id
        version: '1',
        // fullOutput is `unknown`: narrow it before reading a field.
        finalize: async ({ result, fullOutput }) => {
          const text =
            typeof fullOutput === 'object' && fullOutput !== null && 'text' in fullOutput && typeof fullOutput.text === 'string'
              ? fullOutput.text
              : null;
          return { projectionKind: 'chat.wake', projectionId: result.runId, payload: { status: result.status, text } };
        },
      },
    },
  },
});
```

New `@internal` member: `Agent.__fixedMemory()` returns the agent's own fixed memory instance without resolving it (undefined for function, inherited or no memory). It ships in the `Agent` type declarations (`dist/agent/agent.d.ts`). The wake path needs it to prove that the wake's reminder row was saved by the dispatching agent's own memory instance before it keeps a wake grant.

`claimTerminalIntents` accepts `finalizerIds` to claim only intents of those finalizers. Per-session delivery order now holds within one finalizer id, so an unsettled intent of one kind never holds back another kind. This also applies without the filter: if you rename your finalizer and drain the old id through `settleOnly`, intents under the old and new ids in one session are no longer ordered against each other. Keep one finalizer id across versions to keep their order.

```ts
const wakes = await storage.claimTerminalIntents({
  consumerId: 'wake-consumer',
  limit: 10,
  finalizerIds: ['my-app.chat-wake'],
});
```
