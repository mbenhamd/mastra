---
'@mastra/core': patch
---

Harness terminal handoff can now settle turns admitted under an earlier or later finalizer version. Register those finalizers in `sessions.terminalHandoff.settleOnly` next to `finalizer`. New turns are still admitted only under `finalizer`. A pending turn is settled by the registration that matches the finalizer id and version recorded on the turn. A turn whose version is not registered stays pending, as before. Registering the same id and version twice is rejected when the Harness is created.

Harness storage has two new reads for checking that a finalizer version has drained before it is removed: `countPendingTerminalAdmissions` and `countUndeliveredTerminalIntents`. Stores without terminal handoff report them as unsupported.

```typescript
const harness = new Harness({
  agents: { default: agent },
  modes: [{ id: 'default', agentId: 'default' }],
  defaultModeId: 'default',
  sessions: {
    storage,
    terminalHandoff: { finalizer: finalizerV2, settleOnly: [finalizerV1] },
  },
});
```

Before switching admission to `finalizerV2`, a release can register `finalizerV1` as the primary and `finalizerV2` in `settleOnly`. Retire a version only after both drain counts are zero and any dead intents have been handled by the operator.
