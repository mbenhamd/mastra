---
'@mastra/core': patch
---

Hardened Harness terminal handoff for retried, crashed, and cancelled runs.

- A terminal result now commits durably and replays to retried or duplicate streams, so a restart no longer double-invokes the provider or loses the settled outcome.
- Cancellation and fencing outcomes surface to duplicate, resumed, and waiting callers instead of leaving them pending.
- Suspended and resumed runs settle their terminal admission exactly once, and a run whose dispatch outcome is ambiguous is fenced rather than re-executing provider side effects.
- Sessions stored before terminal handoff existed adopt it on first load or update; operators no longer need to recreate them.

```ts
const harness = new Harness({
  modes,
  defaultModeId: 'build',
  sessions: {
    terminalHandoff: {
      finalizer: {
        id: 'chat-terminal',
        version: '1',
        async finalize({ result }) {
          return {
            projectionKind: 'chat-result',
            projectionId: 'latest',
            payload: { status: result.status },
          };
        },
      },
    },
  },
});
```
