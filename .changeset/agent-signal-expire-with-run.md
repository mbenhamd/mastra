---
'@mastra/core': patch
---

Added `ifActive.expireWithRun` to `agent.sendSignal()`. A signal delivered to a run that is already in progress then belongs to that run: if the run ends without reading it, the signal is dropped instead of starting a new follow-up run.

```ts
agent.sendSignal(
  { type: 'system-reminder', contents: 'Also add a glossary.' },
  { resourceId, threadId, ifActive: { behavior: 'deliver', expireWithRun: true }, ifIdle: { behavior: 'discard' } },
);
```
