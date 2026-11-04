---
'@mastra/core': minor
---

Harness v1: a run the Harness starts to deliver a delegated-task settlement notice (`subagents.onDelegatedSettlement: 'wake'`) now says so on its `agent_start` event. The event carries `origin: 'wake'` and `wakeNoticeId`, the stable id of the notice the run delivers. Runs a caller starts are unchanged and carry neither field, so a consumer can show a woken turn differently without parsing internal signal ids.

```ts
session.subscribe(event => {
  if (event.type === 'agent_start' && event.origin === 'wake') {
    showBackgroundReply(event.runId, event.wakeNoticeId);
  }
});
```
