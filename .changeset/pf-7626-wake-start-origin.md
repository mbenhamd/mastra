---
'@mastra/core': minor
---

Added `origin: 'wake'` and `wakeNoticeId` to the Harness v1 `agent_start` event of a settlement wake run (`subagents.onDelegatedSettlement: 'wake'`). `wakeNoticeId` is the stable id of the notice the run delivers. Use these fields to show a woken turn without parsing internal signal ids. Caller-started runs are unchanged and carry neither field.

```ts
session.subscribe(event => {
  if (event.type === 'agent_start' && event.origin === 'wake') {
    showBackgroundReply(event.runId, event.wakeNoticeId);
  }
});
```
