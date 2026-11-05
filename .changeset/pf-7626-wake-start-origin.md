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

Differs from fork main: if Stop (`abortActiveWork`) or an edit/regenerate (`resetPlanTasks`) lands while a claimed wake builds its context, the wake does not run (honours the Stop contract 'not stopped or cancelled since the settlement'). Without memory the notice is then not delivered; when the active mode's agent has memory, the persisted notice stays in thread history for its next turn.
