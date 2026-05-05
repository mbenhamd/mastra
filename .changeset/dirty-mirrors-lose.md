---
'@mastra/core': minor
---

Added lifecycle timestamps to Harness display state so UIs can show when visible tool and subagent entries start and complete. Restarted tool entries now also clear stale terminal output before the next run begins, and suspended runs preserve in-flight display state for later resume.

```ts
harness.subscribeDisplayState(state => {
  const toolCallId = "tool-call-id";
  const tool = state.activeTools.get(toolCallId);

  console.log(tool?.startedAt, tool?.completedAt);
});
```
