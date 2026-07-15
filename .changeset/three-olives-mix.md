---
'@mastra/inngest': minor
---

Added canonical replayable workflow lifecycle routing and stable event envelopes to the Inngest workflow adapter.

```ts
const run = await inngestWorkflow.createRun({ pubsub });
const stop = await run.watchLifecycle(event => {
  console.log(event.cursor, event.logGeneration, event.event);
});
```
