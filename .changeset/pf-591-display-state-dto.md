---
'@mastra/core': minor
'@mastra/server': minor
'@mastra/client-js': minor
---

Harness v1 remote sessions now let developers reliably inspect remote session state through a versioned `displayState` snapshot (`version: 1`).

The snapshot includes active tools, tool input buffers, active subagents, token usage, pending operations, queue state, and goal metadata without requiring local runtime objects.

```ts
const session = await client.getHarness('default').getSession('session-id');
const snapshot = session.getDisplayState();

if (snapshot?.version === 1) {
  console.log(snapshot.activeTools);
  console.log(snapshot.toolInputBuffers);
  console.log(snapshot.activeSubagents);
  console.log(snapshot.tokenUsage);
  console.log(snapshot.pending);
}
```
