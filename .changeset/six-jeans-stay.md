---
'@mastra/server': patch
'@mastra/core': patch
'@mastra/libsql': patch
'@mastra/pg': patch
---

Added reconnectable Harness session event replay and result recovery APIs for remote clients.

```ts
const eventStream = await fetch(`/harness/${name}/sessions/${sessionId}/events`, {
  headers: { 'Last-Event-ID': lastEventId },
});

const messageResult = await fetch(
  `/harness/${name}/sessions/${sessionId}/message-results/${signalId}`,
).then(response => response.json());

const queueResult = await fetch(
  `/harness/${name}/sessions/${sessionId}/queue/${queuedItemId}/result`,
).then(response => response.json());
```
