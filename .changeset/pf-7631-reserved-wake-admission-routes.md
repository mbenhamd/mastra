---
'@mastra/server': patch
'@mastra/ai-sdk': patch
---

The Harness message and queue routes (`POST /harness/:name/sessions/:sessionId/messages` and `/queue`) now reject an `admissionId` in the Harness-reserved `harness-wake:` namespace with a 400 `harness.validation` error (`details.field: 'admissionId'`). The check runs before any URL attachment is fetched or uploaded and before the session is loaded. `handleHarnessChatStream` rejects the same namespace with a `HarnessChatStreamValidationError` before it calls the session, whether the id comes from `params.admissionId` or from the default UI message id.

Before, any non-empty `admissionId` was accepted. After, an id that starts with `harness-wake:` is refused before any side effect, so nothing is admitted under it. If you generated ids with that prefix, choose another prefix. A retry of an id that was accepted before this release gets the same 400 and admits nothing. To avoid a duplicate turn, check that earlier admission's result before you retry the turn under a new id.

```ts
// Before: accepted
await fetch(`/harness/${name}/sessions/${sessionId}/messages`, {
  method: 'POST',
  body: JSON.stringify({ content: 'hello', admissionId: 'harness-wake:123' }),
});
// After: 400 harness.validation (details.field: 'admissionId'). Use your own namespace:
await fetch(`/harness/${name}/sessions/${sessionId}/messages`, {
  method: 'POST',
  body: JSON.stringify({ content: 'hello', admissionId: `client:${crypto.randomUUID()}` }),
});
```
