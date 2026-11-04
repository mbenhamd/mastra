---
'@mastra/server': patch
'@mastra/ai-sdk': patch
---

The Harness message and queue routes (`POST /harness/:name/sessions/:sessionId/messages` and `/queue`) now reject an `admissionId` in the Harness-reserved `harness-wake:` namespace with a 400 `harness.validation` error (`details.field: 'admissionId'`). The check runs before any URL attachment is fetched or uploaded and before the session is loaded. `handleHarnessChatStream` rejects the same namespace with a `HarnessChatStreamValidationError` before it calls the session, whether the id comes from `params.admissionId` or from the default UI message id.
