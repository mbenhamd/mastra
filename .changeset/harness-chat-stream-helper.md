---
'@mastra/ai-sdk': patch
'@mastra/core': patch
---

Added `handleHarnessChatStream` for admitting AI SDK UI chat bodies through Harness v1 `session.message({ stream: true })` while returning v5/v6 UI message streams. The helper preserves Harness admission IDs for normal submits, treats regenerate as a fresh admission, rejects non-`app` request context keys, and leaves human-in-the-loop (HITL) resume flows on native Harness routes.

Exported the Harness v1 stream message option/result types used by the AI SDK helper.

```ts
const stream = await handleHarnessChatStream({
  session,
  params,
});
```
