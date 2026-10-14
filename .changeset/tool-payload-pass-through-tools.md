---
'@mastra/core': minor
---

Added `passThroughToolNames` to `ToolPayloadTransformPolicy`. A policy never transforms the payloads of the tools it lists, for any target or phase, so a display policy can redact some tools while leaving others untouched. A Harness child whose display policy lists `report_subagent_outcome` now streams its answer as `subagent_answer_delta`. Previously any display policy disabled that stream.

```ts
const agent = new Agent({
  // ...
  transform: {
    targets: ['display'],
    passThroughToolNames: ['report_subagent_outcome'],
    transformToolPayload: redactSensitiveToolPayloads,
  },
});
```
