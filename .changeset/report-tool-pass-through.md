---
'@mastra/core': minor
---

Added `passThroughToolNames` to tool payload transform policies. A policy never calls `transformToolPayload` for a listed tool, in any phase or for any target, so that tool's payloads pass through unchanged. A tool's own `transform` still applies. `@mastra/core/harness/v1` now exports `HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID` for this list.

Harness subagent answers now stream under a display policy that lists `report_subagent_outcome` in `passThroughToolNames`. Before, any display policy stopped `subagent_answer_delta` events, even one that left the report tool unchanged. Policies without that entry, legacy `toolPayloadProjection` policies and asynchronous dynamic default options still stop streaming.

```ts
import { HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID } from '@mastra/core/harness/v1';

const agent = new Agent({
  // ...
  transform: {
    targets: ['display'],
    passThroughToolNames: [HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID],
    transformToolPayload: ({ toolName, input }) => (toolName === 'run_python' ? { code: '[hidden]' } : input),
  },
});
```
