---
'@mastra/core': minor
---

Added typed, summary-only `promptWaterfall` observability for agent runs.

`promptWaterfall` is now available on `agent.generate()` results, finalized stream output, and `stream.getFullOutput()`. When tracing has a valid agent span, Mastra also emits the same summary on a `prompt_tool_waterfall` child span.

```ts
const result = await agent.generate('Summarize the thread');

const phases = result.promptWaterfall?.phases.map(phase => ({
  kind: phase.kind,
  promptChars: phase.prompt.totalChars,
  toolCount: phase.toolSurface.toolCount,
}));
```

The waterfall lets you inspect ordered phase summaries, prompt character sizes, schema character sizes, tool identifiers, processor identifiers, and terminal error or tripwire metadata lengths. It does not store raw prompt text, tool descriptions, raw tool schemas, tool inputs, tool outputs, error messages, or tripwire reasons.

Legacy `generateLegacy()` and `streamLegacy()` results do not include `promptWaterfall`.

Refs mastra-ai/mastra#16038.
