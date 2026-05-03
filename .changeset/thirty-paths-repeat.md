---
'@mastra/core': minor
---

Added agent prompt/tool waterfall summaries that show how prompts and tool surfaces change during a vNext run. The same typed, summary-only payload is available on `agent.generate()` results, `stream.getFullOutput()` results, finalized stream outputs, and, when tracing has a valid agent span, the `prompt_tool_waterfall` trace span.

```ts
const result = await agent.generate('Summarize the thread');

const phases = result.promptWaterfall?.phases.map(phase => ({
  kind: phase.kind,
  promptChars: phase.prompt.totalChars,
  toolCount: phase.toolSurface.toolCount,
}));
```

The waterfall records ordered checkpoints for the initial prompt, the memory-enabled preparation branch, configured input processors, input processor phases for nested processor workflows, `prepareStep`, pre-model execution, and structured output handling. It helps developers debug what the model saw before each model call without storing raw prompt text, tool descriptions, raw tool schema definitions, tool inputs, tool outputs, error messages, or tripwire reasons. It stores configured identifiers such as tool names, active tool names, tool choice names, processor ids, processor-workflow step ids, and error class names. Legacy `generateLegacy()` and `streamLegacy()` results do not include `promptWaterfall`.

Refs mastra-ai/mastra#16038.
