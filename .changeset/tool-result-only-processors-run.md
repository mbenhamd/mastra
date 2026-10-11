---
'@mastra/core': patch
---

Fixed output processors that implement only `processToolResult` being skipped on the default agent loop. A tool-result redaction or replacement hook now runs whether the processor is set on the agent or passed per call, even when no processor implements `processOutputStream`.

Processor workflows annotated with `setProcessorWorkflowPhases` now receive tool results only when their declared phases include `toolResult`. If an annotated workflow inspects or redacts tool results, add `'toolResult'` to its phases, or it will no longer run for them.
