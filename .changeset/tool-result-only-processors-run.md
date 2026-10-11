---
'@mastra/core': patch
---

Fixed output processors that implement only `processToolResult` being skipped on the default agent loop. A tool-result redaction or replacement hook now runs whether the processor is set on the agent or passed per call, even when no processor implements `processOutputStream`.

A processor workflow that handles tool results now has the messages it returns applied, as on the other phases, so a workflow can redact a tool result by returning transformed `messages` without mutating `messageList`.

Processor workflows annotated with `setProcessorWorkflowPhases` now receive tool results only when their declared phases include `toolResult`. Unannotated workflows are unchanged. If an annotated workflow inspects or redacts tool results, add `'toolResult'` to its phases:

```ts
// Before: the workflow no longer runs for tool results
setProcessorWorkflowPhases(redactionWorkflow, ['outputResult']);

// After: the workflow keeps running for tool results
setProcessorWorkflowPhases(redactionWorkflow, ['outputResult', 'toolResult']);
```
