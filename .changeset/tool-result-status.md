---
'@mastra/core': patch
---

A tool can now tell the framework that an output it returned is a failure. Add `resultStatus` to `createTool`. It receives the returned output and answers `'success'` or `'error'`.

This is for tools that return a failure as data, such as `{ success: false, stderr }`, so the model can read the error and try again. Before, Harness v1 recorded every returned output as a successful tool call. A subagent could then report `completed` and cite that call as successful evidence, and the outcome check accepted it.

With `resultStatus`, the tool-result chunk carries `providerMetadata.mastra.resultStatus: 'error'` and the Harness tool-call receipt records the call as failed. A `report_subagent_outcome` that claims success for that call is now rejected with `harness.subagent_evidence_unverified`. The model, the stream, the transcript and the `tool_end` event still get the returned output unchanged. The classifier receives the recorded output, the serialization-safe value the stream and transcript carry, on every engine and for awaited background completions; a background dispatch placeholder is never classified. A provider-executed tool's result comes from the provider and is not classified. A truthful `failed` or `blocked` report that cites the call with `status: 'error'` is accepted. A classifier that throws records the call as failed, and the tool-surface fence protects `resultStatus` like the tool's other executable policies. Tools without `resultStatus` behave as before and keep their recovery fingerprints.

```ts
const runAnalysis = createTool({
  id: 'run_analysis',
  description: 'Run Python for an analysis.',
  inputSchema: z.object({ code: z.string() }),
  resultStatus: output => (output.success ? 'success' : 'error'),
  execute: async ({ code }) => sandbox.run(code), // returns { success, stdout, stderr }
});
```
